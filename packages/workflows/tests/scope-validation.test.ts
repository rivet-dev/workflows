import { expect, it } from "vitest";
import {
	HistoryDivergedError,
	InMemoryDriver,
	Loop,
	runWorkflow,
	type WorkflowContextInterface,
} from "../src/testing.js";

function newDriver(): InMemoryDriver {
	const driver = new InMemoryDriver();
	driver.latency = 0;
	driver.workerPollInterval = 0;
	return driver;
}

// Runs the workflow until `reached` resolves, evicts it, and waits for it to stop.
async function runUntilEvicted(
	driver: InMemoryDriver,
	workflow: (ctx: WorkflowContextInterface) => Promise<unknown>,
	reached: Promise<void>,
	release: () => void,
): Promise<void> {
	const handle = runWorkflow("wf", workflow, undefined, driver);
	await reached;
	handle.evict();
	release();
	await handle.result.catch(() => undefined);
}

it("resumes a loop inside a try block at its saved iteration", async () => {
	const driver = newDriver();
	const timeline: string[] = [];
	const reached = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let pauseOnce = true;
	const workflow = async (ctx: WorkflowContextInterface) =>
		ctx.try("guarded", (block) =>
			block.loop({
				name: "jobs",
				state: 0,
				run: async (iterationCtx, i) => {
					await iterationCtx.step("work", async () => {
						timeline.push(`${i}`);
					});
					if (i === 1 && pauseOnce) {
						pauseOnce = false;
						reached.resolve();
						await release.promise;
					}
					if (i >= 2) return Loop.break("looped");
					return Loop.continue(i + 1);
				},
			}),
		);

	await runUntilEvicted(driver, workflow, reached.promise, () =>
		release.resolve(),
	);
	expect(timeline).toEqual(["0", "1"]);

	timeline.length = 0;
	const resumed = await runWorkflow("wf", workflow, undefined, driver).result;
	expect({ state: resumed.state, output: resumed.output, timeline }).toEqual({
		state: "completed",
		output: { ok: true, value: "looped" },
		timeline: ["2"],
	});
});

it("resumes nested loops at the saved inner iteration", async () => {
	const driver = newDriver();
	const timeline: string[] = [];
	const reached = Promise.withResolvers<void>();
	const release = Promise.withResolvers<void>();
	let pauseOnce = true;
	const workflow = async (ctx: WorkflowContextInterface) => {
		await ctx.loop({
			name: "outer",
			state: 0,
			run: async (outerCtx, i) => {
				await outerCtx.loop({
					name: "inner",
					state: 0,
					run: async (innerCtx, j) => {
						await innerCtx.step("work", async () => {
							timeline.push(`${i}.${j}`);
						});
						if (i === 1 && j === 1 && pauseOnce) {
							pauseOnce = false;
							reached.resolve();
							await release.promise;
						}
						if (j >= 2) return Loop.break(undefined);
						return Loop.continue(j + 1);
					},
				});
				if (i >= 1) return Loop.break(undefined);
				return Loop.continue(i + 1);
			},
		});
		return "done";
	};

	await runUntilEvicted(driver, workflow, reached.promise, () =>
		release.resolve(),
	);
	expect(timeline).toEqual(["0.0", "0.1", "0.2", "1.0", "1.1"]);

	timeline.length = 0;
	const resumed = await runWorkflow("wf", workflow, undefined, driver).result;
	expect({ state: resumed.state, output: resumed.output, timeline }).toEqual({
		state: "completed",
		output: "done",
		timeline: ["1.2"],
	});
});

it("still rejects the recorded steps of a try scope that the code removed", async () => {
	const driver = newDriver();
	const first = await runWorkflow(
		"wf",
		async (ctx) => {
			await ctx.try("container", async (outer) => {
				await outer.try("optional", async (block) => {
					await block.step("recorded", async () => "receipt");
				});
			});
			return "original";
		},
		undefined,
		driver,
	).result;
	expect(first).toMatchObject({ state: "completed", output: "original" });

	await expect(
		runWorkflow(
			"wf",
			async (ctx) => ctx.try("container", async () => "changed"),
			undefined,
			driver,
		).result,
	).rejects.toBeInstanceOf(HistoryDivergedError);
});

it("rejects the recorded steps of a top-level try scope that the code removed", async () => {
	const driver = newDriver();
	await runWorkflow(
		"wf",
		async (ctx) => {
			await ctx.try("optional", async (block) => {
				await block.step("recorded", async () => "receipt");
			});
			return "original";
		},
		undefined,
		driver,
	).result;

	await expect(
		runWorkflow("wf", async () => "changed", undefined, driver).result,
	).rejects.toBeInstanceOf(HistoryDivergedError);
});

it("replays a finished join inside a try scope that the code kept", async () => {
	const driver = newDriver();
	const runs: string[] = [];
	const workflow = async (ctx: WorkflowContextInterface) => {
		const joined = await ctx.try("guarded", (block) =>
			block.join("jobs", {
				a: {
					run: (branch) =>
						branch.step("work", async () => {
							runs.push("work");
							return "a";
						}),
				},
			}),
		);
		await ctx.queue.next("wait", { names: ["wake"] });
		return joined;
	};
	await runWorkflow("wf", workflow, undefined, driver).result;
	await driver.messageDriver.addMessage({
		id: "wake",
		name: "wake",
		data: {},
		sentAt: Date.now(),
	});

	const resumed = await runWorkflow("wf", workflow, undefined, driver).result;

	expect({ resumed, runs }).toEqual({
		resumed: {
			state: "completed",
			output: { ok: true, value: { a: "a" } },
		},
		runs: ["work"],
	});
});
