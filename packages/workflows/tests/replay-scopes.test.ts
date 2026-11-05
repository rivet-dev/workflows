import { beforeEach, describe, expect, it } from "vitest";
import {
	HistoryDivergedError,
	InMemoryDriver,
	Loop,
	runWorkflow,
	type WorkflowContextInterface,
} from "../src/testing.js";

type Body = (ctx: WorkflowContextInterface) => Promise<unknown>;

const waitForTick = (ctx: WorkflowContextInterface, name = "tick") =>
	ctx.queue.next(name, { names: ["tick"] });

// Runs to completion in one run, so a later suspend replays it as finished.
const finishedLoop: Body = (ctx) =>
	ctx.loop({
		name: "inner",
		state: { count: 0 },
		run: async (loopCtx, state) => {
			if (state.count === 2) {
				return Loop.break(state.count);
			}
			await loopCtx.step("work", async () => state.count);
			return Loop.continue({ count: state.count + 1 });
		},
	});

// Waits for one tick per iteration, so it resumes past its first iteration.
const resumedLoop: Body = (ctx) =>
	ctx.loop({
		name: "inner",
		state: { count: 0 },
		run: async (loopCtx, state) => {
			if (state.count === 3) {
				return Loop.break(state.count);
			}
			await waitForTick(loopCtx);
			return Loop.continue({ count: state.count + 1 });
		},
	});

const parents: Record<string, (body: Body) => Body> = {
	loop: (body) => (ctx) =>
		ctx.loop("outer", async (outerCtx) => {
			await body(outerCtx);
			return Loop.break("done");
		}),
	try: (body) => async (ctx) => {
		await ctx.try("guarded", async (tryCtx) => await body(tryCtx));
		return "done";
	},
	join: (body) => async (ctx) => {
		await ctx.join("parent", {
			only: { run: (branchCtx) => body(branchCtx) },
		});
		return "done";
	},
	race: (body) => async (ctx) => {
		await ctx.race("parent", [
			{ name: "only", run: (branchCtx) => body(branchCtx) },
		]);
		return "done";
	},
};

const finishedScopes: Record<string, Body> = {
	loop: finishedLoop,
	join: (ctx) => ctx.join("scope", { a: { run: finishedLoop } }),
	race: (ctx) => ctx.race("scope", [{ name: "a", run: finishedLoop }]),
};

describe(
	"Workflow Engine replay of settled scopes",
	{ sequential: true },
	() => {
		let driver: InMemoryDriver;

		beforeEach(() => {
			driver = new InMemoryDriver();
			driver.latency = 0;
		});

		// Delivers one tick before every run after the first until the run completes.
		async function runOneTickPerRun(workflow: Body): Promise<unknown> {
			for (let run = 0; run < 20; run++) {
				if (run > 0) {
					await driver.messageDriver.addMessage({
						id: `tick-${run}`,
						name: "tick",
						data: {},
						sentAt: Date.now(),
					});
				}
				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode: "yield",
					},
				).result;
				if (result.state === "completed") {
					return result.output;
				}
				expect(result.state).toBe("sleeping");
			}
			throw new Error("workflow did not complete");
		}

		for (const [parentName, parent] of Object.entries(parents)) {
			it(`completes a resumed loop inside a ${parentName}`, async () => {
				expect(await runOneTickPerRun(parent(resumedLoop))).toBe(
					"done",
				);
			});

			for (const [scopeName, scope] of Object.entries(finishedScopes)) {
				it(`completes when a finished ${scopeName} inside a ${parentName} replays`, async () => {
					const output = await runOneTickPerRun(
						parent(async (ctx) => {
							await scope(ctx);
							await waitForTick(ctx, "after-scope");
						}),
					);

					expect(output).toBe("done");
				});
			}
		}

		it("completes when a finished join branch replays beside a waiting branch", async () => {
			const output = await runOneTickPerRun(
				parents.loop((ctx) =>
					ctx.join("scope", {
						finished: { run: finishedLoop },
						waiting: { run: (branchCtx) => waitForTick(branchCtx) },
					}),
				),
			);

			expect(output).toBe("done");
		});

		it("completes when a failed join branch replays inside a handled try", async () => {
			const output = await runOneTickPerRun(
				parents.loop(async (ctx) => {
					const result = await ctx.try("handled", (tryCtx) =>
						tryCtx.join("scope", {
							failing: {
								run: async (branchCtx) => {
									await branchCtx.step(
										"work",
										async () => "kept",
									);
									throw new Error("branch failed");
								},
							},
						}),
					);
					expect(result.ok).toBe(false);
					await waitForTick(ctx, "after-scope");
				}),
			);

			expect(output).toBe("done");
		});

		it("resolves getVersion to latest after a resumed loop", async () => {
			const output = await runOneTickPerRun(async (ctx) => {
				await resumedLoop(ctx);
				return await ctx.getVersion("gate", 2);
			});

			expect(output).toBe(2);
		});

		it("resolves getVersion to latest after finished scopes replay", async () => {
			const output = await runOneTickPerRun(async (ctx) => {
				await finishedLoop(ctx);
				await ctx.join("scope", { a: { run: finishedLoop } });
				await waitForTick(ctx, "after-scope");
				return await ctx.getVersion("gate", 2);
			});

			expect(output).toBe(2);
		});

		it("completes when ctx.removed replaces a finished loop inside a loop", async () => {
			const original: Body = parents.loop(async (ctx) => {
				await finishedLoop(ctx);
				await waitForTick(ctx, "after-scope");
			});
			const migrated: Body = parents.loop(async (ctx) => {
				await ctx.removed("inner", "loop");
				await waitForTick(ctx, "after-scope");
			});

			const firstRun = await runWorkflow(
				"wf-1",
				original,
				undefined,
				driver,
				{
					mode: "yield",
				},
			).result;
			expect(firstRun.state).toBe("sleeping");

			await driver.messageDriver.addMessage({
				id: "tick-1",
				name: "tick",
				data: {},
				sentAt: Date.now(),
			});

			const result = await runWorkflow(
				"wf-1",
				migrated,
				undefined,
				driver,
				{
					mode: "yield",
				},
			).result;
			expect(result.state).toBe("completed");
			expect(result.output).toBe("done");
		});

		it("rejects a removed scope that history still holds", async () => {
			const original: Body = parents.loop(async (ctx) => {
				await finishedLoop(ctx);
				await waitForTick(ctx, "after-scope");
			});
			const changed: Body = parents.loop((ctx) =>
				waitForTick(ctx, "after-scope"),
			);

			const firstRun = await runWorkflow(
				"wf-1",
				original,
				undefined,
				driver,
				{
					mode: "yield",
				},
			).result;
			expect(firstRun.state).toBe("sleeping");

			await driver.messageDriver.addMessage({
				id: "tick-1",
				name: "tick",
				data: {},
				sentAt: Date.now(),
			});

			await expect(
				runWorkflow("wf-1", changed, undefined, driver, {
					mode: "yield",
				}).result,
			).rejects.toThrow(HistoryDivergedError);
		});
	},
);
