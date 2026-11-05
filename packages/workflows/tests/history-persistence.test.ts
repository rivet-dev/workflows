import { describe, expect, it } from "vitest";
import {
	InMemoryDriver,
	type KVWrite,
	Loop,
	loadStorage,
	runWorkflow,
	type WorkflowContextInterface,
} from "../src/testing.js";

// Holds one history write open so a test can change workflow state while
// that write is in flight.
class HeldWriteDriver extends InMemoryDriver {
	holdNext = false;
	failNext = false;
	readonly writeStarted = Promise.withResolvers<void>();
	readonly releaseWrite = Promise.withResolvers<void>();
	readonly heldWriteFinished = Promise.withResolvers<void>();

	override async batch(writes: KVWrite[]): Promise<void> {
		if (this.holdNext) {
			this.holdNext = false;
			this.writeStarted.resolve();
			await this.releaseWrite.promise;
			await super.batch(writes);
			this.heldWriteFinished.resolve();
			return;
		}
		if (this.failNext) {
			this.failNext = false;
			throw new Error("write failed");
		}
		await super.batch(writes);
	}
}

function newDriver(): HeldWriteDriver {
	const driver = new HeldWriteDriver();
	driver.latency = 0;
	driver.workerPollInterval = 0;
	return driver;
}

async function savedStep(driver: InMemoryDriver, key: string) {
	const storage = await loadStorage(driver);
	const entry = storage.history.entries.get(key);
	if (entry?.kind.type !== "step") throw new Error(`no step at ${key}`);
	return {
		output: entry.kind.data.output,
		error: entry.kind.data.error,
		status: storage.entryMetadata.get(entry.id)?.status,
	};
}

// Wakes a workflow parked on ctx.queue.next("wake") and runs it again.
async function wakeAndRerun(
	driver: InMemoryDriver,
	workflow: (ctx: WorkflowContextInterface) => Promise<unknown>,
) {
	await driver.messageDriver.addMessage({
		id: "wake",
		name: "wake",
		data: {},
		sentAt: Date.now(),
	});
	return await runWorkflow("wf", workflow, undefined, driver).result;
}

describe("history saved while join branches overlap", () => {
	it("reloads after a branch starts a new step while a sibling's save is in flight", async () => {
		const driver = newDriver();
		driver.latency = 50;

		const result = await runWorkflow(
			"wf",
			async (ctx) =>
				await ctx.join("parallel", {
					first: {
						run: async (branch) => {
							await branch.step("fetch-first", async () => 1);
							return await branch.step("save-first", async () => {
								await new Promise((r) => setTimeout(r, 100));
								return 2;
							});
						},
					},
					second: {
						run: async (branch) =>
							await branch.step("fetch-second", async () => {
								await new Promise((r) => setTimeout(r, 25));
								return 3;
							}),
					},
				}),
			undefined,
			driver,
		).result;

		expect(result).toEqual({
			state: "completed",
			output: { first: 2, second: 3 },
		});
		await expect(loadStorage(driver)).resolves.toBeDefined();
	});

	it("replays after a wake without re-running finished steps, then runs the next step", async () => {
		const driver = newDriver();
		const runs: string[] = [];
		const workflow = async (ctx: WorkflowContextInterface) => {
			const joined = await ctx.join("jobs", {
				a: {
					run: async (branch) => {
						await branch.step("first", async () => {
							runs.push("first");
							driver.holdNext = true;
							return "a";
						});
						return "a";
					},
				},
				b: {
					run: async (branch) => {
						// Starts after the sibling's save opens, so this step's name
						// is registered during that save.
						await driver.writeStarted.promise;
						return await branch.step("second", async () => {
							runs.push("second");
							// Finish the sibling's save before this step saves.
							driver.releaseWrite.resolve();
							await driver.heldWriteFinished.promise;
							await new Promise((resolve) =>
								setImmediate(resolve),
							);
							return "b";
						});
					},
				},
			});
			await ctx.queue.next("wait", { names: ["wake"] });
			const after = await ctx.step("after-wake", async () => {
				runs.push("after-wake");
				return "woke";
			});
			return { joined, after };
		};

		const first = await runWorkflow("wf", workflow, undefined, driver)
			.result;
		expect(first.state).toBe("sleeping");

		const resumed = await wakeAndRerun(driver, workflow);

		expect({ resumed, runs }).toEqual({
			resumed: {
				state: "completed",
				output: { joined: { a: "a", b: "b" }, after: "woke" },
			},
			runs: ["first", "second", "after-wake"],
		});
	});

	it("does not re-run a finished step when its branch resumes after a wake", async () => {
		const driver = newDriver();
		const runs: string[] = [];
		const workflow = async (ctx: WorkflowContextInterface) =>
			await ctx.join("jobs", {
				a: {
					run: async (branch) => {
						const output = await branch.step("slow", async () => {
							runs.push("slow");
							await driver.writeStarted.promise;
							return "a";
						});
						// Keeps the branch open, so a wake replays its steps.
						await branch.queue.next("wait", { names: ["wake"] });
						return output;
					},
				},
				b: {
					run: (branch) =>
						branch.step("record", async () => {
							runs.push("record");
							driver.holdNext = true;
							return "b";
						}),
				},
			});

		const handle = runWorkflow("wf", workflow, undefined, driver);
		await driver.writeStarted.promise;
		// Let branch a finish its step and save it behind the held write.
		await new Promise((resolve) => setImmediate(resolve));
		driver.releaseWrite.resolve();
		expect((await handle.result).state).toBe("sleeping");

		const resumed = await wakeAndRerun(driver, workflow);

		expect({ resumed, runs }).toEqual({
			resumed: { state: "completed", output: { a: "a", b: "b" } },
			runs: ["slow", "record"],
		});
	});

	it("keeps a completed step completed when an older save finishes later", async () => {
		const driver = newDriver();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) =>
						branch.step("work", async () => {
							driver.holdNext = true;
							await driver.writeStarted.promise;
							return "a";
						}),
				},
				b: {
					run: (branch) => branch.step("record", async () => "b"),
				},
			});
		const result = runWorkflow("wf", workflow, undefined, driver).result;
		await driver.writeStarted.promise;
		// Let branch a complete and queue its save behind the held one.
		await new Promise((resolve) => setImmediate(resolve));
		driver.releaseWrite.resolve();

		expect(await result).toEqual({
			state: "completed",
			output: { a: "a", b: "b" },
		});
		expect(await savedStep(driver, "jobs/a/work")).toMatchObject({
			status: "completed",
		});
	});

	it("saves a step failure that happens after a sibling saved the step as running", async () => {
		const driver = newDriver();
		const siblingSaved = Promise.withResolvers<void>();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) =>
						branch.step({
							name: "work",
							maxRetries: 0,
							run: async () => {
								await siblingSaved.promise;
								throw new Error("down");
							},
						}),
				},
				b: {
					run: async (branch) => {
						await branch.step("record", async () => "b");
						siblingSaved.resolve();
					},
				},
			});

		await expect(
			runWorkflow("wf", workflow, undefined, driver).result,
		).rejects.toThrow();

		expect(await savedStep(driver, "jobs/a/work")).toMatchObject({
			error: expect.stringContaining("down"),
			status: "exhausted",
		});
	});

	it("saves a step failure that happens while a sibling's save is in flight", async () => {
		const driver = newDriver();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) =>
						branch.step({
							name: "work",
							maxRetries: 0,
							run: async () => {
								await driver.writeStarted.promise;
								throw new Error("down");
							},
						}),
				},
				b: {
					run: async (branch) => {
						await branch.step("record", async () => {
							driver.holdNext = true;
							return "b";
						});
					},
				},
			});

		const result = runWorkflow("wf", workflow, undefined, driver).result;
		await driver.writeStarted.promise;
		// Let branch a fail while branch b's save is still open.
		await new Promise((resolve) => setImmediate(resolve));
		driver.releaseWrite.resolve();
		await expect(result).rejects.toThrow();

		expect(await savedStep(driver, "jobs/a/work")).toMatchObject({
			error: expect.stringContaining("down"),
			status: "exhausted",
		});
	});

	it("keeps a finished step's result when the save carrying it fails", async () => {
		const driver = newDriver();
		const workflow = async (ctx: WorkflowContextInterface) => {
			// Ephemeral, so the next step's save is the one that carries it.
			await ctx.step({
				name: "quiet",
				ephemeral: true,
				run: async () => "kept",
			});
			await ctx.step("work", async () => {
				driver.failNext = true;
			});
		};

		const result = await runWorkflow("wf", workflow, undefined, driver)
			.result;

		expect(result.state).toBe("sleeping");
		expect(await savedStep(driver, "quiet")).toMatchObject({
			output: "kept",
			status: "completed",
		});
	});

	it("does not confirm a step until its result is saved", async () => {
		const driver = newDriver();
		let savedWhenConfirmed: unknown;
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) =>
						branch.step("held", async () => {
							driver.holdNext = true;
							return "a";
						}),
				},
				b: {
					run: async (branch) => {
						await driver.writeStarted.promise;
						const output = await branch.step("late", async () => {
							// Finish while the held save is still open, then let it close.
							setImmediate(() => driver.releaseWrite.resolve());
							return "b";
						});
						savedWhenConfirmed = (
							await savedStep(driver, "jobs/b/late")
						).output;
						return output;
					},
				},
			});

		await runWorkflow("wf", workflow, undefined, driver).result;

		expect(savedWhenConfirmed).toBe("b");
	});

	it("keeps every branch's result when a save that several branches wait on fails", async () => {
		const driver = newDriver();
		const runs: string[] = [];
		const waitingStep = (name: string) => ({
			run: async (branch: WorkflowContextInterface) => {
				await driver.writeStarted.promise;
				return await branch.step(name, async () => {
					runs.push(name);
					return name;
				});
			},
		});
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) =>
						branch.step("held", async () => {
							runs.push("held");
							driver.holdNext = true;
							return "held";
						}),
				},
				b: waitingStep("b"),
				c: waitingStep("c"),
				d: waitingStep("d"),
			});

		const first = runWorkflow("wf", workflow, undefined, driver).result;
		await driver.writeStarted.promise;
		// Let the other branches finish and wait behind the held save.
		await new Promise((resolve) => setImmediate(resolve));
		driver.failNext = true;
		driver.releaseWrite.resolve();
		expect((await first).state).toBe("sleeping");

		const resumed = await runWorkflow("wf", workflow, undefined, driver)
			.result;

		expect({ resumed, runs: runs.sort() }).toEqual({
			resumed: {
				state: "completed",
				output: { a: "held", b: "b", c: "c", d: "d" },
			},
			runs: ["b", "c", "d", "held"],
		});
	});

	it("removes pruned loop iterations when the prune overlaps a sibling's save", async () => {
		const driver = newDriver();
		const siblingsMayStart = Promise.withResolvers<void>();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				held: {
					run: async (branch) => {
						await siblingsMayStart.promise;
						await branch.step("held", async () => {
							driver.holdNext = true;
						});
					},
				},
				sibling: {
					run: async (branch) => {
						await driver.writeStarted.promise;
						await branch.step("sibling", async () => {});
					},
				},
				looping: {
					run: (branch) =>
						branch.loop({
							name: "loop",
							state: 0,
							historyPruneInterval: 2,
							historySize: 1,
							run: async (iteration, i) => {
								// Saved before the prune, so the prune must delete it.
								await iteration.step({
									name: "work",
									ephemeral: i > 0,
									run: async () => i,
								});
								if (i === 1) {
									// The prune starts behind the held save and the sibling's save.
									siblingsMayStart.resolve();
									await driver.writeStarted.promise;
									await new Promise((resolve) =>
										setImmediate(resolve),
									);
									setImmediate(() =>
										driver.releaseWrite.resolve(),
									);
								}
								return i === 3
									? Loop.break("done")
									: Loop.continue(i + 1);
							},
						}),
				},
			});

		await runWorkflow("wf", workflow, undefined, driver).result;

		const storage = await loadStorage(driver);
		const savedIterations = [...storage.history.entries.keys()].filter(
			(key) => key.endsWith("/work"),
		);
		expect(savedIterations).toEqual(["jobs/looping/loop/~3/work"]);
	});

	it("does not stall a workflow when another workflow's save is in flight", async () => {
		const stuck = newDriver();
		const other = newDriver();
		const stuckResult = runWorkflow(
			"stuck",
			(ctx) =>
				ctx.step("work", async () => {
					stuck.holdNext = true;
					return "stuck";
				}),
			undefined,
			stuck,
		).result;
		await stuck.writeStarted.promise;

		const otherResult = await runWorkflow(
			"other",
			(ctx) => ctx.step("work", async () => "other"),
			undefined,
			other,
		).result;
		expect(otherResult).toEqual({ state: "completed", output: "other" });

		stuck.releaseWrite.resolve();
		await expect(stuckResult).resolves.toEqual({
			state: "completed",
			output: "stuck",
		});
	});
});
