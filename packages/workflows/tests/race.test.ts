import { beforeEach, describe, expect, it, vi } from "vitest";
import {
	CriticalError,
	HistoryDivergedError,
	InMemoryDriver,
	Loop,
	RaceError,
	runWorkflow,
	type WorkflowContextInterface,
	type WorkflowErrorEvent,
	type WorkflowHistorySnapshot,
} from "../src/testing.js";

const modes = ["yield", "live"] as const;

function deferred<T>() {
	let resolve!: (value: T) => void;
	let reject!: (error: Error) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}

function timeoutBranch(durationMs: number) {
	return {
		name: "timeout",
		run: async (ctx: WorkflowContextInterface) => {
			await ctx.sleep("wait", durationMs);
			return "timed-out";
		},
	};
}

function raceBranchStatuses(
	history: WorkflowHistorySnapshot | undefined,
): Record<string, string> {
	for (const entry of history?.entries ?? []) {
		if (entry.kind.type === "race") {
			return Object.fromEntries(
				Object.entries(entry.kind.data.branches).map(
					([name, branch]) => [name, branch.status],
				),
			);
		}
	}
	throw new Error("history has no race entry");
}

for (const mode of modes) {
	describe(`Workflow Engine Race (${mode})`, { sequential: true }, () => {
		let driver: InMemoryDriver;

		beforeEach(() => {
			driver = new InMemoryDriver();
			driver.latency = 0;
		});

		it("should return first completed branch", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("race", [
					{
						name: "fast",
						run: async (ctx) => {
							return await ctx.step(
								"fast-step",
								async () => "fast",
							);
						},
					},
					{
						name: "slow",
						run: async (ctx) => {
							return await ctx.step(
								"slow-step",
								async () => "slow",
							);
						},
					},
				]);
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output?.winner).toBe("fast");
			expect(result.output?.value).toBe("fast");
		});

		it("should cancel losing branches", async () => {
			let aborted = false;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("cancel", [
					{
						name: "fast",
						run: async () => "winner",
					},
					{
						name: "slow",
						run: async (ctx) => {
							await new Promise<void>((resolve) => {
								if (ctx.abortSignal.aborted) {
									aborted = true;
									resolve();
									return;
								}
								ctx.abortSignal.addEventListener(
									"abort",
									() => {
										aborted = true;
										resolve();
									},
									{ once: true },
								);
							});
							return "aborted";
						},
					},
				]);
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(aborted).toBe(true);
		});

		it("should surface errors when all branches fail", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("fail", [
					{
						name: "one",
						run: async () => {
							throw new Error("fail one");
						},
					},
					{
						name: "two",
						run: async () => {
							throw new Error("fail two");
						},
					},
				]);
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(RaceError);
		});

		it("should replay race winner", async () => {
			let runs = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				const result = await ctx.race("replay", [
					{
						name: "first",
						run: async () => {
							runs += 1;
							return "winner";
						},
					},
					{
						name: "second",
						run: async () => "loser",
					},
				]);

				return result.value;
			};

			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;
			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;

			expect(runs).toBe(1);
		});

		it("should support nested joins in the winning branch", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("outer-race", [
					{
						name: "fast",
						run: async (ctx) => {
							const nested = await ctx.join("inner-join", {
								left: {
									run: async (ctx) =>
										await ctx.step(
											"left-step",
											async () => 1,
										),
								},
								right: {
									run: async (ctx) =>
										await ctx.step(
											"right-step",
											async () => 2,
										),
								},
							});

							return nested.left + nested.right;
						},
					},
					{
						name: "slow",
						run: async (ctx) => {
							await new Promise<void>((resolve) => {
								if (ctx.abortSignal.aborted) {
									resolve();
									return;
								}
								ctx.abortSignal.addEventListener(
									"abort",
									() => resolve(),
									{ once: true },
								);
							});
							return 0;
						},
					},
				]);
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toEqual({
				winner: "fast",
				value: 3,
			});
		});

		it("should support nested races in the winning branch", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("outer-race", [
					{
						name: "fast",
						run: async (ctx) => {
							const nested = await ctx.race("inner-race", [
								{
									name: "nested-fast",
									run: async (ctx) =>
										await ctx.step(
											"nested-fast-step",
											async () => "nested-fast",
										),
								},
								{
									name: "nested-slow",
									run: async (ctx) => {
										await new Promise<void>((resolve) => {
											if (ctx.abortSignal.aborted) {
												resolve();
												return;
											}
											ctx.abortSignal.addEventListener(
												"abort",
												() => resolve(),
												{ once: true },
											);
										});
										return "nested-slow";
									},
								},
							]);

							return nested.value;
						},
					},
					{
						name: "slow",
						run: async (ctx) => {
							await new Promise<void>((resolve) => {
								if (ctx.abortSignal.aborted) {
									resolve();
									return;
								}
								ctx.abortSignal.addEventListener(
									"abort",
									() => resolve(),
									{ once: true },
								);
							});
							return "slow";
						},
					},
				]);
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toEqual({
				winner: "fast",
				value: "nested-fast",
			});
		});

		it("should preserve branch retries instead of failing the race", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.race("retrying-race", [
					{
						name: "flaky",
						run: async (branchCtx) => {
							return await branchCtx.step({
								name: "flaky-step",
								maxRetries: 1,
								retryBackoffBase: 5,
								retryBackoffMax: 5,
								run: async () => {
									attempts += 1;
									if (attempts === 1) {
										throw new Error("retry");
									}
									return "winner";
								},
							});
						},
					},
					{
						name: "slow",
						run: async (branchCtx) => {
							await branchCtx.sleep("slow-wait", 200);
							return "slow";
						},
					},
				]);
			};

			// The first run suspends if the retry comes due after every branch
			// has yielded. It finishes in-process if the sleep branch is still
			// writing, so only the final outcome is fixed.
			let result = await runWorkflow(
				"wf-race-retry",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			if (result.state === "sleeping") {
				await new Promise((resolve) => setTimeout(resolve, 10));
				result = await runWorkflow(
					"wf-race-retry",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
			}

			expect(result.state).toBe("completed");
			expect(result.output).toEqual({
				winner: "flaky",
				value: "winner",
			});
			expect(attempts).toBe(2);
		});
	});
}

for (const mode of modes) {
	describe(
		`Workflow Engine Race Deadlines (${mode})`,
		{ sequential: true },
		() => {
			let driver: InMemoryDriver;

			beforeEach(() => {
				driver = new InMemoryDriver();
				driver.latency = 0;
			});

			it("should time out a step that is still running and keep the result on replay", async () => {
				const work = deferred<string>();
				let workRuns = 0;
				let nextStepRuns = 0;
				let afterRaceRuns = 0;

				const workflow = async (ctx: WorkflowContextInterface) => {
					const result = await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) => {
								const value = await branchCtx.step(
									"slow",
									async () => {
										workRuns += 1;
										return await work.promise;
									},
								);
								await branchCtx.step("next", async () => {
									nextStepRuns += 1;
								});
								return value;
							},
						},
						timeoutBranch(150),
					]);
					await ctx.step("after-race", async () => {
						afterRaceRuns += 1;
					});
					return result;
				};
				const expected = { winner: "timeout", value: "timed-out" };

				const startedAt = Date.now();
				const updates: WorkflowHistorySnapshot[] = [];
				const first = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode,
						onHistoryUpdated: (history) => updates.push(history),
					},
				).result;

				expect(first.state).toBe("completed");
				expect(first.output).toEqual(expected);
				expect(Date.now() - startedAt).toBeGreaterThanOrEqual(140);
				expect(raceBranchStatuses(updates.at(-1))).toEqual({
					work: "cancelled",
					timeout: "completed",
				});

				// The late result must not change what the workflow recorded.
				work.resolve("late");
				await new Promise((resolve) => setTimeout(resolve, 10));

				const replayed = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(replayed.output).toEqual(expected);
				expect(workRuns).toBe(1);
				expect(nextStepRuns).toBe(0);
				expect(afterRaceRuns).toBe(1);
			});

			it("should neither retry nor report a cancelled step that fails late", async () => {
				const work = deferred<string>();
				let workRuns = 0;
				const events: WorkflowErrorEvent[] = [];

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step("slow", async () => {
									workRuns += 1;
									return await work.promise;
								}),
						},
						timeoutBranch(50),
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode,
						onError: (event) => {
							events.push(event);
						},
					},
				).result;
				expect(result.output?.winner).toBe("timeout");

				work.reject(new Error("failed after the race ended"));
				await new Promise((resolve) => setTimeout(resolve, 10));

				expect(workRuns).toBe(1);
				expect(events).toEqual([]);
			});

			it("should let a step that finishes first win against a far deadline", async () => {
				const updates: WorkflowHistorySnapshot[] = [];
				const fortyDaysMs = 40 * 24 * 60 * 60 * 1000;

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step("slow", async () => {
									await new Promise((resolve) =>
										setTimeout(resolve, 50),
									);
									return "done";
								}),
						},
						timeoutBranch(fortyDaysMs),
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode,
						onHistoryUpdated: (history) => updates.push(history),
					},
				).result;

				expect(result.output).toEqual({
					winner: "work",
					value: "done",
				});
				expect(raceBranchStatuses(updates.at(-1))).toEqual({
					work: "completed",
					timeout: "cancelled",
				});
			});

			it("should wake each sleep of a branch while a step is running", async () => {
				const work = deferred<string>();
				const startedAt = Date.now();

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step(
									"slow",
									async () => await work.promise,
								),
						},
						{
							name: "timeout",
							run: async (branchCtx) => {
								await branchCtx.sleep("first", 120);
								await branchCtx.sleep("second", 120);
								return "timed-out";
							},
						},
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.output?.winner).toBe("timeout");
				expect(Date.now() - startedAt).toBeGreaterThanOrEqual(230);
				work.resolve("late");
			});

			it("should pick the earliest deadline among sleeping branches", async () => {
				const work = deferred<string>();

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step(
									"slow",
									async () => await work.promise,
								),
						},
						{
							name: "far",
							run: async (branchCtx) => {
								await branchCtx.sleep("far-wait", 60_000);
								return "far";
							},
						},
						{
							name: "near",
							run: async (branchCtx) => {
								await branchCtx.sleep("near-wait", 120);
								return "near";
							},
						},
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.output).toEqual({
					winner: "near",
					value: "near",
				});
				work.resolve("late");
			});

			it("should retry a failed step on schedule while another branch runs", async () => {
				const work = deferred<string>();
				let attempts = 0;

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("retry-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step(
									"slow",
									async () => await work.promise,
								),
						},
						{
							name: "flaky",
							run: async (branchCtx) =>
								await branchCtx.step({
									name: "flaky-step",
									maxRetries: 1,
									retryBackoffBase: 20,
									retryBackoffMax: 20,
									run: async () => {
										attempts += 1;
										if (attempts === 1) {
											throw new Error("retry");
										}
										return "recovered";
									},
								}),
						},
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.output).toEqual({
					winner: "flaky",
					value: "recovered",
				});
				expect(attempts).toBe(2);
				work.resolve("late");
			});

			it("should end a queue wait at its timeout while a step is running", async () => {
				const work = deferred<string>();

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("queue-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step(
									"slow",
									async () => await work.promise,
								),
						},
						{
							name: "queue",
							run: async (branchCtx) => {
								const messages =
									await branchCtx.queue.nextBatch("wait", {
										names: ["never-sent"],
										timeout: 120,
									});
								return `received-${messages.length}`;
							},
						},
					]);

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.output).toEqual({
					winner: "queue",
					value: "received-0",
				});
				work.resolve("late");
			});

			const loserScopes: Record<
				string,
				(
					ctx: WorkflowContextInterface,
					slow: (ctx: WorkflowContextInterface) => Promise<string>,
				) => Promise<unknown>
			> = {
				race: (ctx, slow) =>
					ctx.race("scope", [
						{ name: "inner", run: slow },
						{
							name: "inner-timeout",
							run: async (innerCtx) => {
								await innerCtx.sleep("inner-wait", 60_000);
								return "inner-timed-out";
							},
						},
					]),
				join: (ctx, slow) =>
					ctx.join("scope", { inner: { run: slow } }),
				try: (ctx, slow) => ctx.try("scope", slow),
				loop: (ctx, slow) =>
					ctx.loop("scope", async (loopCtx) => {
						await slow(loopCtx);
						return Loop.break(undefined);
					}),
			};

			for (const [scope, runScope] of Object.entries(loserScopes)) {
				it(`should cancel a step inside a losing ${scope} and replay the result`, async () => {
					const work = deferred<string>();
					const warn = vi
						.spyOn(console, "warn")
						.mockImplementation(() => {});

					const workflow = async (ctx: WorkflowContextInterface) =>
						await ctx.race("outer", [
							{
								name: "work",
								run: async (branchCtx) => {
									await runScope(
										branchCtx,
										async (innerCtx) =>
											await innerCtx.step(
												"slow",
												async () => await work.promise,
											),
									);
									return "work-done";
								},
							},
							timeoutBranch(120),
						]);
					const expected = { winner: "timeout", value: "timed-out" };

					try {
						const first = await runWorkflow(
							"wf-1",
							workflow,
							undefined,
							driver,
							{ mode },
						).result;
						expect(first.output).toEqual(expected);
						expect(warn).not.toHaveBeenCalled();
					} finally {
						warn.mockRestore();
					}

					work.resolve("late");
					await new Promise((resolve) => setTimeout(resolve, 10));

					const replayed = await runWorkflow(
						"wf-1",
						workflow,
						undefined,
						driver,
						{ mode },
					).result;
					expect(replayed.output).toEqual(expected);
				});
			}

			it("should keep concurrent races from observing each other", async () => {
				const slowWork = deferred<string>();

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.join("both", {
						timesOut: {
							run: async (branchCtx) =>
								await branchCtx.race("times-out", [
									{
										name: "work",
										run: async (raceCtx) =>
											await raceCtx.step(
												"slow",
												async () =>
													await slowWork.promise,
											),
									},
									timeoutBranch(100),
								]),
						},
						finishes: {
							run: async (branchCtx) =>
								await branchCtx.race("finishes", [
									{
										name: "work",
										run: async (raceCtx) =>
											await raceCtx.step(
												"quick",
												async () => {
													await new Promise(
														(resolve) =>
															setTimeout(
																resolve,
																200,
															),
													);
													return "done";
												},
											),
									},
									timeoutBranch(60_000),
								]),
						},
					});

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.output).toEqual({
					timesOut: { winner: "timeout", value: "timed-out" },
					finishes: { winner: "work", value: "done" },
				});
				slowWork.resolve("late");
			});

			it("should leave a durable wake when evicted and time out on resume", async () => {
				const work = deferred<string>();

				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "work",
							run: async (branchCtx) =>
								await branchCtx.step(
									"slow",
									async () => await work.promise,
								),
						},
						timeoutBranch(150),
					]);

				const startedAt = Date.now();
				const evicted = runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				);
				// The alarm write is the only signal that the race is armed.
				await vi.waitFor(() => {
					expect(driver.getAlarm("wf-1")).toBeDefined();
				});
				evicted.evict();

				const wakeAt = driver.getAlarm("wf-1") as number;
				expect(wakeAt).toBeGreaterThanOrEqual(startedAt + 150);

				// The evicted run must not start the sleeping branch again.
				await new Promise((resolve) =>
					setTimeout(resolve, wakeAt - Date.now() + 50),
				);
				const resumed = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(resumed.output).toEqual({
					winner: "timeout",
					value: "timed-out",
				});
				expect(driver.getAlarm("wf-1")).toBeUndefined();

				work.resolve("late");
				await evicted.result;
			});

			it("should suspend at once when every branch is waiting and wake at the deadline", async () => {
				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.race("timeout-race", [
						{
							name: "message",
							run: async (branchCtx) => {
								await branchCtx.queue.next("wait", {
									names: ["never-sent"],
								});
								return "message";
							},
						},
						timeoutBranch(500),
					]);
				const expected = { winner: "timeout", value: "timed-out" };

				const first = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				if (mode === "live") {
					expect(first.output).toEqual(expected);
					return;
				}

				const sleepUntil = first.sleepUntil as number;
				expect(first.state).toBe("sleeping");
				expect(Date.now()).toBeLessThan(sleepUntil);
				expect(driver.getAlarm("wf-1")).toBe(sleepUntil);

				await new Promise((resolve) =>
					setTimeout(resolve, sleepUntil - Date.now() + 20),
				);
				const resumed = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(resumed.output).toEqual(expected);
			});
		},
	);
}

describe("Workflow Engine Race (replay)", { sequential: true }, () => {
	it("should suspend at once on replay when a settled branch is skipped", async () => {
		const driver = new InMemoryDriver();
		driver.latency = 0;

		const workflow = async (ctx: WorkflowContextInterface) =>
			await ctx.race("timeout-race", [
				{
					name: "failed",
					run: async (): Promise<string> => {
						throw new CriticalError("failed for good");
					},
				},
				timeoutBranch(60_000),
			]);

		const first = await runWorkflow("wf-1", workflow, undefined, driver, {
			mode: "yield",
		}).result;
		expect(first.state).toBe("sleeping");

		// A hung replay fails this test on its timeout.
		const replayed = await runWorkflow(
			"wf-1",
			workflow,
			undefined,
			driver,
			{
				mode: "yield",
			},
		).result;
		expect(replayed.state).toBe("sleeping");
		expect(replayed.sleepUntil).toBe(first.sleepUntil);
	});
});

describe("Workflow Engine Race (changed code)", { sequential: true }, () => {
	let driver: InMemoryDriver;

	beforeEach(() => {
		driver = new InMemoryDriver();
		driver.latency = 0;
	});

	it("should reject when the winning branch diverges from history", async () => {
		const original = async (ctx: WorkflowContextInterface) =>
			await ctx.race("first", [
				{
					name: "a",
					run: async (branchCtx) => {
						await branchCtx.step("removed-step", async () => {});
						await branchCtx.queue.next("go", { names: ["go"] });
						return "a";
					},
				},
			]);
		const changed = async (ctx: WorkflowContextInterface) =>
			await ctx.race("first", [
				{
					name: "a",
					run: async (branchCtx) => {
						await branchCtx.queue.next("go", { names: ["go"] });
						return "a";
					},
				},
			]);

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
			id: "go-1",
			name: "go",
			data: {},
			sentAt: Date.now(),
		});

		await expect(
			runWorkflow("wf-1", changed, undefined, driver, { mode: "yield" })
				.result,
		).rejects.toThrow(HistoryDivergedError);
	});
});
