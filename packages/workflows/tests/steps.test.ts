import { execFile } from "node:child_process";
import { getEventListeners } from "node:events";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { deserializeEntry } from "../schemas/serde.js";
import { buildHistoryPrefixAll } from "../src/keys.js";
import {
	CriticalError,
	EntryInProgressError,
	HistoryDivergedError,
	InMemoryDriver,
	loadStorage,
	RollbackError,
	runWorkflow,
	StepExhaustedError,
	type WorkflowContextInterface,
	type WorkflowErrorEvent,
} from "../src/testing.js";

const modes = ["yield", "live"] as const;

// Longer than the 2^31-1 ms that a single setTimeout can wait.
const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
const JUST_OVER_SET_TIMEOUT_LIMIT_MS = 2 ** 31;

const execFileAsync = promisify(execFile);
const LONG_TIMEOUT_STEP_FIXTURE = fileURLToPath(
	new URL("./fixtures/long-timeout-step.ts", import.meta.url),
);

class CountingDriver extends InMemoryDriver {
	batchCalls = 0;

	async batch(
		writes: { key: Uint8Array; value: Uint8Array }[],
	): Promise<void> {
		this.batchCalls += 1;
		await super.batch(writes);
	}
}

for (const mode of modes) {
	describe(`Workflow Engine Steps (${mode})`, { sequential: true }, () => {
		let driver: CountingDriver;

		beforeEach(() => {
			driver = new CountingDriver();
			driver.latency = 0;
		});

		it("should execute a simple step", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				const result = await ctx.step("my-step", async () => {
					return "hello world";
				});
				return result;
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toBe("hello world");
		});

		it("should replay step on restart", async () => {
			let callCount = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				const result = await ctx.step("my-step", async () => {
					callCount++;
					return "hello";
				});
				return result;
			};

			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;
			expect(callCount).toBe(1);

			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;
			expect(callCount).toBe(1);
		});
		it("should reject a renamed root step on replay", async () => {
			const originalWorkflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step("original-step-name", async () => "original");
			};
			const renamedWorkflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step("renamed-step-name", async () => "changed");
			};

			await runWorkflow("wf-1", originalWorkflow, undefined, driver, { mode })
				.result;

			await expect(
				runWorkflow("wf-1", renamedWorkflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(HistoryDivergedError);
			const storage = await loadStorage(driver);
			expect(storage.nameRegistry).toEqual(["original-step-name"]);
			expect([...storage.history.entries.keys()]).toEqual([
				"original-step-name",
			]);
			expect(storage.state).not.toBe("completed");
		});

		it("should replay void step on restart", async () => {
			let callCount = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				const result = await ctx.step("void-step", async () => {
					callCount++;
				});
				return result;
			};

			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;
			expect(callCount).toBe(1);

			await runWorkflow("wf-1", workflow, undefined, driver, { mode })
				.result;
			expect(callCount).toBe(1);
		});

		it("should execute multiple steps in sequence", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				const a = await ctx.step("step-a", async () => 1);
				const b = await ctx.step("step-b", async () => 2);
				const c = await ctx.step("step-c", async () => 3);
				return a + b + c;
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toBe(6);
		});

		it("should retry failed steps", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "flaky-step",
					maxRetries: 3,
					retryBackoffBase: 1,
					retryBackoffMax: 10,
					run: async () => {
						attempts++;
						if (attempts < 3) {
							throw new Error("Transient failure");
						}
						return "success";
					},
				});
			};

			try {
				await runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result;
			} catch {}

			try {
				await runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result;
			} catch {}

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toBe("success");
			expect(attempts).toBe(3);
		});

		it("should report retryable step failures to onError", async () => {
			let attempts = 0;
			const events: WorkflowErrorEvent[] = [];

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "flaky-step",
					maxRetries: 3,
					retryBackoffBase: 5,
					retryBackoffMax: 5,
					run: async () => {
						attempts++;
						if (attempts === 1) {
							throw new Error("Transient failure");
						}
						return "ok";
					},
				});
			};

			const firstResult = await runWorkflow(
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

			if (mode === "yield") {
				expect(firstResult.state).toBe("sleeping");
				await new Promise((resolve) => setTimeout(resolve, 10));
				const secondResult = await runWorkflow(
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
				expect(secondResult.state).toBe("completed");
				expect(secondResult.output).toBe("ok");
			} else {
				expect(firstResult.state).toBe("completed");
				expect(firstResult.output).toBe("ok");
			}

			expect(events).toHaveLength(1);
			expect(events[0]).toMatchObject({
				step: {
					stepName: "flaky-step",
					attempt: 1,
					maxRetries: 3,
					remainingRetries: 3,
					willRetry: true,
					retryDelay: 5,
					error: {
						name: "Error",
						message: "Transient failure",
					},
				},
			});
			expect("step" in events[0] && events[0].step.retryAt).toBeTypeOf(
				"number",
			);
		});

		it("should yield during backoff retries", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "always-fails",
					maxRetries: 3,
					retryBackoffBase: 50,
					retryBackoffMax: 100,
					run: async () => {
						attempts++;
						throw new Error("Failure");
					},
				});
			};

			if (mode === "yield") {
				const result1 = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(result1.state).toBe("sleeping");
				expect(result1.sleepUntil).toBeDefined();

				const result2 = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(result2.state).toBe("sleeping");
				expect(result2.sleepUntil).toBeDefined();
				expect(result2.sleepUntil).toBeGreaterThan(Date.now());
				expect(driver.getAlarm("wf-1")).toBe(result2.sleepUntil);
				expect(attempts).toBe(1);
				return;
			}

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(StepExhaustedError);
			expect(attempts).toBe(4);
		});

		it("should not retry CriticalError", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step("critical-step", async () => {
					attempts++;
					throw new CriticalError("Unrecoverable");
				});
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(CriticalError);

			expect(attempts).toBe(1);
		});

		it("should report terminal step failures to onError", async () => {
			const events: WorkflowErrorEvent[] = [];

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step("critical-step", async () => {
					throw new CriticalError("Unrecoverable");
				});
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, {
					mode,
					onError: (event) => {
						events.push(event);
					},
				}).result,
			).rejects.toThrow(CriticalError);

			expect(events).toEqual([
				expect.objectContaining({
					step: expect.objectContaining({
						stepName: "critical-step",
						attempt: 1,
						willRetry: false,
						error: expect.objectContaining({
							name: "CriticalError",
							message: "Unrecoverable",
						}),
					}),
				}),
			]);
		});

		it("should not re-report exhausted step failures on rerun", async () => {
			const events: WorkflowErrorEvent[] = [];

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "always-fails",
					maxRetries: 0,
					retryBackoffBase: 0,
					retryBackoffMax: 0,
					run: async () => {
						throw new Error("boom");
					},
				});
			};

			const runOnce = async () =>
				await runWorkflow("wf-1", workflow, undefined, driver, {
					mode,
					onError: (event) => {
						events.push(event);
					},
				}).result;

			await expect(runOnce()).rejects.toThrow(StepExhaustedError);
			await expect(runOnce()).rejects.toThrow(StepExhaustedError);

			expect(events).toHaveLength(1);
			expect(events[0]).toMatchObject({
				step: {
					stepName: "always-fails",
					attempt: 1,
					willRetry: false,
					error: {
						name: "Error",
						message: "boom",
					},
				},
			});
		});

		it("should report workflow-level failures to onError", async () => {
			const events: WorkflowErrorEvent[] = [];

			const workflow = async (_ctx: WorkflowContextInterface) => {
				throw new Error("workflow failed");
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, {
					mode,
					onError: (event) => {
						events.push(event);
					},
				}).result,
			).rejects.toThrow("workflow failed");

			expect(events).toEqual([
				expect.objectContaining({
					workflow: expect.objectContaining({
						error: expect.objectContaining({
							name: "Error",
							message: "workflow failed",
						}),
					}),
				}),
			]);
		});

		it("should not retry RollbackError", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step("rollback-step", async () => {
					attempts++;
					throw new RollbackError("Rollback now");
				});
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(RollbackError);

			expect(attempts).toBe(1);
		});

		it("should exhaust retries", async () => {
			let _attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "always-fails",
					maxRetries: 2,
					retryBackoffBase: 1,
					run: async () => {
						_attempts++;
						throw new Error("Always fails");
					},
				});
			};

			if (mode === "yield") {
				const firstResult = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(firstResult.state).toBe("sleeping");
				await new Promise((resolve) => setTimeout(resolve, 10));

				const secondResult = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(secondResult.state).toBe("sleeping");
				await new Promise((resolve) => setTimeout(resolve, 10));
			}

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(StepExhaustedError);
		});

		it("should recover exhausted retries", async () => {
			let attempts = 0;

			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "recoverable",
					maxRetries: 1,
					retryBackoffBase: 1,
					run: async () => {
						attempts++;
						throw new Error("Always fails");
					},
				});
			};

			const runOnce = () =>
				runWorkflow("wf-1", workflow, undefined, driver, { mode });

			if (mode === "yield") {
				const firstResult = await runOnce().result;
				expect(firstResult.state).toBe("sleeping");
			}

			const exhaustedHandle = runOnce();
			await expect(exhaustedHandle.result).rejects.toThrow(
				StepExhaustedError,
			);

			const attemptsAfterExhaust = attempts;
			await exhaustedHandle.recover();

			if (mode === "yield") {
				const resumedResult = await runOnce().result;
				expect(resumedResult.state).toBe("sleeping");
			}

			await expect(runOnce().result).rejects.toThrow(StepExhaustedError);

			expect(attempts).toBeGreaterThan(attemptsAfterExhaust);
		});

		it("should fail steps that exceed timeout", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "timeout-step",
					timeout: 5,
					run: async () => {
						await new Promise((resolve) => setTimeout(resolve, 25));
						return "late";
					},
				});
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(CriticalError);
		});

		for (const [label, timeout] of [
			["30 days", THIRTY_DAYS_MS],
			["Infinity", Number.POSITIVE_INFINITY],
			["0", 0],
			["-1", -1],
			["NaN", Number.NaN],
		] as const) {
			it(`should finish steps with a timeout of ${label}`, async () => {
				const workflow = async (ctx: WorkflowContextInterface) => {
					return await ctx.step({
						name: "long-timeout-step",
						timeout,
						run: async () => {
							await new Promise((resolve) => setTimeout(resolve, 25));
							return "done";
						},
					});
				};

				const result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(result.state).toBe("completed");
				expect(result.output).toBe("done");
			});
		}

		for (const [label, timeout] of [
			["2^31 ms", JUST_OVER_SET_TIMEOUT_LIMIT_MS],
			["30 days", THIRTY_DAYS_MS],
		] as const) {
			it(`should time out a step when a timeout of ${label} elapses`, async () => {
				vi.useFakeTimers();
				try {
					const workflow = async (ctx: WorkflowContextInterface) => {
						return await ctx.step({
							name: "stuck-step",
							timeout,
							run: () => new Promise<string>(() => {}),
						});
					};

					const result = runWorkflow(
						"wf-1",
						workflow,
						undefined,
						driver,
						{ mode },
					).result;
					let settled = false;
					result.then(
						() => {
							settled = true;
						},
						() => {
							settled = true;
						},
					);

					await vi.advanceTimersByTimeAsync(timeout - 1000);
					expect(settled).toBe(false);

					await vi.advanceTimersByTimeAsync(2000);
					await expect(result).rejects.toThrow(CriticalError);
					await expect(result).rejects.toThrow(
						`Step "stuck-step" timed out after ${timeout}ms`,
					);
				} finally {
					vi.useRealTimers();
				}
			});
		}

		it("should exit the process when a step finishes before a long timeout", async () => {
			// A timer left behind keeps the process alive for 24.8 days.
			const { stdout } = await execFileAsync(
				process.execPath,
				["--import", "tsx", LONG_TIMEOUT_STEP_FIXTURE, mode],
				{ timeout: 8000 },
			);

			expect(JSON.parse(stdout)).toEqual({
				state: "completed",
				output: "done",
			});
		});

		it("should leave no abort listeners on ctx.abortSignal after many steps", async () => {
			// Node never warns about AbortSignal listeners, so count them.
			const workflow = async (ctx: WorkflowContextInterface) => {
				const listenerCount = () =>
					getEventListeners(ctx.abortSignal, "abort").length;
				const before = listenerCount();
				for (let i = 0; i < 30; i++) {
					await ctx.step({
						name: `step-${i}`,
						timeout: THIRTY_DAYS_MS,
						run: async () => i,
					});
				}
				const after = listenerCount();
				return { before, after };
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output?.after).toBe(result.output?.before);
		});

		for (const retryOnTimeout of [false, true]) {
			it(`should retry a step that throws when retryOnTimeout is ${retryOnTimeout}`, async () => {
				let attempts = 0;
				const workflow = async (ctx: WorkflowContextInterface) => {
					return await ctx.step({
						name: "flaky-step",
						timeout: 1000,
						retryOnTimeout,
						maxRetries: 3,
						retryBackoffBase: 1,
						retryBackoffMax: 1,
						run: async () => {
							attempts++;
							if (attempts < 3) {
								throw new Error("Transient failure");
							}
							return "success";
						},
					});
				};

				let result = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				while (result.state === "sleeping") {
					await new Promise((resolve) => setTimeout(resolve, 5));
					result = await runWorkflow(
						"wf-1",
						workflow,
						undefined,
						driver,
						{ mode },
					).result;
				}

				expect(result.state).toBe("completed");
				expect(result.output).toBe("success");
				expect(attempts).toBe(3);
			});
		}

		it("should retry steps that exceed timeout when retryOnTimeout is set", async () => {
			let attempts = 0;
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "timeout-step",
					timeout: 5,
					retryOnTimeout: true,
					maxRetries: 2,
					retryBackoffBase: 1,
					retryBackoffMax: 1,
					run: async () => {
						attempts++;
						await new Promise((resolve) => setTimeout(resolve, 25));
						return "late";
					},
				});
			};

			if (mode === "yield") {
				const firstResult = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(firstResult.state).toBe("sleeping");
				await new Promise((resolve) => setTimeout(resolve, 10));

				const secondResult = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;
				expect(secondResult.state).toBe("sleeping");
				await new Promise((resolve) => setTimeout(resolve, 10));
			}

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(StepExhaustedError);

			expect(attempts).toBe(3);
		});

		it("should fail when a step is not awaited", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				const first = ctx.step("step-a", async () => "a");
				await ctx.step("step-b", async () => "b");
				return await first;
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(EntryInProgressError);
		});

		it("should reject duplicate entry names", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				await ctx.step("dup", async () => "first");
				await ctx.step("dup", async () => "second");
				return "done";
			};

			await expect(
				runWorkflow("wf-1", workflow, undefined, driver, { mode })
					.result,
			).rejects.toThrow(HistoryDivergedError);
		});

		it("should batch ephemeral steps until a durable flush", async () => {
			let batchesBeforeDurable = -1;
			const workflow = async (ctx: WorkflowContextInterface) => {
				await ctx.step({
					name: "ephemeral-a",
					ephemeral: true,
					run: async () => "a",
				});
				await ctx.step({
					name: "ephemeral-b",
					ephemeral: true,
					run: async () => "b",
				});
				batchesBeforeDurable = driver.batchCalls;
				return await ctx.step("durable", async () => "done");
			};

			const result = await runWorkflow(
				"wf-1",
				workflow,
				undefined,
				driver,
				{ mode },
			).result;

			expect(result.state).toBe("completed");
			expect(result.output).toBe("done");
			// Ephemeral steps do not save immediately. The durable step and workflow
			// completion may result in up to three writes.
			expect(batchesBeforeDurable).toBe(0);
			expect(driver.batchCalls).toBeLessThanOrEqual(3);
			const storage = await loadStorage(driver);
			for (const [name, output] of [
				["ephemeral-a", "a"],
				["ephemeral-b", "b"],
			]) {
				expect(storage.history.entries.get(name)?.kind).toMatchObject({
					type: "step",
					data: { output },
				});
			}
		});

		it("should commit error but not output to step entry on failure", async () => {
			const workflow = async (ctx: WorkflowContextInterface) => {
				return await ctx.step({
					name: "fail-once",
					maxRetries: 3,
					retryBackoffBase: 0,
					retryBackoffMax: 0,
					run: async () => {
						throw new Error("step failed");
					},
				});
			};

			// Run once so the step fails and state is flushed
			try {
				await runWorkflow("wf-1", workflow, undefined, driver, {
					mode,
				}).result;
			} catch {}

			// Inspect all history entries in KV
			const historyPrefix = buildHistoryPrefixAll();
			const entries = await driver.list(historyPrefix);

			for (const kv of entries) {
				const entry = deserializeEntry(kv.value);
				if (entry.kind.type === "step") {
					// The step entry should have no output committed on failure.
					expect(entry.kind.data.output).toBeUndefined();
					// The error should be committed for inspection.
					expect(entry.kind.data.error).toBe("Error: step failed");
				}
			}
		});
	});
}
