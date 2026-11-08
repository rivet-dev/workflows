import { beforeEach, describe, expect, it } from "vitest";
import {
	EvictedError,
	InMemoryDriver,
	runWorkflow,
	type WorkflowContextInterface,
} from "../src/testing.js";

const modes = ["yield", "live"] as const;

function deferred<T>() {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((res) => {
		resolve = res;
	});
	return { promise, resolve };
}

for (const mode of modes) {
	describe(
		`Workflow Engine Eviction and Cancellation (${mode})`,
		{
			sequential: true,
		},
		() => {
			let driver: InMemoryDriver;

			beforeEach(() => {
				driver = new InMemoryDriver();
				driver.latency = 0;
			});

			it("should surface eviction through the abort event", async () => {
				const workflow = async (ctx: WorkflowContextInterface) => {
					await new Promise<void>((resolve) => {
						if (ctx.abortSignal.aborted) {
							resolve();
							return;
						}
						ctx.abortSignal.addEventListener(
							"abort",
							() => resolve(),
							{
								once: true,
							},
						);
					});
					return ctx.isEvicted();
				};

				const handle = runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode,
					},
				);
				handle.evict();

				const result = await handle.result;
				expect(result.state).toBe("completed");
				expect(result.output).toBe(true);
			});

			it("should cancel workflow and clear alarms", async () => {
				const workflow = async (_ctx: WorkflowContextInterface) => {
					return "done";
				};

				const handle = runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{
						mode,
					},
				);
				await driver.setAlarm("wf-1", Date.now() + 1000);

				await handle.cancel();

				await expect(handle.result).rejects.toThrow(EvictedError);
				expect(await handle.getState()).toBe("cancelled");
				expect(driver.getAlarm("wf-1")).toBeUndefined();

				await expect(
					runWorkflow("wf-1", workflow, undefined, driver, { mode })
						.result,
				).rejects.toThrow(EvictedError);
			});

			it("should keep the result of a step that finishes after eviction", async () => {
				const started = deferred<void>();
				const work = deferred<string>();
				let runs = 0;
				const workflow = async (ctx: WorkflowContextInterface) =>
					await ctx.step({
						name: "slow-step",
						timeout: 60_000,
						run: async () => {
							runs++;
							started.resolve();
							return await work.promise;
						},
					});

				const handle = runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				);
				await started.promise;
				handle.evict();
				work.resolve("finished after eviction");
				await handle.result;

				const resumed = await runWorkflow(
					"wf-1",
					workflow,
					undefined,
					driver,
					{ mode },
				).result;

				expect(resumed.state).toBe("completed");
				expect(resumed.output).toBe("finished after eviction");
				expect(runs).toBe(1);
			});
		},
	);
}
