import { setup, workflow } from "@rivet-dev/workflows";

async function pollExternal(attempt: number): Promise<boolean> {
	return attempt % 3 === 0;
}
export const pollBackoffActor = workflow({
	state: {
		attempts: 0,
		backoffMs: 100,
		status: "unknown" as "unknown" | "healthy" | "retrying",
	},
	run: async (ctx) => {
		await ctx.loop("poll-loop", async (loopCtx) => {
			const success = await loopCtx.step("poll-target", async (step) => {
				step.state.attempts += 1;
				return pollExternal(step.state.attempts);
			});
			if (success) {
				await loopCtx.step("reset-backoff", async (step) => {
					step.state.status = "healthy";
					step.state.backoffMs = 100;
				});
				await loopCtx.sleep("healthy-interval", 1000);
				return;
			}
			const retryDelay = await loopCtx.step("grow-backoff", async (ctx) => {
				ctx.state.status = "retrying";
				ctx.state.backoffMs = Math.min(ctx.state.backoffMs * 2, 5000);
				return ctx.state.backoffMs;
			});
			await loopCtx.sleep("retry-delay", retryDelay);
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
export const registry = setup({ use: { pollBackoffActor } });
