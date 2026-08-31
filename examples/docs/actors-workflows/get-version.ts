import { setup, workflow } from "@rivet-dev/workflows";
export const versionGateActor = workflow({
	state: {
		processed: 0,
	},
	run: async (ctx) => {
		await ctx.loop("process-loop", async (loopCtx) => {
			// Gate the changed code path. Each loop iteration resolves its version
			// independently: an iteration that already ran under the old code
			// (in-flight across the deploy) resolves to version 1, while a fresh
			// iteration resolves to `latest` (2 here). The resolved value is pinned
			// in history, so replays stay deterministic.
			const version = await loopCtx.getVersion("process-message", 2);
			if (version === 1) {
				// Preserve the original behavior for in-flight iterations.
				await loopCtx.step("process-v1", async (step) => {
					step.state.processed += 1;
				});
			} else {
				// New behavior for iterations that begin after this deploy.
				await loopCtx.step("process-v2", async (step) => {
					step.state.processed += 1;
				});
			}
			await loopCtx.sleep("idle", 1000);
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
export const registry = setup({ use: { versionGateActor } });
