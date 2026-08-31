import {
	setup,
	type WorkflowStepContextOf,
	workflow,
} from "@rivet-dev/workflows";

type ControlSignal = {
	kind: "pause" | "resume" | "stop";
};
export const controlLoopActor = workflow({
	state: {
		mode: "running" as "running" | "paused" | "stopped",
		handledSignals: 0,
	},
	run: async (ctx) => {
		await ctx.loop("control-loop", async (loopCtx) => {
			const [message] = await loopCtx.queue.nextBatch("wait-signal", {
				timeout: 30000,
			});
			if (!message) return;
			const signal = message.body as ControlSignal;
			await loopCtx.step("apply-signal", async (step) =>
				applyControlSignal(step, signal.kind),
			);
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
function applyControlSignal(
	ctx: WorkflowStepContextOf<typeof controlLoopActor>,
	kind: ControlSignal["kind"],
): void {
	ctx.state.handledSignals += 1;
	if (kind === "pause") ctx.state.mode = "paused";
	if (kind === "resume") ctx.state.mode = "running";
	if (kind === "stop") ctx.state.mode = "stopped";
}
export const registry = setup({ use: { controlLoopActor } });
