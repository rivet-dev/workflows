import {
	queue,
	setup,
	type WorkflowStepContextOf,
	workflow,
} from "@rivet-dev/workflows";
export const workflowCounter = workflow({
	state: {
		value: 0,
		processed: 0,
		lastOperationId: null as string | null,
	},
	queues: {
		counter: queue<{
			delta: number;
		}>(),
	},
	run: async (ctx) => {
		await ctx.loop("counter-loop", async (loopCtx) => {
			const message = await loopCtx.queue.next("wait-counter-command");
			await loopCtx.step("apply-counter-command", async (step) =>
				applyCounterCommand(step, message.body.delta),
			);
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
async function applyCounterCommand(
	ctx: WorkflowStepContextOf<typeof workflowCounter>,
	delta: number,
): Promise<void> {
	const response = await fetch("https://api.example.com/counter/apply", {
		method: "POST",
		headers: {
			"content-type": "application/json",
		},
		body: JSON.stringify({ delta }),
	});
	if (!response.ok) {
		throw new Error(`counter apply failed: ${response.status}`);
	}
	const result = (await response.json()) as {
		nextValue: number;
		operationId: string;
	};
	ctx.state.value = result.nextValue;
	ctx.state.lastOperationId = result.operationId;
	ctx.state.processed += 1;
}
export const registry = setup({ use: { workflowCounter } });
