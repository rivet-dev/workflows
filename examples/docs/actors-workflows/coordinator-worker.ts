import {
	actor,
	setup,
	type WorkflowStepContextOf,
	workflow,
} from "@rivet-dev/workflows";

type TaskMessage = {
	taskId: string;
	workerId: string;
	value: number;
};
export const workerActor = actor({
	actions: {
		runTask: async (_c, value: number) => value * 2,
	},
});
export const coordinatorActor = workflow({
	state: {
		lastTaskId: null as string | null,
		lastResult: 0,
	},
	run: async (ctx) => {
		await ctx.loop("orchestrator-loop", async (loopCtx) => {
			const [message] = await loopCtx.queue.nextBatch("wait-task", {
				timeout: 30000,
			});
			if (!message) return;
			const task = message.body as TaskMessage;
			const result = await loopCtx.step("dispatch-rpc", async (step) =>
				dispatchTask(step, task),
			);
			await loopCtx.step("record-result", async (step) => {
				step.state.lastTaskId = task.taskId;
				step.state.lastResult = result as number;
			});
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
async function dispatchTask(
	ctx: WorkflowStepContextOf<typeof coordinatorActor>,
	task: TaskMessage,
): Promise<number> {
	const client = ctx.client();
	const worker = client.workerActor.getOrCreate([task.workerId]);
	return await worker.runTask(task.value);
}
export const registry = setup({ use: { coordinatorActor, workerActor } });
