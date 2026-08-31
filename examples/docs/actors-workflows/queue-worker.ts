import { setup, workflow } from "@rivet-dev/workflows";

type Job = {
	id: string;
	amount: number;
};
export const queueWorkerActor = workflow({
	state: {
		processed: 0,
		totalAmount: 0,
	},
	run: async (ctx) => {
		await ctx.loop("worker-loop", async (loopCtx) => {
			const [message] = await loopCtx.queue.nextBatch("wait-job", {
				timeout: 30000,
			});
			if (!message) return;
			const job = message.body as Job;
			await loopCtx.step("process-job", async (step) => {
				step.state.processed += 1;
				step.state.totalAmount += job.amount;
			});
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
export const registry = setup({ use: { queueWorkerActor } });
