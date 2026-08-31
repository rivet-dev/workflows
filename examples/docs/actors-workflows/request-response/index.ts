import { queue, setup, workflow } from "@rivet-dev/workflows";
export const requestResponseActor = workflow({
	state: {
		handled: 0,
	},
	queues: {
		requests: queue<
			{
				value: number;
			},
			{
				doubled: number;
			}
		>(),
	},
	run: async (ctx) => {
		await ctx.loop("request-loop", async (loopCtx) => {
			const message = await loopCtx.queue.next("wait-request", {
				completable: true,
			});
			if (!message.complete) return;
			const doubled = await loopCtx.step("handle-request", async (step) => {
				step.state.handled += 1;
				return message.body.value * 2;
			});
			await message.complete({ doubled });
		});
	},
});
export const registry = setup({ use: { requestResponseActor } });
