import { queue, setup, workflow } from "@rivet-dev/workflows";

async function chargeCard(orderId: string): Promise<string> {
	return `charge-${orderId}`;
}
export const timeoutActor = workflow({
	state: {
		lastChargeId: null as string | null,
	},
	queues: {
		charge: queue<{
			orderId: string;
		}>(),
	},
	run: async (ctx) => {
		await ctx.loop("charge-loop", async (loopCtx) => {
			const message = await loopCtx.queue.next("wait-charge");
			const chargeId = await loopCtx.step<string>({
				name: "charge-card",
				timeout: 5000,
				retryOnTimeout: true,
				maxRetries: 5,
				retryBackoffBase: 200,
				retryBackoffMax: 2000,
				run: async (_loopCtx) => await chargeCard(message.body.orderId),
			});
			await loopCtx.step("save-charge", async (step) => {
				step.state.lastChargeId = chargeId;
			});
		});
	},
});
export const registry = setup({ use: { timeoutActor } });
