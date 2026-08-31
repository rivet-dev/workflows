import { setup, workflow } from "@rivet-dev/workflows";
export const paymentActor = workflow({
	state: {
		status: "pending" as "pending" | "manual-review" | "paid",
		reason: null as string | null,
	},
	run: async (ctx) => {
		const charge = await ctx.tryStep({
			name: "charge-card",
			maxRetries: 3,
			run: async (_ctx) => await chargeCard("order-123"),
		});
		await ctx.step("store-charge-result", async (step) => {
			if (!charge.ok) {
				step.state.status = "manual-review";
				step.state.reason = charge.failure.error.message;
				return;
			}
			step.state.status = "paid";
			step.state.reason = null;
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
async function chargeCard(orderId: string): Promise<string> {
	return `charge-${orderId}`;
}
export const registry = setup({ use: { paymentActor } });
