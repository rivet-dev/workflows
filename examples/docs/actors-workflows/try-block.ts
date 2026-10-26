import { setup, workflow } from "@rivet-dev/workflows";
export const paymentFlowActor = workflow({
	state: {
		auth: null as string | null,
		capture: null as string | null,
		failed: false,
	},
	run: async (ctx) => {
		const result = await ctx.try("payment-flow", async (blockCtx) => {
			const auth = await blockCtx.step("authorize", async (step) => {
				const auth = await authorizeOrder("order-123");
				step.state.auth = auth;
				return auth;
			});
			const capture = await blockCtx.step("capture", async (step) => {
				const capture = await captureOrder("order-123");
				step.state.capture = capture;
				return capture;
			});
			return { auth, capture };
		});
		if (!result.ok) {
			await ctx.step("mark-failed", async (step) => {
				step.state.failed = true;
			});
		}
	},
	actions: {
		getState: (c) => c.state,
	},
});
async function authorizeOrder(orderId: string): Promise<string> {
	return `auth-${orderId}`;
}
async function captureOrder(orderId: string): Promise<string> {
	return `capture-${orderId}`;
}
export const registry = setup({ use: { paymentFlowActor } });
