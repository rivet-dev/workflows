// Tools that change something important. The approval workflow runs them only after a person approves.
// Each tool gets the approval request id, so your API can ignore a repeated call for the same request.
const tools = {
	refund_order: async (
		args: { orderId: string; amountCents: number },
		requestId: string,
	) => {
		const apiUrl = process.env.ORDERS_API_URL ?? "https://api.example.com";
		const res = await fetch(`${apiUrl}/orders/${args.orderId}/refunds`, {
			method: "POST",
			headers: {
				"content-type": "application/json",
				"idempotency-key": requestId,
			},
			body: JSON.stringify({ amountCents: args.amountCents }),
		});
		if (!res.ok) throw new Error(`refund failed with status ${res.status}`);
		return (await res.json()) as { refundId: string };
	},
};

type Tools = typeof tools;

// A tool name with matching arguments, such as { tool: "refund_order", args: { orderId, amountCents } }.
export type ToolCall = {
	[K in keyof Tools]: { tool: K; args: Parameters<Tools[K]>[0] };
}[keyof Tools];

export async function runTool(call: ToolCall, requestId: string) {
	return await tools[call.tool](call.args, requestId);
}
