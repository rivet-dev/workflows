import { randomUUID } from "node:crypto";
import { createClient } from "rivetkit/client";
import type { ApprovalRequest, Outcome } from "./approval";
import type { registry } from "./server";

const client = createClient<typeof registry>();

// docs:start request-approval
// Starts an approval workflow for a new request and resolves when it finishes.
export async function requestApproval(
	request: ApprovalRequest,
): Promise<Outcome> {
	const handle = client.approval.getOrCreate([randomUUID()]);
	const conn = handle.connect();
	try {
		const finished = new Promise<Outcome>((resolve) => {
			conn.once("finished", (outcome) => resolve(outcome as Outcome));
		});
		await handle.send("request", request);
		return await finished;
	} finally {
		await conn.dispose();
	}
}
// docs:end request-approval

// docs:start gated-tool
// An agent tool that asks a person before it refunds an order.
export const refundOrderTool = {
	name: "refund_order",
	description:
		"Refund part or all of an order. A person approves every refund before it happens.",
	parameters: {
		type: "object",
		properties: {
			orderId: { type: "string" },
			amountCents: { type: "number" },
			reason: { type: "string" },
		},
		required: ["orderId", "amountCents", "reason"],
	},
	execute: async (args: {
		orderId: string;
		amountCents: number;
		reason: string;
	}) => {
		const outcome = await requestApproval({
			tool: "refund_order",
			args: { orderId: args.orderId, amountCents: args.amountCents },
			summary: `Refund $${(args.amountCents / 100).toFixed(2)} on order ${args.orderId}. Reason: ${args.reason}`,
			requestedBy: process.env.AGENT_SLACK_CHANNEL ?? "C0AGENT",
			approver: process.env.APPROVER_SLACK_ID ?? "U0APPROVER",
		});
		if (outcome.status === "approved")
			return `Refund done: ${JSON.stringify(outcome.result)}`;
		return `No refund was made. The request ended as ${outcome.status}.`;
	},
};
// docs:end gated-tool
