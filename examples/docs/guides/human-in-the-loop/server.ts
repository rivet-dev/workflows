import { serve } from "@hono/node-server";
import { setup } from "@rivet-dev/workflows";
import { Hono } from "hono";
import { createClient } from "rivetkit/client";
import { approval } from "./approval";
import { respond, type SlackButtonClick, verifySlackRequest } from "./slack";

export const registry = setup({ use: { approval } });
const client = createClient<typeof registry>();

registry.start();

const app = new Hono();

// docs:start approvals
// Slack calls this when the approver clicks Approve or Reject.
app.post("/approvals", async (c) => {
	const rawBody = await c.req.text();
	const signed = verifySlackRequest(
		rawBody,
		c.req.header("x-slack-request-timestamp"),
		c.req.header("x-slack-signature"),
	);
	if (!signed) return c.text("invalid signature", 401);

	const click = JSON.parse(
		new URLSearchParams(rawBody).get("payload") ?? "{}",
	) as SlackButtonClick;
	const [button] = click.actions;
	const handle = client.approval.get([button.value]);

	// The pending request is readable while the workflow waits.
	const { status, request } = await handle.getStatus();
	if (status !== "pending") {
		await respond(click.response_url, {
			response_type: "ephemeral",
			replace_original: false,
			text: `This request is already ${status}.`,
		});
		return c.body(null, 200);
	}
	if (click.user.id !== request?.approver) {
		await respond(click.response_url, {
			response_type: "ephemeral",
			replace_original: false,
			text: "Only the assigned approver can answer.",
		});
		return c.body(null, 200);
	}

	const approved = button.action_id === "approve";
	await handle.send("decision", { approved, decidedBy: click.user.id });
	// Replace the buttons so nobody clicks twice.
	await respond(click.response_url, {
		replace_original: true,
		text: `${approved ? "Approved" : "Rejected"} by <@${click.user.id}>.`,
	});
	return c.body(null, 200);
});
// docs:end approvals

serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
