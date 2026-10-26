import { createHmac, timingSafeEqual } from "node:crypto";

// Posts a message to a Slack user or channel id. Without SLACK_BOT_TOKEN it only logs the message, for local testing.
export async function postMessage(
	channel: string,
	text: string,
	blocks?: unknown[],
) {
	const token = process.env.SLACK_BOT_TOKEN;
	if (!token) {
		console.log(`slack message to ${channel}: ${text}`);
		return;
	}
	const res = await fetch("https://slack.com/api/chat.postMessage", {
		method: "POST",
		headers: {
			authorization: `Bearer ${token}`,
			"content-type": "application/json; charset=utf-8",
		},
		body: JSON.stringify({ channel, text, blocks }),
	});
	const body = (await res.json()) as { ok: boolean; error?: string };
	if (!body.ok) throw new Error(`slack chat.postMessage failed: ${body.error}`);
}

// docs:start buttons
// A message with Approve and Reject buttons. Each button carries the request id back to /approvals.
export function approvalBlocks(requestId: string, text: string) {
	const button = (actionId: string, label: string, style: string) => ({
		type: "button",
		action_id: actionId,
		style,
		text: { type: "plain_text", text: label },
		value: requestId,
	});
	return [
		{ type: "section", text: { type: "mrkdwn", text } },
		{
			type: "actions",
			elements: [
				button("approve", "Approve", "primary"),
				button("reject", "Reject", "danger"),
			],
		},
	];
}
// docs:end buttons

// The parts of a Slack button click that /approvals reads.
export type SlackButtonClick = {
	user: { id: string };
	actions: Array<{ action_id: string; value: string }>;
	response_url: string;
};

// Replaces or adds to the message that held the clicked button.
export async function respond(
	responseUrl: string,
	body: Record<string, unknown>,
) {
	await fetch(responseUrl, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
	});
}

// Slack signs "v0:<timestamp>:<raw body>" with your app's signing secret.
export function verifySlackRequest(
	rawBody: string,
	timestamp: string | undefined,
	signature: string | undefined,
) {
	if (!timestamp || !signature) return false;
	if (Math.abs(Date.now() / 1000 - Number(timestamp)) > 60 * 5) return false;
	const expected = `v0=${createHmac(
		"sha256",
		process.env.SLACK_SIGNING_SECRET ?? "",
	)
		.update(`v0:${timestamp}:${rawBody}`)
		.digest("hex")}`;
	return (
		expected.length === signature.length &&
		timingSafeEqual(Buffer.from(expected), Buffer.from(signature))
	);
}
