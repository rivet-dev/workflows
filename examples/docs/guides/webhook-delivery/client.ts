import type { Attempt } from "./endpoint";
import { startReceiver } from "./receiver";

const api = process.env.API_URL ?? "http://localhost:3000";
const receiverPort = Number(process.env.RECEIVER_PORT ?? 4000);
// Set FAIL_FIRST=100 to watch the endpoint run out of retries and get disabled.
const failFirst = Number(process.env.FAIL_FIRST ?? 3);

async function call<T>(path: string, body?: unknown): Promise<T> {
	const res = await fetch(`${api}${path}`, {
		method: body === undefined ? "GET" : "POST",
		headers: { "content-type": "application/json" },
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	return (await res.json()) as T;
}

// Add an endpoint the way a customer would in your portal, then start a
// receiver that checks signatures with its secret.
const { id, secret } = await call<{ id: string; secret: string }>(
	"/endpoints",
	{
		url: `http://localhost:${receiverPort}/webhooks`,
		eventTypes: ["order.created"],
	},
);
const receiver = startReceiver({ port: receiverPort, secret, failFirst });
console.log(`endpoint ${id}`);

// Emit two events. The second one waits until the first is delivered.
await call("/events", { type: "order.created", data: { orderId: "order_1" } });
await call("/events", { type: "order.created", data: { orderId: "order_2" } });

// Print the delivery log as a portal would, until both events are delivered.
let printed = 0;
for (;;) {
	await new Promise((resolve) => setTimeout(resolve, 1000));
	const log = await call<{
		disabled: boolean;
		delivered: number;
		attempts: Attempt[];
	}>(`/endpoints/${id}/attempts`);
	for (const a of log.attempts.toReversed().slice(printed)) {
		const time = new Date(a.at).toISOString().slice(11, 19);
		console.log(
			`${time} ${a.eventId.slice(0, 12)} attempt ${a.attempt}: ${a.status ?? a.error} (${a.durationMs}ms)`,
		);
	}
	printed = log.attempts.length;
	if (log.delivered === 2) break;
	if (log.disabled) {
		console.log(
			"endpoint disabled. Fixing the receiver and turning the endpoint back on.",
		);
		receiver.recover();
		await call(`/endpoints/${id}/enable`, {});
	}
}

console.log("both events delivered");
receiver.close();
