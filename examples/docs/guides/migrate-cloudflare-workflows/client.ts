import { createClient } from "rivetkit/client";
import type { Progress, registry } from "./order";

const serverUrl = process.env.SERVER_URL ?? "http://localhost:3000";
const client = createClient<typeof registry>();

const orderId = crypto.randomUUID();
await fetch(`${serverUrl}/orders`, {
	method: "POST",
	headers: { "content-type": "application/json" },
	body: JSON.stringify({
		orderId,
		email: "buyer@example.com",
		amountCents: 4200,
	}),
});

// docs:start progress
// Subscribe to progress instead of polling the status.
const order = client.orderWorkflow.get([orderId]);
const conn = order.connect();
conn.on("progress", (event) => {
	console.log("step", (event as Progress).step);
});
// docs:end progress

console.log(await order.getStatus());
