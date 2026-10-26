import { createClient } from "rivetkit/client";
import type { registry } from "./server";
import { type StripeEvent, signStripePayload } from "./stripe";

const secret = process.env.STRIPE_WEBHOOK_SECRET;
if (!secret) {
	throw new Error("Set STRIPE_WEBHOOK_SECRET to the server's value.");
}
const url = process.env.WEBHOOK_URL ?? "http://localhost:3000/webhooks";

const event: StripeEvent = {
	id: `evt_test_${Date.now()}`,
	type: "customer.subscription.updated",
	created: Math.floor(Date.now() / 1000),
	data: { object: { id: "sub_123", customer: "cus_123", status: "active" } },
};
const body = JSON.stringify(event);

// Post the same signed event twice, the way a provider redelivers an event
// when it does not get your answer.
for (const label of ["delivery", "redelivery"]) {
	const res = await fetch(url, {
		method: "POST",
		headers: {
			"content-type": "application/json",
			"stripe-signature": signStripePayload(body, secret),
		},
		body,
	});
	console.log(`${label}: ${res.status} ${await res.text()}`);
}

// A request with a bad signature is rejected before it reaches the workflow.
const forged = await fetch(url, {
	method: "POST",
	headers: {
		"content-type": "application/json",
		"stripe-signature": "t=0,v1=00",
	},
	body,
});
console.log(`forged: ${forged.status} ${await forged.text()}`);

// Give the workflow a moment to drain its queue, then read its counts.
await new Promise((resolve) => setTimeout(resolve, 1000));
const client = createClient<typeof registry>();
console.log(await client.customerEvents.getOrCreate(["cus_123"]).getStatus());
