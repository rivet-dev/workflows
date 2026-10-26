import { serve } from "@hono/node-server";
import { setup } from "@rivet-dev/workflows";
import { Hono } from "hono";
import { createClient } from "rivetkit/client";
import { customerEvents } from "./customer-events";
import { type StripeEvent, verifyStripeSignature } from "./stripe";

const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;
if (!webhookSecret) {
	throw new Error("Set STRIPE_WEBHOOK_SECRET to your signing secret.");
}

export const registry = setup({ use: { customerEvents } });
const client = createClient<typeof registry>();

registry.start();

const app = new Hono();

// docs:start webhooks
app.post("/webhooks", async (c) => {
	// Check the signature against the raw body. Parsed and re-encoded JSON
	// would not match the bytes the provider signed.
	const rawBody = await c.req.text();
	const signature = c.req.header("stripe-signature");
	if (!verifyStripeSignature(rawBody, signature, webhookSecret)) {
		return c.text("invalid signature", 400);
	}

	const event = JSON.parse(rawBody) as StripeEvent;
	const customerId = event.data.object.customer ?? event.data.object.id;

	// `send` returns once the event is stored in the customer's queue,
	// before the workflow handles it.
	await client.customerEvents.getOrCreate([customerId]).send("events", event);
	return c.json({ received: true });
});
// docs:end webhooks

serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
