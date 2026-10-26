import { randomUUID } from "node:crypto";
import { serve } from "@hono/node-server";
import { setup } from "@rivet-dev/workflows";
import { Hono } from "hono";
import { createClient } from "rivetkit/client";
import { endpoint, type WebhookEvent } from "./endpoint";
import { createSecret } from "./sign";

export const registry = setup({ use: { endpoint } });
const client = createClient<typeof registry>();

registry.start();

// Which endpoints get which event types. Keep this in your database.
const subscriptions = new Map<string, Set<string>>();

// docs:start emit
// Call this from your app whenever something happens that customers can subscribe to.
export async function emit(type: string, data: unknown): Promise<WebhookEvent> {
	const event: WebhookEvent = {
		id: `msg_${randomUUID()}`,
		type,
		data,
		createdAt: Date.now(),
	};
	for (const endpointId of subscriptions.get(type) ?? []) {
		await client.endpoint.get([endpointId]).send("events", event);
	}
	return event;
}
// docs:end emit

const app = new Hono();

// docs:start register
// Your customer portal calls this when a customer adds an endpoint.
app.post("/endpoints", async (c) => {
	const { url, eventTypes } = await c.req.json<{
		url: string;
		eventTypes: string[];
	}>();
	const id = `ep_${randomUUID()}`;
	const secret = createSecret();
	await client.endpoint.create([id], { input: { url, secret } });
	for (const type of eventTypes) {
		if (!subscriptions.has(type)) subscriptions.set(type, new Set());
		subscriptions.get(type)?.add(id);
	}
	// Show the secret to the customer once so they can check signatures.
	return c.json({ id, secret });
});
// docs:end register

// docs:start portal
app.get("/endpoints/:id/attempts", async (c) => {
	return c.json(await client.endpoint.get([c.req.param("id")]).listAttempts());
});

app.post("/endpoints/:id/enable", async (c) => {
	await client.endpoint.get([c.req.param("id")]).enable();
	return c.json({ enabled: true });
});
// docs:end portal

// Stands in for your app code, so you can emit test events over HTTP.
app.post("/events", async (c) => {
	const { type, data } = await c.req.json<{ type: string; data: unknown }>();
	return c.json(await emit(type, data));
});

serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
