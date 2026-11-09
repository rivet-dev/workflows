import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { createClient } from "rivetkit/client";
import { type OrderInput, registry } from "./order";

const client = createClient<typeof registry>();

registry.start();

const app = new Hono();

// docs:start create
// Starts a workflow for the order and returns right away.
app.post("/orders", async (c) => {
	const input = await c.req.json<OrderInput>();
	// Fails with `actor.duplicate_key` if a workflow with this key already exists.
	await client.orderWorkflow.create([input.orderId], { input });
	return c.json({ id: input.orderId }, 202);
});
// docs:end create

// docs:start get
app.get("/orders/:id", async (c) => {
	const order = client.orderWorkflow.get([c.req.param("id")]);
	return c.json(await order.getStatus());
});

app.delete("/orders/:id", async (c) => {
	await client.orderWorkflow.get([c.req.param("id")]).terminate();
	return c.body(null, 204);
});
// docs:end get

serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
