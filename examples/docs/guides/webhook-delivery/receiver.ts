import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { verifyWebhook } from "./sign";

// A test endpoint that plays the customer. It answers 503 to the first
// `failFirst` requests, then 200.
export function startReceiver(opts: {
	port: number;
	secret: string;
	failFirst: number;
}) {
	let remainingFailures = opts.failFirst;
	const seen = new Set<string>();
	const app = new Hono();

	app.post("/webhooks", async (c) => {
		const body = await c.req.text();
		if (!verifyWebhook(opts.secret, c.req.raw.headers, body))
			return c.text("bad signature", 401);

		const id = c.req.header("webhook-id") ?? "";
		const { type } = JSON.parse(body) as { type: string };
		if (remainingFailures > 0) {
			remainingFailures -= 1;
			console.log(`receiver: ${id} ${type} -> 503`);
			return c.text("unavailable", 503);
		}
		// Skip ids already handled, since a delivery can arrive more than once.
		const duplicate = seen.has(id);
		seen.add(id);
		console.log(
			`receiver: ${id} ${type} -> 200${duplicate ? " (duplicate, skipped)" : ""}`,
		);
		return c.text("ok");
	});

	const server = serve({ fetch: app.fetch, port: opts.port });
	return {
		// Makes every later request succeed.
		recover: () => {
			remainingFailures = 0;
		},
		close: () => server.close(),
	};
}
