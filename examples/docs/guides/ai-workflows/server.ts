import { randomUUID } from "node:crypto";
import { serve } from "@hono/node-server";
import { setup } from "@rivet-dev/workflows";
import { Hono } from "hono";
import { createClient } from "rivetkit/client";
import { aiJob, type JobInput } from "./ai-job";

export const registry = setup({ use: { aiJob } });
const client = createClient<typeof registry>();

registry.start();

const app = new Hono();

// docs:start jobs
// Starts a job and returns its id without waiting for it to finish.
app.post("/jobs", async (c) => {
	const input = await c.req.json<JobInput>();
	if (!input.title || !input.transcript)
		return c.text("title and transcript are required", 400);

	const jobId = randomUUID();
	await client.aiJob.getOrCreate([jobId]).send("start", input);
	return c.json({ jobId }, 202);
});
// docs:end jobs

serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
