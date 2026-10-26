import { createClient } from "rivetkit/client";
import type { Progress } from "./ai-job";
import type { registry } from "./server";

const serverUrl = process.env.SERVER_URL ?? "http://localhost:3000";
const client = createClient<typeof registry>();

// 1. Submit a job to the HTTP server.
const res = await fetch(`${serverUrl}/jobs`, {
	method: "POST",
	headers: { "content-type": "application/json" },
	body: JSON.stringify({
		title: "Pricing call with Acme and Globex",
		transcript:
			"Dana (Acme Corp): We need pricing for 1,200 seats. Lee (Globex): We are evaluating too. Sam: I will send pricing to Acme by Friday.",
	}),
});
const { jobId } = (await res.json()) as { jobId: string };
console.log("job", jobId);

// 2. Follow the job's progress until it finishes.
const job = client.aiJob.get([jobId]);
const conn = job.connect();
await new Promise<void>((resolve) => {
	conn.on("progress", (event) => {
		const progress = event as Progress;
		console.log(progress.stage, progress.detail ?? "");
		if (progress.stage === "published" || progress.stage === "failed")
			resolve();
	});
});
console.log(await job.getStatus());
await conn.dispose();
