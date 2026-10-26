import { createClient } from "rivetkit/client";
import type { registry } from "./server";

// Answer a request without Slack: npx tsx decide.ts <request-id> approve|reject
const [requestId, answer] = process.argv.slice(2);
const client = createClient<typeof registry>();
const handle = client.approval.get([requestId]);

const { request } = await handle.getStatus();
await handle.send("decision", {
	approved: answer === "approve",
	decidedBy: request?.approver ?? "unknown",
});
console.log(`sent ${answer} for request ${requestId}`);
