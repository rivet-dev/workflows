import { createClient } from "rivetkit/client";
import type { registry } from "./index";

// Connect to the server from index.ts. The registry type checks every call.
const client = createClient<typeof registry>("http://localhost:6420");

// Get the order keyed "order-123". Creating it starts its workflow.
const order = client.order.getOrCreate(["order-123"]);

// Give the workflow a moment to run, then read the steps it has finished.
await new Promise((resolve) => setTimeout(resolve, 2_000));
console.log(await order.getProgress());
