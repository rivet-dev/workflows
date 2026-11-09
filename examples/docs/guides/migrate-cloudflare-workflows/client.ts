import { createClient } from "rivetkit/client";
import type { OrderInput, Progress, registry } from "./order";

const client = createClient<typeof registry>();

const input: OrderInput = {
	orderId: crypto.randomUUID(),
	email: "buyer@example.com",
	amountCents: 4200,
};

// docs:start progress
// Connect with createWithInput, so the client subscribes as the workflow starts
// instead of after its first steps have already run.
const order = client.orderWorkflow.getOrCreate([input.orderId], {
	createWithInput: input,
});
const conn = order.connect();

await new Promise<void>((resolve) => {
	conn.on("progress", (event) => {
		const { step } = event as Progress;
		console.log("step", step);
		if (step === "complete") resolve();
	});
});
// docs:end progress

console.log(await order.getStatus());
await conn.dispose();
