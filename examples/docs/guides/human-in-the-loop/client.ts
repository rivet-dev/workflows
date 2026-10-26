import { refundOrderTool } from "./agent-tool";

// Call the tool the way an agent would. This waits until someone approves or rejects the refund.
const answer = await refundOrderTool.execute({
	orderId: "order-1042",
	amountCents: 2500,
	reason: "The package arrived damaged.",
});
console.log(answer);
