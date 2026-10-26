import type { StripeEvent } from "./stripe";

// A stand-in for your database so the example runs on its own.
// Replace it with writes to your own database.
const subscriptions = new Map<
	string,
	{ customerId: string; status: string; updatedBy: string }
>();

// docs:start db
export async function applyEvent(event: StripeEvent): Promise<void> {
	const object = event.data.object;
	switch (event.type) {
		case "customer.subscription.created":
		case "customer.subscription.updated":
		case "customer.subscription.deleted":
			// An upsert keyed by the subscription id. Writing the same event twice
			// leaves the same row.
			subscriptions.set(object.id, {
				customerId: String(object.customer),
				status: String(object.status),
				updatedBy: event.id,
			});
			console.log(
				`applied ${event.id} (${event.type}): ${object.id} is ${object.status}`,
			);
			return;
		default:
			console.log(`ignored ${event.id} (${event.type})`);
	}
}
// docs:end db
