import { queue, workflow } from "@rivet-dev/workflows";
import { applyEvent } from "./db";
import type { StripeEvent } from "./stripe";

// How long to remember an event id. Stripe retries a failed delivery for
// up to three days.
const SEEN_FOR_MS = 7 * 24 * 60 * 60 * 1000;

// One workflow per customer, keyed by the Stripe customer id.
export const customerEvents = workflow({
	state: {
		// Event id to the time it was first received.
		seen: {} as Record<string, number>,
		applied: 0,
		duplicates: 0,
		failed: [] as { eventId: string; type: string; error: string }[],
	},
	queues: {
		events: queue<StripeEvent>(),
	},
	run: async (ctx) => {
		await ctx.loop("event-loop", async (loopCtx) => {
			// docs:start dedupe
			// Events come off the queue one at a time, in the order they arrived.
			const { body: event } = await loopCtx.queue.next("wait-event", {
				names: ["events"],
			});

			const isNew = await loopCtx.step("record-event-id", async (step) => {
				const now = Date.now();
				for (const [id, seenAt] of Object.entries(step.state.seen)) {
					if (now - seenAt > SEEN_FOR_MS) delete step.state.seen[id];
				}
				if (step.state.seen[event.id] !== undefined) {
					step.state.duplicates += 1;
					return false;
				}
				step.state.seen[event.id] = now;
				return true;
			});
			if (!isNew) return;
			// docs:end dedupe

			// docs:start apply
			// Retries with backoff while your database is down: 1s, 2s, 4s, and so
			// on, up to 1 minute apart.
			const result = await loopCtx.tryStep({
				name: "apply-event",
				maxRetries: 10,
				retryBackoffBase: 500,
				retryBackoffMax: 60_000,
				run: async () => await applyEvent(event),
			});

			await loopCtx.step("record-result", async (step) => {
				if (result.ok) {
					step.state.applied += 1;
					return;
				}
				// Park the event so it does not block this customer's later events.
				step.state.failed.push({
					eventId: event.id,
					type: event.type,
					error: result.failure.error.message,
				});
			});
			// docs:end apply
		});
	},
	actions: {
		getStatus: (c) => ({
			applied: c.state.applied,
			duplicates: c.state.duplicates,
			failed: c.state.failed,
		}),
	},
});
