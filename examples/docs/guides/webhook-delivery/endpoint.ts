import { Loop, queue, workflow } from "@rivet-dev/workflows";
import { signatureHeaders } from "./sign";

export type WebhookEvent = {
	id: string;
	type: string;
	data: unknown;
	createdAt: number;
};

export type Attempt = {
	eventId: string;
	eventType: string;
	attempt: number;
	status: number | null;
	error: string | null;
	durationMs: number;
	at: number;
};

// A failed delivery is retried 5 more times, 2s, 4s, 8s, 16s, and 32s apart,
// before the endpoint is disabled. Raise these numbers for production.
const MAX_RETRIES = 5;
const RETRY_BACKOFF_BASE_MS = 1000;
const RETRY_BACKOFF_MAX_MS = 60 * 60 * 1000;
const REQUEST_TIMEOUT_MS = 10_000;
// How many attempts the delivery log keeps.
const LOG_SIZE = 100;

// One workflow per customer endpoint, keyed by the endpoint id.
export const endpoint = workflow({
	state: {
		url: "",
		secret: "",
		disabled: false,
		// The id of the event being delivered, if any.
		pending: null as string | null,
		delivered: 0,
		attempts: [] as Attempt[],
	},
	// The backend passes the URL and signing secret when it creates the endpoint.
	onCreate: (c, input: { url: string; secret: string }) => {
		c.state.url = input.url;
		c.state.secret = input.secret;
	},
	queues: {
		events: queue<WebhookEvent>(),
		enable: queue<Record<string, never>>(),
	},
	run: async (ctx) => {
		await ctx.loop("delivery-loop", async (loopCtx) => {
			// docs:start outbox
			// The queue is the outbox. Events come off it one at a time, in order.
			const { body: event } = await loopCtx.queue.next("wait-event", {
				names: ["events"],
			});
			await loopCtx.step("mark-pending", async (step) => {
				step.state.pending = event.id;
			});

			// Keep at this event until it is delivered. Later events wait behind it.
			await loopCtx.loop("deliver-event", async (eventCtx) => {
				// docs:end outbox
				// docs:start deliver
				const delivery = await eventCtx.tryStep({
					name: "deliver",
					maxRetries: MAX_RETRIES,
					retryBackoffBase: RETRY_BACKOFF_BASE_MS,
					retryBackoffMax: RETRY_BACKOFF_MAX_MS,
					timeout: REQUEST_TIMEOUT_MS + 5_000,
					retryOnTimeout: true,
					run: async (step) => {
						const body = JSON.stringify(event);
						const startedAt = Date.now();
						let status: number | null = null;
						let error: string | null = null;
						try {
							const res = await fetch(step.state.url, {
								method: "POST",
								headers: {
									"content-type": "application/json",
									...signatureHeaders(step.state.secret, event.id, body),
								},
								body,
								signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
							});
							status = res.status;
						} catch (err) {
							error = err instanceof Error ? err.message : String(err);
						}

						// Record the attempt before throwing. A failed step keeps its
						// state changes, so every attempt is logged.
						const log = step.state.attempts;
						log.push({
							eventId: event.id,
							eventType: event.type,
							attempt: log.filter((a) => a.eventId === event.id).length + 1,
							status,
							error,
							durationMs: Date.now() - startedAt,
							at: startedAt,
						});
						if (log.length > LOG_SIZE) log.splice(0, log.length - LOG_SIZE);

						// Any 2xx answer counts as delivered. Anything else is retried.
						if (status === null || status < 200 || status >= 300) {
							throw new Error(
								`delivery failed: ${error ?? `status ${status}`}`,
							);
						}
					},
				});
				// docs:end deliver

				// docs:start disable
				if (delivery.ok) {
					await eventCtx.step("record-delivery", async (step) => {
						step.state.pending = null;
						step.state.delivered += 1;
					});
					return Loop.break(undefined);
				}

				// Out of retries. Stop sending until the endpoint is turned back on.
				await eventCtx.step("disable", async (step) => {
					step.state.disabled = true;
					step.log.warn({ msg: "endpoint disabled", eventId: event.id });
				});
				await eventCtx.queue.nextBatch("wait-enable", {
					names: ["enable"],
					count: 10,
				});
				await eventCtx.step("enable", async (step) => {
					step.state.disabled = false;
				});
				// The loop runs again and retries the same event.
				// docs:end disable
			});
		});
	},
	// docs:start actions
	actions: {
		listAttempts: (c) => ({
			url: c.state.url,
			disabled: c.state.disabled,
			delivered: c.state.delivered,
			pending: c.state.pending,
			attempts: c.state.attempts.toReversed(),
		}),
		enable: async (c) => {
			if (c.state.disabled) await c.queue.send("enable", {});
		},
	},
	// docs:end actions
});
