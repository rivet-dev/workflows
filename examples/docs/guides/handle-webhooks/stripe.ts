import { createHmac, timingSafeEqual } from "node:crypto";

// The parts of a Stripe event this guide reads.
export type StripeEvent = {
	id: string;
	type: string;
	created: number;
	data: {
		object: {
			id: string;
			customer?: string | null;
			[field: string]: unknown;
		};
	};
};

// Reject requests signed longer ago than this, so a captured request
// cannot be replayed later.
const TOLERANCE_SECONDS = 5 * 60;

// docs:start verify
// Checks the `Stripe-Signature` header, which looks like `t=1700000000,v1=<hex>`.
export function verifyStripeSignature(
	rawBody: string,
	header: string | undefined,
	secret: string,
): boolean {
	if (!header) return false;
	const pairs = header.split(",").map((part) => part.split("=", 2));
	const timestamp = Number(pairs.find(([key]) => key === "t")?.[1]);
	const signatures = pairs
		.filter(([key]) => key === "v1")
		.map(([, value]) => value);
	if (!Number.isFinite(timestamp) || signatures.length === 0) return false;
	if (Math.abs(Date.now() / 1000 - timestamp) > TOLERANCE_SECONDS) return false;

	// Stripe signs `<timestamp>.<raw body>` with HMAC-SHA256, keyed by the
	// endpoint's signing secret.
	const expected = createHmac("sha256", secret)
		.update(`${timestamp}.${rawBody}`)
		.digest();
	return signatures.some((signature) => {
		const given = Buffer.from(signature, "hex");
		return given.length === expected.length && timingSafeEqual(given, expected);
	});
}
// docs:end verify

// Signs a payload the way Stripe does, for `send-test-event.ts`.
export function signStripePayload(rawBody: string, secret: string): string {
	const timestamp = Math.floor(Date.now() / 1000);
	const signature = createHmac("sha256", secret)
		.update(`${timestamp}.${rawBody}`)
		.digest("hex");
	return `t=${timestamp},v1=${signature}`;
}
