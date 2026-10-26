import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

// Signatures follow the Standard Webhooks spec (standardwebhooks.com), so
// customers can check them with any Standard Webhooks library. A secret is
// `whsec_` followed by the base64 key bytes.
export function createSecret(): string {
	return `whsec_${randomBytes(24).toString("base64")}`;
}

function sign(
	secret: string,
	id: string,
	timestamp: number,
	body: string,
): string {
	const key = Buffer.from(secret.slice("whsec_".length), "base64");
	return createHmac("sha256", key)
		.update(`${id}.${timestamp}.${body}`)
		.digest("base64");
}

// docs:start sign
export function signatureHeaders(
	secret: string,
	id: string,
	body: string,
): Record<string, string> {
	const timestamp = Math.floor(Date.now() / 1000);
	return {
		"webhook-id": id,
		"webhook-timestamp": String(timestamp),
		"webhook-signature": `v1,${sign(secret, id, timestamp, body)}`,
	};
}
// docs:end sign

// Reject requests signed longer ago than this, so a captured request
// cannot be replayed later.
const TOLERANCE_SECONDS = 5 * 60;

// docs:start verify
// What your customers run on their side to check that a webhook came from you.
export function verifyWebhook(
	secret: string,
	headers: Headers,
	body: string,
): boolean {
	const id = headers.get("webhook-id");
	const timestamp = Number(headers.get("webhook-timestamp"));
	const header = headers.get("webhook-signature");
	if (!id || !header || !Number.isFinite(timestamp)) return false;
	if (Math.abs(Date.now() / 1000 - timestamp) > TOLERANCE_SECONDS) return false;

	const expected = Buffer.from(sign(secret, id, timestamp, body), "base64");
	// The header holds several space-separated signatures while a secret is
	// being rotated.
	return header.split(" ").some((entry) => {
		const [version, signature] = entry.split(",", 2);
		if (version !== "v1" || !signature) return false;
		const given = Buffer.from(signature, "base64");
		return given.length === expected.length && timingSafeEqual(given, expected);
	});
}
// docs:end verify
