// Stand-ins for your own APIs.

export async function chargeCard(
	orderId: string,
	amountCents: number,
): Promise<string> {
	const res = await fetch("https://payments.example.com/charges", {
		method: "POST",
		headers: { "idempotency-key": orderId },
		body: JSON.stringify({ orderId, amountCents }),
	});
	if (!res.ok) throw new Error(`charge failed: ${res.status}`);
	return ((await res.json()) as { id: string }).id;
}

export async function scoreFraud(orderId: string): Promise<number> {
	const res = await fetch(`https://fraud.example.com/score/${orderId}`);
	return ((await res.json()) as { score: number }).score;
}

export async function createLabel(orderId: string): Promise<string> {
	const res = await fetch("https://shipping.example.com/labels", {
		method: "POST",
		body: JSON.stringify({ orderId }),
	});
	return ((await res.json()) as { labelId: string }).labelId;
}

export async function sendReceipt(
	email: string,
	chargeId: string,
): Promise<void> {
	await fetch("https://email.example.com/send", {
		method: "POST",
		body: JSON.stringify({ to: email, template: "receipt", chargeId }),
	});
}

export async function sendReminder(email: string): Promise<void> {
	await fetch("https://email.example.com/send", {
		method: "POST",
		body: JSON.stringify({ to: email, template: "review-reminder" }),
	});
}
