import { CriticalError } from "@rivet-dev/workflows";

// docs:start model
// The only function that talks to a model. Swap the body to use another provider or SDK.
export async function callModel(opts: {
	system: string;
	prompt: string;
	json?: boolean;
}): Promise<string> {
	const baseUrl = process.env.OPENAI_BASE_URL ?? "https://api.openai.com/v1";
	const res = await fetch(`${baseUrl}/chat/completions`, {
		method: "POST",
		headers: {
			authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
			"content-type": "application/json",
		},
		body: JSON.stringify({
			model: process.env.OPENAI_MODEL ?? "gpt-4.1-mini",
			messages: [
				{ role: "system", content: opts.system },
				{ role: "user", content: opts.prompt },
			],
			response_format: opts.json ? { type: "json_object" } : undefined,
		}),
	});

	// Rate limits and server errors throw a plain Error, so the step retries with backoff.
	if (res.status === 429 || res.status >= 500) {
		throw new Error(`model returned ${res.status}`);
	}
	// Other errors, such as a bad API key, will not fix themselves, so the step fails without retrying.
	if (!res.ok) {
		throw new CriticalError(
			`model returned ${res.status}: ${await res.text()}`,
		);
	}

	const body = (await res.json()) as {
		choices: Array<{ message: { content: string } }>;
	};
	return body.choices[0].message.content;
}
// docs:end model
