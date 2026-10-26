// Calls to your own APIs. Each one runs inside a workflow step, so a failure retries only that call.

export type Company = { name: string; plan: string; seats: number };

// Looks up a company in your CRM.
export async function lookupCompany(name: string): Promise<Company> {
	const crmUrl = process.env.CRM_API_URL ?? "https://crm.example.com";
	const res = await fetch(
		`${crmUrl}/companies?name=${encodeURIComponent(name)}`,
	);
	if (!res.ok) throw new Error(`CRM lookup failed with status ${res.status}`);
	return (await res.json()) as Company;
}

// Publishes a post to your CMS. The job id is the idempotency key, so a retry never publishes twice.
export async function publishPost(post: {
	jobId: string;
	title: string;
	body: string;
}): Promise<string> {
	const cmsUrl = process.env.CMS_API_URL ?? "https://cms.example.com";
	const res = await fetch(`${cmsUrl}/posts`, {
		method: "POST",
		headers: {
			"content-type": "application/json",
			"idempotency-key": post.jobId,
		},
		body: JSON.stringify({ title: post.title, body: post.body }),
	});
	if (!res.ok) throw new Error(`CMS publish failed with status ${res.status}`);
	return ((await res.json()) as { url: string }).url;
}
