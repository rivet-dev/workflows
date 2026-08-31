import { setup, workflow } from "@rivet-dev/workflows";
export const fanInOutActor = workflow({
	state: {
		total: 0,
	},
	run: async (ctx) => {
		await ctx.loop("join-loop", async (loopCtx) => {
			const [message] = await loopCtx.queue.nextBatch("wait-refresh", {
				timeout: 30000,
			});
			if (!message) return;
			const joined = await loopCtx.join("parallel-work", {
				users: {
					run: async (branchCtx) =>
						await branchCtx.step("fetch-users", (_branchCtx) =>
							fetchCount("/users"),
						),
				},
				orders: {
					run: async (branchCtx) =>
						await branchCtx.step("fetch-orders", (_branchCtx) =>
							fetchCount("/orders"),
						),
				},
				invoices: {
					run: async (branchCtx) =>
						await branchCtx.step("fetch-invoices", (_branchCtx) =>
							fetchCount("/invoices"),
						),
				},
			});
			await loopCtx.step("merge-results", async (step) => {
				step.state.total = joined.users + joined.orders + joined.invoices;
			});
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
async function fetchCount(path: string): Promise<number> {
	const res = await fetch(`https://api.example.com${path}`);
	if (!res.ok) throw new Error(`fetch ${path} failed: ${res.status}`);
	return (
		(await res.json()) as {
			count: number;
		}
	).count;
}
export const registry = setup({ use: { fanInOutActor } });
