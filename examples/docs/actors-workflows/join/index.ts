import { queue, setup, workflow } from "@rivet-dev/workflows";
export const dashboardActor = workflow({
	state: {
		summary: null as null | {
			users: number;
			orders: number;
			revenue: number;
		},
	},
	queues: {
		refresh: queue<Record<string, never>>(),
	},
	run: async (ctx) => {
		await ctx.loop("dashboard-loop", async (loopCtx) => {
			await loopCtx.queue.next("wait-refresh");
			const summary = await loopCtx.join("fetch-summary", {
				users: {
					run: async (branchCtx) => {
						return await branchCtx.step("fetch-users", (_branchCtx) =>
							fetchCount("/users"),
						);
					},
				},
				orders: {
					run: async (branchCtx) => {
						return await branchCtx.step("fetch-orders", (_branchCtx) =>
							fetchCount("/orders"),
						);
					},
				},
				revenue: {
					run: async (branchCtx) => {
						return await branchCtx.step("fetch-revenue", (_branchCtx) =>
							fetchCount("/revenue"),
						);
					},
				},
			});
			await loopCtx.step("save-summary", async (step) => {
				step.state.summary = summary;
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
export const registry = setup({ use: { dashboardActor } });
