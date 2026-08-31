import {
	event,
	setup,
	type WorkflowErrorEvent,
	workflow,
} from "@rivet-dev/workflows";
export const errorHookActor = workflow(
	{
		state: {
			lastError: null as WorkflowErrorEvent | null,
		},
		events: {
			workflowError: event<[WorkflowErrorEvent]>(),
		},
		run: async (ctx) => {
			await ctx.step({
				name: "sync-ledger",
				maxRetries: 3,
				retryBackoffBase: 250,
				retryBackoffMax: 1000,
				run: async (_ctx) => {
					throw new Error("ledger unavailable");
				},
			});
		},
		actions: {
			getState: (c) => c.state,
		},
	},
	{
		onError: (c, event) => {
			c.state.lastError = event;
			c.broadcast("workflowError", event);
		},
	},
);
export const registry = setup({ use: { errorHookActor } });
