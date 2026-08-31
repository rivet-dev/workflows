import { setup, workflow } from "@rivet-dev/workflows";

function openResource(): string {
	return "connected";
}
function closeResource(_resource: string): void {}
export const setupRunTeardownActor = workflow({
	vars: {
		resource: null as string | null,
	},
	state: {
		initialized: false,
		ticks: 0,
	},
	onWake: (c) => {
		c.vars.resource = openResource();
	},
	onSleep: (c) => {
		if (!c.vars.resource) return;
		closeResource(c.vars.resource);
		c.vars.resource = null;
	},
	run: async (ctx) => {
		await ctx.step("setup", async (step) => {
			if (!step.vars.resource) step.vars.resource = openResource();
			step.state.initialized = true;
		});
		await ctx.loop("main-loop", async (loopCtx) => {
			await loopCtx.sleep("tick", 1000);
			await loopCtx.step("tick-step", async (step) => {
				step.state.ticks += 1;
			});
		});
	},
	actions: {
		getState: (c) => c.state,
	},
});
export const registry = setup({ use: { setupRunTeardownActor } });
