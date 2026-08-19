import { actor, setup } from "rivetkit";
import { setupTest } from "rivetkit/test";
import { expect, test } from "vitest";
import { workflow } from "../../src/rivetkit/mod";

const sleepAcrossWake = actor({
	state: { completed: [] as string[] },
	run: workflow(async (ctx) => {
		await ctx.step("before-sleep", async (step) => {
			step.state.completed.push("before-sleep");
		});
		await ctx.sleep("sleep", 100);
		await ctx.step("after-sleep", async (step) => {
			step.state.completed.push("after-sleep");
		});
	}),
	actions: {
		getCompleted: (ctx) => ctx.state.completed,
	},
	options: {
		sleepTimeout: 20,
	},
});

const registry = setup({ use: { sleepAcrossWake } });

test("published runtime resumes a sleeping workflow exactly once", async (context) => {
	const { client } = await setupTest(context, registry);
	const handle = client.sleepAcrossWake.getOrCreate(["preview-e2e"]);
	const deadline = Date.now() + 10_000;
	let completed: string[] = [];

	while (Date.now() < deadline) {
		completed = await handle.getCompleted();
		if (completed.includes("after-sleep")) break;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}

	expect(completed).toEqual(["before-sleep", "after-sleep"]);
	await new Promise((resolve) => setTimeout(resolve, 150));
	expect(await handle.getCompleted()).toEqual(["before-sleep", "after-sleep"]);
});
