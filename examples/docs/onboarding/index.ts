import {
	setup,
	type WorkflowStepContextOf,
	workflow,
} from "@rivet-dev/workflows";
import { db } from "rivetkit/db";

// An order workflow, one instance per order. Each step runs exactly once:
// after a crash or redeploy, the workflow replays and skips finished steps.
export const order = workflow({
	// Each order gets its own SQLite database to record its progress.
	db: db({
		onMigrate: async (db) => {
			await db.execute(`
				CREATE TABLE IF NOT EXISTS progress (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					step TEXT NOT NULL
				)
			`);
		},
	}),

	run: async (ctx) => {
		// Reserve stock, then record that the step finished.
		await ctx.step("reserve", (step) => record(step, "reserve"));

		// Wait durably. The workflow keeps its place across restarts.
		await ctx.sleep("wait-for-payment", 1_000);

		// Charge the card and send the receipt, recording each step.
		await ctx.step("charge", (step) => record(step, "charge"));
		await ctx.step("receipt", (step) => record(step, "receipt"));
	},

	actions: {
		// Read which steps have finished so far.
		getProgress: async (c) => {
			return (await c.db.execute("SELECT step FROM progress ORDER BY id")) as {
				step: string;
			}[];
		},
	},
});

// Write one progress row to this order's database.
async function record(step: WorkflowStepContextOf<typeof order>, name: string) {
	await step.db.execute("INSERT INTO progress (step) VALUES (?)", name);
}

// Register the workflow and start serving it on http://localhost:6420.
export const registry = setup({ use: { order } });
registry.start();
