import { CriticalError, event, setup, workflow } from "@rivet-dev/workflows";
import { db } from "rivetkit/db";
import {
	chargeCard,
	createLabel,
	scoreFraud,
	sendReceipt,
	sendReminder,
} from "./tools";

export type OrderInput = {
	orderId: string;
	email: string;
	amountCents: number;
};
export type OrderStatus = "running" | "complete" | "failed";
export type Progress = { step: string };

// One workflow per order. The key is the instance ID.
export const orderWorkflow = workflow(
	{
		// docs:start input
		db: db({
			onMigrate: async (db) => {
				await db.execute(`
					CREATE TABLE IF NOT EXISTS run (
						id INTEGER PRIMARY KEY CHECK (id = 1),
						input TEXT NOT NULL,
						status TEXT NOT NULL,
						output TEXT,
						error TEXT,
						created_at INTEGER NOT NULL
					)
				`);
			},
		}),

		// Runs once, before `run` starts. Store the input so steps can read it.
		onCreate: async (c, input: OrderInput) => {
			await c.db.execute(
				"INSERT INTO run (id, input, status, created_at) VALUES (1, ?, 'running', ?)",
				JSON.stringify(input),
				Date.now(),
			);
		},
		// docs:end input

		events: {
			progress: event<Progress>(),
		},

		run: async (ctx) => {
			// docs:start load-input
			const input = await ctx.step("load-input", async (step) => {
				const rows = await step.db.execute<{ input: string }>(
					"SELECT input FROM run WHERE id = 1",
				);
				return JSON.parse(rows[0].input) as OrderInput;
			});
			// docs:end load-input

			// docs:start critical
			await ctx.step("validate", async () => {
				// CriticalError fails the step right away, with no retries.
				if (input.amountCents <= 0) {
					throw new CriticalError("amount must be positive");
				}
			});
			// docs:end critical

			// docs:start retries
			const chargeId = await ctx.step({
				name: "charge-card",
				maxRetries: 5,
				retryBackoffBase: 1_000,
				retryBackoffMax: 60_000,
				timeout: 10 * 60_000,
				retryOnTimeout: true,
				run: async (step) => {
					step.broadcast("progress", { step: "charge-card" });
					return await chargeCard(input.orderId, input.amountCents);
				},
			});
			// docs:end retries

			// docs:start join
			// Both branches run at the same time. Each branch records its own steps.
			const checks = await ctx.join("checks", {
				fraud: {
					run: async (branch) =>
						await branch.step("score-fraud", async () =>
							scoreFraud(input.orderId),
						),
				},
				label: {
					run: async (branch) =>
						await branch.step("create-label", async () =>
							createLabel(input.orderId),
						),
				},
			});
			// docs:end join

			// docs:start try-step
			// tryStep returns failures as data instead of failing the workflow.
			const receipt = await ctx.tryStep({
				name: "send-receipt",
				maxRetries: 3,
				run: async () => await sendReceipt(input.email, chargeId),
			});
			if (!receipt.ok) {
				await ctx.step("log-receipt-failure", async (step) => {
					step.log.warn({
						msg: "receipt not sent",
						error: receipt.failure.error.message,
					});
				});
			}
			// docs:end try-step

			// docs:start child
			// Start a child workflow without waiting for it to finish.
			await ctx.step("start-shipment", async (step) => {
				const client = step.client<typeof registry>();
				await client.shipmentWorkflow.getOrCreate([input.orderId]).resolve();
			});
			// docs:end child

			// docs:start complete
			await ctx.step("complete", async (step) => {
				const output = { chargeId, fraudScore: checks.fraud };
				await step.db.execute(
					"UPDATE run SET status = 'complete', output = ?, error = NULL WHERE id = 1",
					JSON.stringify(output),
				);
				step.broadcast("progress", { step: "complete" });
			});
			// docs:end complete

			// docs:start sleep
			// Compute times inside a step, so a replay sees the same value.
			const followUpAt = await ctx.step(
				"follow-up-time",
				async () => Date.now() + 7 * 24 * 60 * 60 * 1000,
			);
			await ctx.sleep("wait-a-day", 24 * 60 * 60 * 1000);
			await ctx.sleepUntil("wait-for-follow-up", followUpAt);
			await ctx.step("send-reminder", async () => sendReminder(input.email));
			// docs:end sleep
		},

		// docs:start status
		actions: {
			getStatus: async (c) => {
				const rows = await c.db.execute<{
					status: OrderStatus;
					output: string | null;
					error: string | null;
				}>("SELECT status, output, error FROM run WHERE id = 1");
				const row = rows[0];
				return {
					status: row.status,
					output: row.output ? JSON.parse(row.output) : null,
					error: row.error,
				};
			},
			// Stops the workflow and deletes it with its data.
			terminate: (c) => {
				c.destroy();
			},
		},
		// docs:end status
	},
	{
		// docs:start on-error
		// Record terminal failures, so getStatus can report them.
		onError: async (c, event) => {
			// onError also sees attempts that will retry and failures that
			// run handles with tryStep. Skip both.
			if (
				"step" in event &&
				(event.step.willRetry || event.step.stepName === "send-receipt")
			) {
				return;
			}
			const error =
				"step" in event
					? event.step.error
					: "rollback" in event
						? event.rollback.error
						: event.workflow.error;
			await c.db.execute(
				"UPDATE run SET status = 'failed', error = ? WHERE id = 1",
				error.message,
			);
		},
		// docs:end on-error
	},
);

// docs:start shipment
// A small identifier like the order ID can travel in the key instead of the input.
export const shipmentWorkflow = workflow({
	run: async (ctx) => {
		const orderId = ctx.key[0];
		await ctx.step("notify-warehouse", async () => {
			await fetch("https://warehouse.example.com/shipments", {
				method: "POST",
				body: JSON.stringify({ orderId }),
			});
		});
	},
	actions: {},
});
// docs:end shipment

// docs:start registry
export const registry = setup({ use: { orderWorkflow, shipmentWorkflow } });
// docs:end registry
