import { type AnyActorDefinition, queue } from "rivetkit";
import type { ActorHandle } from "rivetkit/client";
import type { RawAccess } from "rivetkit/db";
import { describe, expectTypeOf, test } from "vitest";
import {
	type WorkflowContextOf,
	type WorkflowStepContextOf,
	workflow,
} from "../../src/rivetkit/mod";

const definition = workflow({
	state: { count: 0 },
	queues: {
		jobs: queue<{ id: string }>(),
	},
	actions: {
		increment: (ctx, amount: number) => {
			ctx.state.count += amount;
			return ctx.state.count;
		},
	},
	run: async (ctx) => {
		await ctx.step("typed", async (step) => {
			expectTypeOf(step.state.count).toEqualTypeOf<number>();
			expectTypeOf(step.db).toEqualTypeOf<RawAccess>();
			await step.queue.send("jobs", { id: "one" });
		});
	},
});

type HasWorkflowAction =
	ActorHandle<typeof definition> extends {
		increment: (...args: any[]) => any;
	}
		? true
		: false;

function customDatabaseIsRejected() {
	workflow({
		// @ts-expect-error Workflows requires RivetKit's standard embedded database.
		db: {},
		run: async () => {},
	});
}

describe("workflow actor types", () => {
	test("returns a normal actor definition", () => {
		expectTypeOf<HasWorkflowAction>().toEqualTypeOf<true>();
		expectTypeOf(definition).toMatchTypeOf<AnyActorDefinition>();
		expectTypeOf<
			WorkflowContextOf<typeof definition>["actorId"]
		>().toEqualTypeOf<string>();
		expectTypeOf<
			WorkflowStepContextOf<typeof definition>["state"]
		>().toEqualTypeOf<{ count: number }>();
		expectTypeOf(customDatabaseIsRejected).toBeFunction();
	});
});
