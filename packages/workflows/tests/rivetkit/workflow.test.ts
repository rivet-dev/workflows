import { describe, expect, test, vi } from "vitest";
import { workflow } from "../../src/rivetkit/mod";
import { getDefinedRunHandlerOptions } from "../fixtures/rivetkit";
import { createTestDatabase } from "../fixtures/rivetkit-db";

function createRunContext() {
	const { db, rows } = createTestDatabase();
	const waitUntil: Promise<unknown>[] = [];
	const setWakeAt = vi.fn(async () => {});
	return {
		ctx: {
			actorId: "actor-1",
			name: "example",
			key: ["one"],
			log: {
				fatal: vi.fn(),
				error: vi.fn(),
				warn: vi.fn(),
				info: vi.fn(),
				debug: vi.fn(),
				trace: vi.fn(),
				child: () => undefined,
			},
			abortSignal: new AbortController().signal,
			db,
			run: { setWakeAt },
			queue: {
				send: async () => {},
				tryNextBatch: async () => [],
				complete: async () => {},
				waitForAvailable: async () => {},
			},
			waitUntil: (promise: Promise<unknown>) => waitUntil.push(promise),
		},
		rows,
		setWakeAt,
		waitUntil,
	};
}

describe("workflow RivetKit integration", () => {
	test("returns an actor definition, forwards config, and disposes Inspector state", async () => {
		const step = vi.fn(async () => "done");
		const definition = workflow({
			state: { count: 0 },
			actions: {
				getCount: (ctx) => ctx.state.count,
			},
			options: { sleepTimeout: 250 },
			run: async (ctx) => {
				await ctx.step("once", step);
			},
		});
		expect(definition).toEqual({
			config: expect.objectContaining({
				state: { count: 0 },
				actions: expect.any(Object),
				options: { sleepTimeout: 250 },
				run: expect.any(Function),
			}),
		});
		const run = definition.config.run;
		if (typeof run !== "function") {
			throw new Error("workflow actor did not install a run handler");
		}
		const options = getDefinedRunHandlerOptions(run);
		expect(options.inspectorKind).toBe("workflow");

		const withInactive = vi.fn(async (_options, callback) => await callback());
		const registration = options.createInspector({
			actorId: "actor-1",
			control: { run: { withInactive } },
		});
		const firstAdapter = registration.inspector.workflow;
		const { ctx, rows, setWakeAt, waitUntil } = createRunContext();
		await run(ctx as never);
		await Promise.all(waitUntil);

		expect(step).toHaveBeenCalledOnce();
		expect(rows.size).toBeGreaterThan(0);
		expect(setWakeAt).toHaveBeenLastCalledWith(null);
		await expect(firstAdapter.getState()).resolves.toBe("completed");
		expect(firstAdapter.getHistory()).toBeInstanceOf(ArrayBuffer);

		await firstAdapter.replayFromStep();
		expect(withInactive).toHaveBeenCalledWith(
			{ restartOnSuccess: true },
			expect.any(Function),
		);

		registration.dispose();
		const nextRegistration = options.createInspector({
			actorId: "actor-1",
			control: { run: { withInactive } },
		});
		expect(nextRegistration.inspector.workflow).not.toBe(firstAdapter);
	});

	test("rejects a custom database provider", () => {
		expect(() =>
			workflow({
				run: async () => {},
				db: {},
			} as never),
		).toThrow("workflow() does not support a custom database provider");
	});
});
