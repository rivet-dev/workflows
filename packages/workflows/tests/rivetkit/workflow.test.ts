import { describe, expect, test, vi } from "vitest";
import { workflow } from "../../src/rivetkit/mod";
import { getDefinedRunHandlerOptions } from "../fixtures/rivetkit";

function compareBytes(a: Uint8Array, b: Uint8Array): number {
	for (let index = 0; index < Math.min(a.length, b.length); index++) {
		if (a[index] !== b[index]) return a[index] - b[index];
	}
	return a.length - b.length;
}

type Write = { key: Uint8Array; value: Uint8Array };
type TestDb = {
	execute: (sql: string, ...params: Uint8Array[]) => Promise<unknown[]>;
	transaction: (
		callback: (tx: TestDb) => Promise<unknown>,
		options?: unknown,
	) => Promise<unknown>;
};

function createRunContext() {
	const rows = new Map<string, { key: Uint8Array; value: Uint8Array }>();
	const keyOf = (key: Uint8Array) => Buffer.from(key).toString("hex");
	const apply = (writes: Write[]) => {
		for (const write of writes) {
			rows.set(keyOf(write.key), write);
		}
	};
	const db: TestDb = {
		execute: async (sql: string, ...params: Uint8Array[]) => {
			if (sql.startsWith("SELECT value")) {
				const row = rows.get(keyOf(params[0]));
				return row ? [{ value: row.value }] : [];
			}
			if (sql.startsWith("SELECT key, value")) {
				return [...rows.values()]
					.filter(
						(row) =>
							compareBytes(row.key, params[0]) >= 0 &&
							(params[1] === undefined || compareBytes(row.key, params[1]) < 0),
					)
					.sort((a, b) => compareBytes(a.key, b.key));
			}
			if (sql.startsWith("INSERT INTO")) {
				apply([{ key: params[0], value: params[1] }]);
				return [];
			}
			if (sql.includes("key >= ? AND key < ?")) {
				for (const [key, row] of rows) {
					if (
						compareBytes(row.key, params[0]) >= 0 &&
						compareBytes(row.key, params[1]) < 0
					) {
						rows.delete(key);
					}
				}
				return [];
			}
			if (sql.includes("key = ?")) {
				rows.delete(keyOf(params[0]));
				return [];
			}
			throw new Error(`Unsupported test SQL: ${sql}`);
		},
		transaction: async (
			callback: (tx: typeof db) => Promise<unknown>,
			_options?: unknown,
		) => await callback(db),
	};
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
	test("creates a full actor definition from a workflow config", () => {
		const definition = workflow({
			state: { count: 0 },
			run: async (ctx) => {
				await ctx.step("increment", async (step) => {
					step.state.count += 1;
				});
			},
			actions: {
				getCount: (ctx) => ctx.state.count,
			},
			options: {
				sleepTimeout: 20,
			},
		});
		const config = definition.config as unknown as {
			state: { count: number };
			actions: { getCount: (ctx: unknown) => number };
			options: { sleepTimeout: number };
			run: (...args: any[]) => any;
		};

		expect(config.state).toEqual({ count: 0 });
		expect(config.actions.getCount).toBeTypeOf("function");
		expect(config.options.sleepTimeout).toBe(20);
		expect(getDefinedRunHandlerOptions(config.run)).toMatchObject({
			inspectorKind: "workflow",
		});
	});

	test("preserves actor lifecycle and connection configuration", () => {
		const definition = workflow({
			types: {} as { state: { count: number } },
			createState: (_ctx, input: { seed: number }) => ({
				count: input.seed,
			}),
			createConnState: (_ctx, params: { token: string }) => ({
				token: params.token,
			}),
			createVars: () => ({ transientCount: 0 }),
			onCreate: (ctx, input) => {
				ctx.state.count = input.seed;
			},
			onConnect: (ctx, conn) => {
				ctx.vars.transientCount += conn.state.token.length;
			},
			run: async (ctx) => {
				await ctx.step("typed-context", async (step) => {
					step.state.count += step.vars.transientCount;
				});
			},
			actions: {
				getCount: (ctx) => ctx.state.count,
			},
		});

		expect(definition.config.onCreate).toBeTypeOf("function");
		expect(definition.config.onConnect).toBeTypeOf("function");
		expect(definition.config).not.toHaveProperty("types");
	});

	test("publishes static Inspector metadata and disposes actor state", async () => {
		const step = vi.fn(async () => "done");
		const run = workflow(async (ctx) => {
			await ctx.step("once", step);
		});
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
});
