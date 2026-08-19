import { describe, expect, test, vi } from "vitest";
import { workflow } from "../../src/rivetkit/mod";
import { getDefinedRunHandlerOptions } from "../fixtures/rivetkit";

function compareBytes(a: Uint8Array, b: Uint8Array): number {
	for (let index = 0; index < Math.min(a.length, b.length); index++) {
		if (a[index] !== b[index]) return a[index] - b[index];
	}
	return a.length - b.length;
}

function startsWith(key: Uint8Array, prefix: Uint8Array): boolean {
	return prefix.every((byte, index) => key[index] === byte);
}

type Write = { key: Uint8Array; value: Uint8Array };

function createRunContext() {
	const rows = new Map<string, { key: Uint8Array; value: Uint8Array }>();
	const keyOf = (key: Uint8Array) => Buffer.from(key).toString("hex");
	const apply = (writes: Write[]) => {
		for (const write of writes) {
			rows.set(keyOf(write.key), write);
		}
	};
	const storage = {
		get: async (key: Uint8Array) => rows.get(keyOf(key))?.value ?? null,
		set: async (key: Uint8Array, value: Uint8Array) => apply([{ key, value }]),
		delete: async (key: Uint8Array) => {
			rows.delete(keyOf(key));
		},
		deletePrefix: async (prefix: Uint8Array) => {
			for (const [key, row] of rows) {
				if (startsWith(row.key, prefix)) rows.delete(key);
			}
		},
		deleteRange: async (start: Uint8Array, end: Uint8Array) => {
			for (const [key, row] of rows) {
				if (
					compareBytes(row.key, start) >= 0 &&
					compareBytes(row.key, end) < 0
				) {
					rows.delete(key);
				}
			}
		},
		list: async (prefix: Uint8Array) =>
			[...rows.values()]
				.filter((row) => startsWith(row.key, prefix))
				.sort((a, b) => compareBytes(a.key, b.key)),
		batch: async (writes: Write[]) => apply(writes),
		flushWithState: async (writes: Write[]) => apply(writes),
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
			storage: { open: () => storage },
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
