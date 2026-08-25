import { WORKFLOW_STORAGE_V1 } from "rivetkit/storage";
import { describe, expect, test, vi } from "vitest";
import { ActorWorkflowDriver } from "../../src/rivetkit/driver";

function write(key = 1, value = 2) {
	return {
		key: new Uint8Array([key]),
		value: new Uint8Array([value]),
	};
}

function createSubject() {
	const storage = {
		get: vi.fn(async () => null),
		set: vi.fn(async () => {}),
		delete: vi.fn(async () => {}),
		deletePrefix: vi.fn(async () => {}),
		deleteRange: vi.fn(async () => {}),
		list: vi.fn(async () => []),
		batch: vi.fn(async () => {}),
		flushWithState: vi.fn(async () => {}),
	};
	const waitUntil: Promise<unknown>[] = [];
	const queue = {
		send: vi.fn(async () => {}),
		tryNextBatch: vi.fn(async () => []),
		complete: vi.fn(async () => {}),
		waitForAvailable: vi.fn(async () => {}),
	};
	const run = { setWakeAt: vi.fn(async () => {}) };
	const open = vi.fn(() => storage);
	const ctx = {
		storage: { open },
		queue,
		run,
		waitUntil: (promise: Promise<unknown>) => waitUntil.push(promise),
	};
	return {
		driver: new ActorWorkflowDriver(ctx as never),
		storage,
		queue,
		run,
		open,
		waitUntil,
	};
}

describe("RivetKit workflow driver", () => {
	test("opens only the opaque workflow storage capability", () => {
		const { open } = createSubject();
		expect(open).toHaveBeenCalledOnce();
		expect(open).toHaveBeenCalledWith(WORKFLOW_STORAGE_V1);
	});

	test("flushes actor state and the full workflow batch atomically", async () => {
		const { driver, storage } = createSubject();
		const writes = [write(), write(3, 4)];
		await driver.batch(writes);
		expect(storage.flushWithState).toHaveBeenCalledWith(writes);
		expect(storage.batch).not.toHaveBeenCalled();
	});

	test("does not flush an empty batch", async () => {
		const { driver, storage } = createSubject();
		await driver.batch([]);
		expect(storage.flushWithState).not.toHaveBeenCalled();
	});

	test("uses the logical run wake source for set and clear", async () => {
		const { driver, run } = createSubject();
		await driver.setAlarm("actor", 1234);
		await driver.clearAlarm("actor");
		expect(run.setWakeAt.mock.calls).toEqual([[1234], [null]]);
	});

	test("waits for queue availability without consuming", async () => {
		const { driver, queue } = createSubject();
		const abort = new AbortController();
		await driver.waitForMessages(["ready"], abort.signal);
		expect(queue.waitForAvailable).toHaveBeenCalledWith(["ready"], {
			signal: abort.signal,
		});
		expect(queue.tryNextBatch).not.toHaveBeenCalled();
	});

	test("completes a persisted message with both its id and name", async () => {
		const { driver, queue } = createSubject();
		await driver.messageDriver.completeMessage(
			{ id: "42", name: "task" },
			{ ok: true },
		);
		expect(queue.complete).toHaveBeenCalledWith(
			{ id: 42n, name: "task" },
			{ ok: true },
		);
	});

	test("tracks host operations with outcome-swallowed waitUntil promises", async () => {
		const { driver, storage, waitUntil } = createSubject();
		storage.get.mockRejectedValueOnce(new Error("read failed"));
		await expect(driver.get(new Uint8Array([1]))).rejects.toThrow(
			"read failed",
		);
		await expect(Promise.all(waitUntil)).resolves.toEqual([undefined]);
	});
});
