import { describe, expect, test, vi } from "vitest";
import {
	ActorWorkflowControlDriver,
	ActorWorkflowDriver,
} from "../../src/rivetkit/driver";
import { createTestDatabase } from "../fixtures/rivetkit-db";

function write(key = 1, value = 2) {
	return {
		key: new Uint8Array([key]),
		value: new Uint8Array([value]),
	};
}

function createSubject() {
	const { db } = createTestDatabase();
	const waitUntil: Promise<unknown>[] = [];
	const queue = {
		send: vi.fn(async () => {}),
		tryNextBatch: vi.fn(async () => []),
		complete: vi.fn(async () => {}),
		waitForAvailable: vi.fn(async () => {}),
	};
	const run = { setWakeAt: vi.fn(async () => {}) };
	const ctx = {
		db,
		queue,
		run,
		waitUntil: (promise: Promise<unknown>) => waitUntil.push(promise),
	};
	return {
		driver: new ActorWorkflowDriver(ctx as never),
		controlDriver: new ActorWorkflowControlDriver(ctx as never),
		db,
		queue,
		run,
		waitUntil,
	};
}

describe("RivetKit workflow driver", () => {
	test("stores the existing workflow rows under the [6, 1] namespace", async () => {
		const { driver, db } = createSubject();
		await driver.batch([write(3, 4)]);

		const insert = db.execute.mock.calls.find(([sql]) =>
			String(sql).startsWith("INSERT INTO _rivet_wf_kv"),
		);
		expect(insert?.[1]).toEqual(new Uint8Array([6, 1, 3]));
		expect(insert?.[2]).toEqual(new Uint8Array([4]));
	});

	test("commits live workflow writes with actor state", async () => {
		const { driver, db } = createSubject();
		await driver.batch([write(), write(3, 4)]);
		expect(db.transaction).toHaveBeenCalledOnce();
		expect(db.transaction.mock.calls[0]?.[1]).toEqual({
			experimental: { includeState: true },
		});
	});

	test("uses an ordinary transaction for control writes", async () => {
		const { controlDriver, db } = createSubject();
		await controlDriver.batch([write()]);
		expect(db.transaction).toHaveBeenCalledOnce();
		expect(db.transaction.mock.calls[0]?.[1]).toBeUndefined();
	});

	test("reads, lists, and deletes byte-compatible rows", async () => {
		const { driver } = createSubject();
		await driver.batch([write(2, 20), write(1, 10), write(3, 30)]);

		await expect(driver.get(new Uint8Array([2]))).resolves.toEqual(
			new Uint8Array([20]),
		);
		await expect(driver.list(new Uint8Array())).resolves.toEqual([
			write(1, 10),
			write(2, 20),
			write(3, 30),
		]);

		await driver.deleteRange(new Uint8Array([1]), new Uint8Array([3]));
		await expect(driver.list(new Uint8Array())).resolves.toEqual([
			write(3, 30),
		]);
		await driver.deletePrefix(new Uint8Array([3]));
		await expect(driver.list(new Uint8Array())).resolves.toEqual([]);
	});

	test("rejects rows outside the [6, 1] namespace", async () => {
		const { driver, db } = createSubject();
		db.execute.mockResolvedValueOnce([
			{ key: new Uint8Array([6, 2, 1]), value: new Uint8Array([1]) },
		]);
		await expect(driver.list(new Uint8Array())).rejects.toThrow(
			"workflow SQLite key escaped the [6, 1] namespace",
		);
	});

	test("does not open a transaction for an empty batch", async () => {
		const { driver, db } = createSubject();
		await driver.batch([]);
		expect(db.transaction).not.toHaveBeenCalled();
	});

	test("rejects values above 256 KiB", async () => {
		const { driver, db } = createSubject();
		await expect(
			driver.batch([
				{
					key: new Uint8Array([1]),
					value: new Uint8Array(256 * 1024 + 1),
				},
			]),
		).rejects.toThrow("exceeding the 262144 byte limit");
		expect(db.transaction).not.toHaveBeenCalled();
	});

	test("rejects batches above 128 rows", async () => {
		const { driver, db } = createSubject();
		await expect(
			driver.batch(Array.from({ length: 129 }, (_, key) => write(key, 1))),
		).rejects.toThrow("exceeding the 128 row limit");
		expect(db.transaction).not.toHaveBeenCalled();
	});

	test("rejects batches above 512 KiB", async () => {
		const { driver, db } = createSubject();
		const value = new Uint8Array((512 * 1024) / 2);
		await expect(
			driver.batch([
				{ key: new Uint8Array([1]), value },
				{ key: new Uint8Array([2]), value },
			]),
		).rejects.toThrow("exceeding the 524288 byte limit");
		expect(db.transaction).not.toHaveBeenCalled();
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
		const { driver, db, waitUntil } = createSubject();
		db.execute.mockRejectedValueOnce(new Error("read failed"));
		await expect(driver.get(new Uint8Array([1]))).rejects.toThrow(
			"read failed",
		);
		await expect(Promise.all(waitUntil)).resolves.toEqual([undefined]);
	});
});
