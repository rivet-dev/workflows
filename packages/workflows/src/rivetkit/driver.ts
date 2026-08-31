import type { ActorQueue, ActorRun, RunContext } from "rivetkit";
import type { RawAccess } from "rivetkit/db";
import type {
	EngineDriver,
	KVEntry,
	KVWrite,
	Message,
	WorkflowMessageDriver,
	WorkflowMessageIdentity,
} from "../index.js";

const WORKFLOW_STORAGE_PREFIX = new Uint8Array([6, 1]);
const WORKFLOW_UPSERT_SQL =
	"INSERT INTO _rivet_wf_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value";

const WORKFLOW_SQLITE_MAX_VALUE_BYTES = 256 * 1024;
const WORKFLOW_SQLITE_MAX_BATCH_ROWS = 128;
const WORKFLOW_SQLITE_MAX_BATCH_BYTES = 512 * 1024;

function track<T>(
	runCtx: RunContext<any, any, any, any, any, any, any, any>,
	promise: Promise<T>,
): Promise<T> {
	runCtx.waitUntil(
		promise.then(
			() => undefined,
			() => undefined,
		),
	);
	return promise;
}

function prefixWorkflowKey(key: Uint8Array): Uint8Array {
	const prefixed = new Uint8Array(
		WORKFLOW_STORAGE_PREFIX.byteLength + key.byteLength,
	);
	prefixed.set(WORKFLOW_STORAGE_PREFIX);
	prefixed.set(key, WORKFLOW_STORAGE_PREFIX.byteLength);
	return prefixed;
}

function stripWorkflowKey(key: Uint8Array): Uint8Array {
	if (
		key.byteLength < WORKFLOW_STORAGE_PREFIX.byteLength ||
		!WORKFLOW_STORAGE_PREFIX.every((byte, index) => key[index] === byte)
	) {
		throw new Error("workflow SQLite key escaped the [6, 1] namespace");
	}
	return key.slice(WORKFLOW_STORAGE_PREFIX.byteLength);
}

function computeUpperBound(prefix: Uint8Array): Uint8Array {
	const upperBound = prefix.slice();
	for (let index = upperBound.length - 1; index >= 0; index--) {
		if (upperBound[index] !== 0xff) {
			upperBound[index]++;
			return upperBound.slice(0, index + 1);
		}
	}

	// Every workflow key begins with 6, so a finite upper bound always exists.
	throw new Error("workflow storage prefix has no upper bound");
}

function normalizeSqlBlob(value: unknown): Uint8Array {
	if (value instanceof Uint8Array) {
		return value;
	}
	if (value instanceof ArrayBuffer) {
		return new Uint8Array(value);
	}
	if (ArrayBuffer.isView(value)) {
		return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
	}
	if (Array.isArray(value)) {
		const bytes = new Uint8Array(value.length);
		for (const [index, byte] of value.entries()) {
			if (!Number.isInteger(byte) || byte < 0 || byte > 255) {
				throw new Error("workflow SQLite value was not a byte array");
			}
			bytes[index] = byte;
		}
		return bytes;
	}
	throw new Error("workflow SQLite value was not a blob");
}

function validateWrites(writes: KVWrite[]): void {
	if (writes.length > WORKFLOW_SQLITE_MAX_BATCH_ROWS) {
		throw new Error(
			`Workflow batch contains ${writes.length} rows, exceeding the ${WORKFLOW_SQLITE_MAX_BATCH_ROWS} row limit`,
		);
	}

	let batchBytes = 0;
	for (const write of writes) {
		if (write.value.byteLength > WORKFLOW_SQLITE_MAX_VALUE_BYTES) {
			throw new Error(
				`Workflow value is ${write.value.byteLength} bytes, exceeding the ${WORKFLOW_SQLITE_MAX_VALUE_BYTES} byte limit`,
			);
		}
		batchBytes +=
			WORKFLOW_STORAGE_PREFIX.byteLength +
			write.key.byteLength +
			write.value.byteLength;
	}

	if (batchBytes > WORKFLOW_SQLITE_MAX_BATCH_BYTES) {
		throw new Error(
			`Workflow batch is ${batchBytes} bytes, exceeding the ${WORKFLOW_SQLITE_MAX_BATCH_BYTES} byte limit`,
		);
	}
}

class WorkflowStorage {
	#db: RawAccess;

	constructor(db: RawAccess) {
		this.#db = db;
	}

	async get(key: Uint8Array): Promise<Uint8Array | null> {
		const rows = await this.#db.execute<{ value: unknown }>(
			"SELECT value FROM _rivet_wf_kv WHERE key = ?",
			prefixWorkflowKey(key),
		);
		const value = rows[0]?.value;
		return value == null ? null : normalizeSqlBlob(value);
	}

	async set(key: Uint8Array, value: Uint8Array): Promise<void> {
		await this.batch([{ key, value }], false);
	}

	async delete(key: Uint8Array): Promise<void> {
		await this.#db.execute(
			"DELETE FROM _rivet_wf_kv WHERE key = ?",
			prefixWorkflowKey(key),
		);
	}

	async batchDelete(keys: Uint8Array[]): Promise<void> {
		if (keys.length === 0) return;
		await this.#db.transaction(async (tx) => {
			for (const key of keys) {
				await tx.execute(
					"DELETE FROM _rivet_wf_kv WHERE key = ?",
					prefixWorkflowKey(key),
				);
			}
		});
	}

	async deletePrefix(prefix: Uint8Array): Promise<void> {
		const start = prefixWorkflowKey(prefix);
		await this.#db.execute(
			"DELETE FROM _rivet_wf_kv WHERE key >= ? AND key < ?",
			start,
			computeUpperBound(start),
		);
	}

	async deleteRange(start: Uint8Array, end: Uint8Array): Promise<void> {
		await this.#db.execute(
			"DELETE FROM _rivet_wf_kv WHERE key >= ? AND key < ?",
			prefixWorkflowKey(start),
			prefixWorkflowKey(end),
		);
	}

	async list(prefix: Uint8Array): Promise<KVEntry[]> {
		const start = prefixWorkflowKey(prefix);
		const rows = await this.#db.execute<{ key: unknown; value: unknown }>(
			"SELECT key, value FROM _rivet_wf_kv WHERE key >= ? AND key < ? ORDER BY key ASC",
			start,
			computeUpperBound(start),
		);
		return rows.map((row) => ({
			key: stripWorkflowKey(normalizeSqlBlob(row.key)),
			value: normalizeSqlBlob(row.value),
		}));
	}

	async batch(writes: KVWrite[], includeState: boolean): Promise<void> {
		if (writes.length === 0) return;
		validateWrites(writes);

		const commit = async (tx: RawAccess) => {
			for (const write of writes) {
				await tx.execute(
					WORKFLOW_UPSERT_SQL,
					prefixWorkflowKey(write.key),
					write.value,
				);
			}
		};

		if (includeState) {
			await this.#db.transaction(commit, {
				experimental: { includeState: true },
			});
		} else {
			await this.#db.transaction(commit);
		}
	}
}

class ActorWorkflowMessageDriver implements WorkflowMessageDriver {
	#runCtx: RunContext<any, any, any, any, any, any, any, any>;
	#queue: ActorQueue;

	constructor(runCtx: RunContext<any, any, any, any, any, any, any, any>) {
		this.#runCtx = runCtx;
		this.#queue = runCtx.queue;
	}

	async addMessage(message: Message): Promise<void> {
		await track(this.#runCtx, this.#queue.send(message.name, message.data));
	}

	async receiveMessages(opts: {
		names?: readonly string[];
		count: number;
		completable: boolean;
	}): Promise<Message[]> {
		const messages = await track(
			this.#runCtx,
			this.#queue.tryNextBatch({
				names:
					opts.names && opts.names.length > 0 ? [...opts.names] : undefined,
				count: opts.count,
				completable: opts.completable,
			}),
		);
		return messages.map((message) => ({
			id: message.id.toString(),
			name: message.name,
			data: message.body,
			sentAt: message.createdAt,
			...(opts.completable
				? {
						complete: async (response?: unknown) => {
							if (!message.complete) {
								throw new Error(
									"RivetKit returned a non-completable queue message",
								);
							}
							await track(this.#runCtx, message.complete(response));
						},
					}
				: {}),
		}));
	}

	async completeMessage(
		message: WorkflowMessageIdentity,
		response?: unknown,
	): Promise<void> {
		let parsedId: bigint;
		try {
			parsedId = BigInt(message.id);
		} catch {
			return;
		}

		await track(
			this.#runCtx,
			this.#queue.complete({ id: parsedId, name: message.name }, response),
		);
	}
}

export class ActorWorkflowDriver implements EngineDriver {
	readonly atomicBatch = true;
	readonly workerPollInterval = 100;
	readonly messageDriver: WorkflowMessageDriver;
	#runCtx: RunContext<any, any, any, any, any, any, any, any>;
	#storage: WorkflowStorage;
	#queue: ActorQueue;
	#run: ActorRun;

	constructor(runCtx: RunContext<any, any, any, any, any, any, any, any>) {
		this.#runCtx = runCtx;
		this.messageDriver = new ActorWorkflowMessageDriver(runCtx);
		this.#queue = runCtx.queue;
		this.#storage = new WorkflowStorage(runCtx.db);
		this.#run = runCtx.run;
	}

	async get(key: Uint8Array): Promise<Uint8Array | null> {
		return await track(this.#runCtx, this.#storage.get(key));
	}

	async set(key: Uint8Array, value: Uint8Array): Promise<void> {
		await track(this.#runCtx, this.#storage.set(key, value));
	}

	async delete(key: Uint8Array): Promise<void> {
		await track(this.#runCtx, this.#storage.delete(key));
	}

	async batchDelete(keys: Uint8Array[]): Promise<void> {
		await track(this.#runCtx, this.#storage.batchDelete(keys));
	}

	async deletePrefix(prefix: Uint8Array): Promise<void> {
		await track(this.#runCtx, this.#storage.deletePrefix(prefix));
	}

	async deleteRange(start: Uint8Array, end: Uint8Array): Promise<void> {
		await track(this.#runCtx, this.#storage.deleteRange(start, end));
	}

	async list(prefix: Uint8Array): Promise<KVEntry[]> {
		return await track(this.#runCtx, this.#storage.list(prefix));
	}

	async batch(writes: KVWrite[]): Promise<void> {
		await track(this.#runCtx, this.#storage.batch(writes, true));
	}

	async setAlarm(_workflowId: string, wakeAt: number): Promise<void> {
		await track(this.#runCtx, this.#run.setWakeAt(wakeAt));
	}

	async clearAlarm(_workflowId: string): Promise<void> {
		await track(this.#runCtx, this.#run.setWakeAt(null));
	}

	waitForMessages(
		messageNames: string[],
		abortSignal: AbortSignal,
	): Promise<void> {
		return track(
			this.#runCtx,
			this.#queue.waitForAvailable(
				messageNames.length > 0 ? messageNames : undefined,
				{ signal: abortSignal },
			),
		);
	}
}

class NoopWorkflowMessageDriver implements WorkflowMessageDriver {
	async addMessage(_message: Message): Promise<void> {
		throw new Error("Workflow control driver does not support messages");
	}

	async receiveMessages(_opts: {
		names?: readonly string[];
		count: number;
		completable: boolean;
	}): Promise<Message[]> {
		throw new Error("Workflow control driver does not support messages");
	}

	async completeMessage(
		_message: WorkflowMessageIdentity,
		_response?: unknown,
	): Promise<void> {
		throw new Error("Workflow control driver does not support messages");
	}
}

export class ActorWorkflowControlDriver implements EngineDriver {
	readonly workerPollInterval = 100;
	readonly messageDriver: WorkflowMessageDriver =
		new NoopWorkflowMessageDriver();
	#storage: WorkflowStorage;
	#run: ActorRun;

	constructor(runCtx: RunContext<any, any, any, any, any, any, any, any>) {
		this.#storage = new WorkflowStorage(runCtx.db);
		this.#run = runCtx.run;
	}

	async get(key: Uint8Array): Promise<Uint8Array | null> {
		return await this.#storage.get(key);
	}

	async set(key: Uint8Array, value: Uint8Array): Promise<void> {
		await this.#storage.set(key, value);
	}

	async delete(key: Uint8Array): Promise<void> {
		await this.#storage.delete(key);
	}

	async batchDelete(keys: Uint8Array[]): Promise<void> {
		await this.#storage.batchDelete(keys);
	}

	async deletePrefix(prefix: Uint8Array): Promise<void> {
		await this.#storage.deletePrefix(prefix);
	}

	async deleteRange(start: Uint8Array, end: Uint8Array): Promise<void> {
		await this.#storage.deleteRange(start, end);
	}

	async list(prefix: Uint8Array): Promise<KVEntry[]> {
		return await this.#storage.list(prefix);
	}

	async batch(writes: KVWrite[]): Promise<void> {
		await this.#storage.batch(writes, false);
	}

	async setAlarm(_workflowId: string, wakeAt: number): Promise<void> {
		await this.#run.setWakeAt(wakeAt);
	}

	async clearAlarm(_workflowId: string): Promise<void> {
		await this.#run.setWakeAt(null);
	}

	waitForMessages(
		_messageNames: string[],
		_abortSignal: AbortSignal,
	): Promise<void> {
		throw new Error("Workflow control driver does not support messages");
	}
}
