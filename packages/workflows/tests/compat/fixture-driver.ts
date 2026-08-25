import * as bare from "@rivetkit/bare-ts";
import * as cbor from "cbor-x";
import type { WorkflowFixture } from "./fixture-codec";

const WORKFLOW_NAMESPACE = new Uint8Array([6, 1]);
const RUN_WAKE_META_KEY = "run_wake_at";
const SCHEMA_VERSION_META_KEY = "schema_version";
const fixtureBareConfig = bare.Config({});

interface WorkflowMessage {
	id: string;
	name: string;
	data: unknown;
	sentAt: number;
	complete?: (response?: unknown) => Promise<void>;
}

interface QueueMessageState extends Omit<WorkflowMessage, "complete"> {
	numericId: bigint;
}

interface Waiter {
	names?: Set<string>;
	resolve: () => void;
	reject: (error: unknown) => void;
	signal: AbortSignal;
	onAbort: () => void;
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
	const buffer = new ArrayBuffer(bytes.byteLength);
	new Uint8Array(buffer).set(bytes);
	return buffer;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
	for (let index = 0; index < Math.min(left.length, right.length); index++) {
		if (left[index] !== right[index]) return left[index] - right[index];
	}
	return left.length - right.length;
}

function startsWith(key: Uint8Array, prefix: Uint8Array): boolean {
	return prefix.every((byte, index) => key[index] === byte);
}

function keyString(key: Uint8Array): string {
	return Buffer.from(key).toString("hex");
}

export class CompatibilityDriver {
	readonly atomicBatch = true;
	workerPollInterval = 0;
	readonly completions: Array<{
		id: string;
		name: string;
		response: unknown;
	}> = [];
	readonly messageDriver;
	actorState: Uint8Array;
	scheduleEvents: WorkflowFixture["scheduleEvents"];
	scheduleHistory: WorkflowFixture["scheduleHistory"];
	#rows = new Map<string, { key: Uint8Array; value: Uint8Array }>();
	#alarms = new Map<string, number>();
	#messages: QueueMessageState[] = [];
	#waiters = new Set<Waiter>();
	#logicalRunWakeAt: bigint | null = null;
	#persistRunWakeMetadata: boolean;
	#existingRunWakeValue: ArrayBuffer | undefined;

	constructor(
		fixture?: WorkflowFixture,
		options: { persistRunWakeMetadata?: boolean } = {},
	) {
		this.#persistRunWakeMetadata = options.persistRunWakeMetadata ?? false;
		this.actorState = fixture?.actorState
			? new Uint8Array(fixture.actorState)
			: cbor.encode({ mutations: 0 });
		this.scheduleEvents = fixture?.scheduleEvents ?? [];
		this.scheduleHistory = fixture?.scheduleHistory ?? [];

		for (const row of fixture?.workflowRows ?? []) {
			const key = new Uint8Array(row.key);
			if (!startsWith(key, WORKFLOW_NAMESPACE)) {
				throw new Error("Workflow fixture row escaped the [6, 1] namespace");
			}
			const logicalKey = key.slice(WORKFLOW_NAMESPACE.length);
			this.#rows.set(keyString(logicalKey), {
				key: logicalKey,
				value: new Uint8Array(row.value),
			});
		}
		const runWakeAt = fixture?.metaRows.find(
			(row) => row.key === RUN_WAKE_META_KEY,
		);
		const lastPushedAlarm = fixture?.runtime?.lastPushedAlarm;
		if (fixture && runWakeAt) {
			this.#existingRunWakeValue = runWakeAt.value;
			this.#logicalRunWakeAt = decodeRunWakeAt(new Uint8Array(runWakeAt.value));
		}
		if (fixture && this.#logicalRunWakeAt != null) {
			this.#alarms.set(
				fixture.metadata.actorId,
				Number(this.#logicalRunWakeAt),
			);
		} else if (fixture && lastPushedAlarm != null) {
			this.#alarms.set(fixture.metadata.actorId, Number(lastPushedAlarm));
		}
		this.#messages = (fixture?.queueRows ?? []).map((row) => ({
			id: row.id.toString(),
			numericId: row.id,
			name: row.name,
			data: cbor.decode(new Uint8Array(row.body)),
			sentAt: Number(row.createdAt),
		}));

		this.messageDriver = {
			addMessage: async (message: WorkflowMessage) => {
				const highestId = this.#messages.reduce(
					(max, candidate) =>
						candidate.numericId > max ? candidate.numericId : max,
					0n,
				);
				const parsed = /^\d+$/.test(message.id)
					? BigInt(message.id)
					: highestId + 1n;
				this.#messages.push({ ...message, numericId: parsed });
				this.#notify(message.name);
			},
			receiveMessages: async (options: {
				names?: readonly string[];
				count: number;
				completable: boolean;
			}) => {
				const nameSet = options.names?.length
					? new Set(options.names)
					: undefined;
				const selected = this.#messages
					.filter((message) => !nameSet || nameSet.has(message.name))
					.slice(0, Math.max(1, options.count));
				if (!options.completable) {
					for (const message of selected) this.#remove(message);
					return selected;
				}
				return selected.map((message) => ({
					...message,
					complete: async (response?: unknown) => {
						this.#complete(message, response);
					},
				}));
			},
			completeMessage: async (
				identity: { id: string; name: string },
				response?: unknown,
			) => {
				const message = this.#messages.find(
					(candidate) =>
						candidate.id === identity.id && candidate.name === identity.name,
				);
				if (message) this.#complete(message, response);
			},
		};
	}

	async get(key: Uint8Array): Promise<Uint8Array | null> {
		return this.#rows.get(keyString(key))?.value ?? null;
	}

	async set(key: Uint8Array, value: Uint8Array): Promise<void> {
		this.#rows.set(keyString(key), { key, value });
	}

	async delete(key: Uint8Array): Promise<void> {
		this.#rows.delete(keyString(key));
	}

	async deletePrefix(prefix: Uint8Array): Promise<void> {
		for (const [encoded, row] of this.#rows) {
			if (startsWith(row.key, prefix)) this.#rows.delete(encoded);
		}
	}

	async deleteRange(start: Uint8Array, end: Uint8Array): Promise<void> {
		for (const [encoded, row] of this.#rows) {
			if (compareBytes(row.key, start) >= 0 && compareBytes(row.key, end) < 0) {
				this.#rows.delete(encoded);
			}
		}
	}

	async list(prefix: Uint8Array) {
		return [...this.#rows.values()]
			.filter((row) => startsWith(row.key, prefix))
			.sort((left, right) => compareBytes(left.key, right.key));
	}

	async batch(writes: Array<{ key: Uint8Array; value: Uint8Array }>) {
		for (const write of writes) await this.set(write.key, write.value);
	}

	async setAlarm(workflowId: string, wakeAt: number): Promise<void> {
		this.#alarms.set(workflowId, wakeAt);
		this.#logicalRunWakeAt = BigInt(wakeAt);
	}

	async clearAlarm(workflowId: string): Promise<void> {
		this.#alarms.delete(workflowId);
		this.#logicalRunWakeAt = null;
	}

	async waitForMessages(names: string[], signal: AbortSignal): Promise<void> {
		if (
			this.#messages.some(
				(message) => names.length === 0 || names.includes(message.name),
			)
		) {
			return;
		}
		await new Promise<void>((resolve, reject) => {
			const waiter: Waiter = {
				names: names.length ? new Set(names) : undefined,
				resolve: () => {
					this.#removeWaiter(waiter);
					resolve();
				},
				reject,
				signal,
				onAbort: () => {
					this.#removeWaiter(waiter);
					reject(new Error("workflow fixture wait aborted"));
				},
			};
			signal.addEventListener("abort", waiter.onAbort, { once: true });
			this.#waiters.add(waiter);
		});
	}

	workflowAlarm(actorId: string): number | undefined {
		return this.#alarms.get(actorId);
	}

	queueSize(): number {
		return this.#messages.length;
	}

	toFixture(
		metadata: WorkflowFixture["metadata"],
		options: {
			runWakeAt?: bigint | null;
			lastPushedAlarm?: bigint | null;
		} = {},
	): WorkflowFixture {
		const workflowRows = [...this.#rows.values()]
			.sort((left, right) => compareBytes(left.key, right.key))
			.map((row) => ({
				key: toArrayBuffer(new Uint8Array([...WORKFLOW_NAMESPACE, ...row.key])),
				value: toArrayBuffer(row.value),
			}));
		const queueRows = [...this.#messages]
			.sort((left, right) =>
				left.numericId < right.numericId
					? -1
					: left.numericId > right.numericId
						? 1
						: 0,
			)
			.map((message) => ({
				id: message.numericId,
				name: message.name,
				body: toArrayBuffer(cbor.encode(message.data)),
				createdAt: BigInt(message.sentAt),
			}));
		const alarm = this.#alarms.get(metadata.actorId);
		if (options.runWakeAt !== undefined) {
			this.#logicalRunWakeAt = options.runWakeAt;
		}
		const metaRows: Array<WorkflowFixture["metaRows"][number]> = [
			{
				key: SCHEMA_VERSION_META_KEY,
				value: toArrayBuffer(
					encodeSchemaVersion(metadata.internalSchemaVersion),
				),
			},
		];
		if (this.#persistRunWakeMetadata) {
			metaRows.push({
				key: RUN_WAKE_META_KEY,
				value: toArrayBuffer(encodeRunWakeAt(this.#logicalRunWakeAt)),
			});
		} else if (this.#existingRunWakeValue) {
			metaRows.push({
				key: RUN_WAKE_META_KEY,
				value: this.#existingRunWakeValue,
			});
		}
		metaRows.sort((left, right) => left.key.localeCompare(right.key));
		return {
			metadata,
			metaRows,
			runtime: {
				lastPushedAlarm:
					options.lastPushedAlarm ??
					(alarm === undefined ? null : BigInt(alarm)),
				inspectorToken: null,
				queueNextId:
					queueRows.reduce(
						(max, row) => (row.id >= max ? row.id + 1n : max),
						0n,
					) || 1n,
			},
			actor: { hasInitialized: 1n, input: null },
			actorState: toArrayBuffer(this.actorState),
			workflowRows,
			queueRows,
			scheduleEvents: this.scheduleEvents,
			scheduleHistory: this.scheduleHistory,
		};
	}

	#remove(message: QueueMessageState): void {
		const index = this.#messages.indexOf(message);
		if (index !== -1) this.#messages.splice(index, 1);
	}

	#complete(message: QueueMessageState, response: unknown): void {
		this.completions.push({
			id: message.id,
			name: message.name,
			response,
		});
		this.#remove(message);
	}

	#notify(name: string): void {
		for (const waiter of [...this.#waiters]) {
			if (!waiter.names || waiter.names.has(name)) waiter.resolve();
		}
	}

	#removeWaiter(waiter: Waiter): void {
		if (this.#waiters.delete(waiter)) {
			waiter.signal.removeEventListener("abort", waiter.onAbort);
		}
	}
}

export function encodeActorState(value: unknown): Uint8Array {
	return cbor.encode(value);
}

export function decodeActorState<T>(bytes: Uint8Array): T {
	return cbor.decode(bytes) as T;
}

function encodeSchemaVersion(version: bigint): Uint8Array {
	const bytes = new Uint8Array(8);
	new DataView(bytes.buffer).setBigInt64(0, version, true);
	return bytes;
}

function encodeRunWakeAt(value: bigint | null): Uint8Array {
	const cursor = new bare.ByteCursor(
		new Uint8Array(fixtureBareConfig.initialBufferLength),
		fixtureBareConfig,
	);
	bare.writeBool(cursor, value !== null);
	if (value !== null) bare.writeI64(cursor, value);
	return new Uint8Array([1, 0, ...cursor.bytes.slice(0, cursor.offset)]);
}

function decodeRunWakeAt(bytes: Uint8Array): bigint | null {
	if (bytes[0] !== 1 || bytes[1] !== 0) {
		throw new Error("Unsupported logical run-wake fixture version");
	}
	const payload = bytes.slice(2);
	const cursor = new bare.ByteCursor(payload, fixtureBareConfig);
	return bare.readBool(cursor) ? bare.readI64(cursor) : null;
}
