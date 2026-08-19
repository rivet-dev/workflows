import type { ActorQueue, ActorRun, RunContext } from "rivetkit";
import {
	WORKFLOW_STORAGE_V1,
	type WorkflowStorageHandle,
} from "rivetkit/storage";
import type {
	EngineDriver,
	KVEntry,
	KVWrite,
	Message,
	WorkflowMessageDriver,
	WorkflowMessageIdentity,
} from "../index.js";

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
	#storage: WorkflowStorageHandle;
	#queue: ActorQueue;
	#run: ActorRun;

	constructor(runCtx: RunContext<any, any, any, any, any, any, any, any>) {
		this.#runCtx = runCtx;
		this.messageDriver = new ActorWorkflowMessageDriver(runCtx);
		this.#queue = runCtx.queue;
		this.#storage = runCtx.storage.open(WORKFLOW_STORAGE_V1);
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
		if (writes.length === 0) return;

		await track(this.#runCtx, this.#storage.flushWithState(writes));
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
	#storage: WorkflowStorageHandle;
	#run: ActorRun;

	constructor(runCtx: RunContext<any, any, any, any, any, any, any, any>) {
		this.#storage = runCtx.storage.open(WORKFLOW_STORAGE_V1);
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
		if (writes.length === 0) {
			return;
		}

		await this.#storage.batch(writes);
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
