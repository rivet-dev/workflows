// @generated - post-processed by compile-bare.ts
import * as bare from "@rivetkit/bare-ts";

const config = /* @__PURE__ */ bare.Config({});

export type i64 = bigint;
export type u64 = bigint;

export type FixtureMetadata = {
	readonly fixtureName: string;
	readonly sourceRivetkitVersion: string;
	readonly sourceWorkflowVersion: string;
	readonly sourceRevision: string;
	readonly actorId: string;
	readonly registryKey: string;
	readonly internalSchemaVersion: i64;
	readonly fakeClockSeed: u64;
	readonly generatedIdSeed: u64;
};

export function readFixtureMetadata(bc: bare.ByteCursor): FixtureMetadata {
	return {
		fixtureName: bare.readString(bc),
		sourceRivetkitVersion: bare.readString(bc),
		sourceWorkflowVersion: bare.readString(bc),
		sourceRevision: bare.readString(bc),
		actorId: bare.readString(bc),
		registryKey: bare.readString(bc),
		internalSchemaVersion: bare.readI64(bc),
		fakeClockSeed: bare.readU64(bc),
		generatedIdSeed: bare.readU64(bc),
	};
}

export function writeFixtureMetadata(
	bc: bare.ByteCursor,
	x: FixtureMetadata,
): void {
	bare.writeString(bc, x.fixtureName);
	bare.writeString(bc, x.sourceRivetkitVersion);
	bare.writeString(bc, x.sourceWorkflowVersion);
	bare.writeString(bc, x.sourceRevision);
	bare.writeString(bc, x.actorId);
	bare.writeString(bc, x.registryKey);
	bare.writeI64(bc, x.internalSchemaVersion);
	bare.writeU64(bc, x.fakeClockSeed);
	bare.writeU64(bc, x.generatedIdSeed);
}

function read0(bc: bare.ByteCursor): i64 | null {
	return bare.readBool(bc) ? bare.readI64(bc) : null;
}

function write0(bc: bare.ByteCursor, x: i64 | null): void {
	bare.writeBool(bc, x !== null);
	if (x !== null) {
		bare.writeI64(bc, x);
	}
}

function read1(bc: bare.ByteCursor): string | null {
	return bare.readBool(bc) ? bare.readString(bc) : null;
}

function write1(bc: bare.ByteCursor, x: string | null): void {
	bare.writeBool(bc, x !== null);
	if (x !== null) {
		bare.writeString(bc, x);
	}
}

export type RuntimeRow = {
	readonly lastPushedAlarm: i64 | null;
	readonly inspectorToken: string | null;
	readonly queueNextId: i64;
};

export function readRuntimeRow(bc: bare.ByteCursor): RuntimeRow {
	return {
		lastPushedAlarm: read0(bc),
		inspectorToken: read1(bc),
		queueNextId: bare.readI64(bc),
	};
}

export function writeRuntimeRow(bc: bare.ByteCursor, x: RuntimeRow): void {
	write0(bc, x.lastPushedAlarm);
	write1(bc, x.inspectorToken);
	bare.writeI64(bc, x.queueNextId);
}

export type MetaRow = {
	readonly key: string;
	readonly value: ArrayBuffer;
};

export function readMetaRow(bc: bare.ByteCursor): MetaRow {
	return {
		key: bare.readString(bc),
		value: bare.readData(bc),
	};
}

export function writeMetaRow(bc: bare.ByteCursor, x: MetaRow): void {
	bare.writeString(bc, x.key);
	bare.writeData(bc, x.value);
}

function read2(bc: bare.ByteCursor): ArrayBuffer | null {
	return bare.readBool(bc) ? bare.readData(bc) : null;
}

function write2(bc: bare.ByteCursor, x: ArrayBuffer | null): void {
	bare.writeBool(bc, x !== null);
	if (x !== null) {
		bare.writeData(bc, x);
	}
}

export type ActorRow = {
	readonly hasInitialized: i64;
	readonly input: ArrayBuffer | null;
};

export function readActorRow(bc: bare.ByteCursor): ActorRow {
	return {
		hasInitialized: bare.readI64(bc),
		input: read2(bc),
	};
}

export function writeActorRow(bc: bare.ByteCursor, x: ActorRow): void {
	bare.writeI64(bc, x.hasInitialized);
	write2(bc, x.input);
}

export type WorkflowRow = {
	readonly key: ArrayBuffer;
	readonly value: ArrayBuffer;
};

export function readWorkflowRow(bc: bare.ByteCursor): WorkflowRow {
	return {
		key: bare.readData(bc),
		value: bare.readData(bc),
	};
}

export function writeWorkflowRow(bc: bare.ByteCursor, x: WorkflowRow): void {
	bare.writeData(bc, x.key);
	bare.writeData(bc, x.value);
}

export type QueueRow = {
	readonly id: i64;
	readonly name: string;
	readonly body: ArrayBuffer;
	readonly createdAt: i64;
};

export function readQueueRow(bc: bare.ByteCursor): QueueRow {
	return {
		id: bare.readI64(bc),
		name: bare.readString(bc),
		body: bare.readData(bc),
		createdAt: bare.readI64(bc),
	};
}

export function writeQueueRow(bc: bare.ByteCursor, x: QueueRow): void {
	bare.writeI64(bc, x.id);
	bare.writeString(bc, x.name);
	bare.writeData(bc, x.body);
	bare.writeI64(bc, x.createdAt);
}

export type ScheduleEventRow = {
	readonly eventId: string;
	readonly triggerAt: i64;
	readonly action: string;
	readonly args: ArrayBuffer | null;
	readonly kind: i64;
	readonly cronExpression: string | null;
	readonly timezone: string | null;
	readonly intervalMs: i64 | null;
	readonly lastStartedAt: i64 | null;
	readonly maxHistory: i64;
};

export function readScheduleEventRow(bc: bare.ByteCursor): ScheduleEventRow {
	return {
		eventId: bare.readString(bc),
		triggerAt: bare.readI64(bc),
		action: bare.readString(bc),
		args: read2(bc),
		kind: bare.readI64(bc),
		cronExpression: read1(bc),
		timezone: read1(bc),
		intervalMs: read0(bc),
		lastStartedAt: read0(bc),
		maxHistory: bare.readI64(bc),
	};
}

export function writeScheduleEventRow(
	bc: bare.ByteCursor,
	x: ScheduleEventRow,
): void {
	bare.writeString(bc, x.eventId);
	bare.writeI64(bc, x.triggerAt);
	bare.writeString(bc, x.action);
	write2(bc, x.args);
	bare.writeI64(bc, x.kind);
	write1(bc, x.cronExpression);
	write1(bc, x.timezone);
	write0(bc, x.intervalMs);
	write0(bc, x.lastStartedAt);
	bare.writeI64(bc, x.maxHistory);
}

export type ScheduleHistoryRow = {
	readonly id: i64;
	readonly scheduleId: string;
	readonly action: string;
	readonly scheduledAt: i64;
	readonly firedAt: i64;
	readonly finishedAt: i64 | null;
	readonly result: i64;
	readonly errorGroup: string | null;
	readonly errorCode: string | null;
	readonly errorMessage: string | null;
	readonly errorMetadata: ArrayBuffer | null;
};

export function readScheduleHistoryRow(
	bc: bare.ByteCursor,
): ScheduleHistoryRow {
	return {
		id: bare.readI64(bc),
		scheduleId: bare.readString(bc),
		action: bare.readString(bc),
		scheduledAt: bare.readI64(bc),
		firedAt: bare.readI64(bc),
		finishedAt: read0(bc),
		result: bare.readI64(bc),
		errorGroup: read1(bc),
		errorCode: read1(bc),
		errorMessage: read1(bc),
		errorMetadata: read2(bc),
	};
}

export function writeScheduleHistoryRow(
	bc: bare.ByteCursor,
	x: ScheduleHistoryRow,
): void {
	bare.writeI64(bc, x.id);
	bare.writeString(bc, x.scheduleId);
	bare.writeString(bc, x.action);
	bare.writeI64(bc, x.scheduledAt);
	bare.writeI64(bc, x.firedAt);
	write0(bc, x.finishedAt);
	bare.writeI64(bc, x.result);
	write1(bc, x.errorGroup);
	write1(bc, x.errorCode);
	write1(bc, x.errorMessage);
	write2(bc, x.errorMetadata);
}

function read3(bc: bare.ByteCursor): readonly MetaRow[] {
	const len = bare.readUintSafe(bc);
	if (len === 0) {
		return [];
	}
	const result = [readMetaRow(bc)];
	for (let i = 1; i < len; i++) {
		result[i] = readMetaRow(bc);
	}
	return result;
}

function write3(bc: bare.ByteCursor, x: readonly MetaRow[]): void {
	bare.writeUintSafe(bc, x.length);
	for (let i = 0; i < x.length; i++) {
		writeMetaRow(bc, x[i]);
	}
}

function read4(bc: bare.ByteCursor): RuntimeRow | null {
	return bare.readBool(bc) ? readRuntimeRow(bc) : null;
}

function write4(bc: bare.ByteCursor, x: RuntimeRow | null): void {
	bare.writeBool(bc, x !== null);
	if (x !== null) {
		writeRuntimeRow(bc, x);
	}
}

function read5(bc: bare.ByteCursor): ActorRow | null {
	return bare.readBool(bc) ? readActorRow(bc) : null;
}

function write5(bc: bare.ByteCursor, x: ActorRow | null): void {
	bare.writeBool(bc, x !== null);
	if (x !== null) {
		writeActorRow(bc, x);
	}
}

function read6(bc: bare.ByteCursor): readonly WorkflowRow[] {
	const len = bare.readUintSafe(bc);
	if (len === 0) {
		return [];
	}
	const result = [readWorkflowRow(bc)];
	for (let i = 1; i < len; i++) {
		result[i] = readWorkflowRow(bc);
	}
	return result;
}

function write6(bc: bare.ByteCursor, x: readonly WorkflowRow[]): void {
	bare.writeUintSafe(bc, x.length);
	for (let i = 0; i < x.length; i++) {
		writeWorkflowRow(bc, x[i]);
	}
}

function read7(bc: bare.ByteCursor): readonly QueueRow[] {
	const len = bare.readUintSafe(bc);
	if (len === 0) {
		return [];
	}
	const result = [readQueueRow(bc)];
	for (let i = 1; i < len; i++) {
		result[i] = readQueueRow(bc);
	}
	return result;
}

function write7(bc: bare.ByteCursor, x: readonly QueueRow[]): void {
	bare.writeUintSafe(bc, x.length);
	for (let i = 0; i < x.length; i++) {
		writeQueueRow(bc, x[i]);
	}
}

function read8(bc: bare.ByteCursor): readonly ScheduleEventRow[] {
	const len = bare.readUintSafe(bc);
	if (len === 0) {
		return [];
	}
	const result = [readScheduleEventRow(bc)];
	for (let i = 1; i < len; i++) {
		result[i] = readScheduleEventRow(bc);
	}
	return result;
}

function write8(bc: bare.ByteCursor, x: readonly ScheduleEventRow[]): void {
	bare.writeUintSafe(bc, x.length);
	for (let i = 0; i < x.length; i++) {
		writeScheduleEventRow(bc, x[i]);
	}
}

function read9(bc: bare.ByteCursor): readonly ScheduleHistoryRow[] {
	const len = bare.readUintSafe(bc);
	if (len === 0) {
		return [];
	}
	const result = [readScheduleHistoryRow(bc)];
	for (let i = 1; i < len; i++) {
		result[i] = readScheduleHistoryRow(bc);
	}
	return result;
}

function write9(bc: bare.ByteCursor, x: readonly ScheduleHistoryRow[]): void {
	bare.writeUintSafe(bc, x.length);
	for (let i = 0; i < x.length; i++) {
		writeScheduleHistoryRow(bc, x[i]);
	}
}

export type WorkflowFixture = {
	readonly metadata: FixtureMetadata;
	readonly metaRows: readonly MetaRow[];
	readonly runtime: RuntimeRow | null;
	readonly actor: ActorRow | null;
	readonly actorState: ArrayBuffer | null;
	readonly workflowRows: readonly WorkflowRow[];
	readonly queueRows: readonly QueueRow[];
	readonly scheduleEvents: readonly ScheduleEventRow[];
	readonly scheduleHistory: readonly ScheduleHistoryRow[];
};

export function readWorkflowFixture(bc: bare.ByteCursor): WorkflowFixture {
	return {
		metadata: readFixtureMetadata(bc),
		metaRows: read3(bc),
		runtime: read4(bc),
		actor: read5(bc),
		actorState: read2(bc),
		workflowRows: read6(bc),
		queueRows: read7(bc),
		scheduleEvents: read8(bc),
		scheduleHistory: read9(bc),
	};
}

export function writeWorkflowFixture(
	bc: bare.ByteCursor,
	x: WorkflowFixture,
): void {
	writeFixtureMetadata(bc, x.metadata);
	write3(bc, x.metaRows);
	write4(bc, x.runtime);
	write5(bc, x.actor);
	write2(bc, x.actorState);
	write6(bc, x.workflowRows);
	write7(bc, x.queueRows);
	write8(bc, x.scheduleEvents);
	write9(bc, x.scheduleHistory);
}

export function encodeWorkflowFixture(x: WorkflowFixture): Uint8Array {
	const bc = new bare.ByteCursor(
		new Uint8Array(config.initialBufferLength),
		config,
	);
	writeWorkflowFixture(bc, x);
	return new Uint8Array(bc.view.buffer, bc.view.byteOffset, bc.offset);
}

export function decodeWorkflowFixture(bytes: Uint8Array): WorkflowFixture {
	const bc = new bare.ByteCursor(bytes, config);
	const result = readWorkflowFixture(bc);
	if (bc.offset < bc.view.byteLength) {
		throw new bare.BareError(bc.offset, "remaining bytes");
	}
	return result;
}

function _assert(condition: boolean, message?: string): asserts condition {
	if (!condition) throw new Error(message ?? "Assertion failed");
}
