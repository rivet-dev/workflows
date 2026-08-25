export const WorkflowEntryStatus = {
	PENDING: "PENDING",
	RUNNING: "RUNNING",
	COMPLETED: "COMPLETED",
	FAILED: "FAILED",
	EXHAUSTED: "EXHAUSTED",
} as const;

export const WorkflowSleepState = {
	PENDING: "PENDING",
	COMPLETED: "COMPLETED",
	INTERRUPTED: "INTERRUPTED",
} as const;

export const WorkflowBranchStatusType = {
	PENDING: "PENDING",
	RUNNING: "RUNNING",
	COMPLETED: "COMPLETED",
	FAILED: "FAILED",
	CANCELLED: "CANCELLED",
} as const;

export type WorkflowLocation = unknown[];
export type WorkflowEntryKind = unknown;
export type WorkflowEntry = unknown;
export type WorkflowEntryStatus = string;
export type WorkflowSleepState = string;
export type WorkflowBranchStatusType = string;
export type WorkflowBranchStatus = unknown;
export type WorkflowEntryMetadata = unknown;
export type WorkflowHistory = unknown;
export interface WorkflowInspectorAdapter {
	getHistory(): ArrayBuffer | null;
	getState(): Promise<string | null>;
	onHistoryUpdated(listener: (history: ArrayBuffer) => void): () => void;
	replayFromStep(entryId?: string): Promise<ArrayBuffer | null>;
}

export function encodeWorkflowHistoryTransport(value: unknown): ArrayBuffer {
	const json = JSON.stringify(value, (_key, item) => {
		if (typeof item === "bigint") return item.toString();
		if (item instanceof Map) return [...item.entries()];
		if (item instanceof ArrayBuffer) return [...new Uint8Array(item)];
		return item;
	});
	return new TextEncoder().encode(json).buffer;
}

export function encodeWorkflowInspectorValue(value: unknown): ArrayBuffer {
	return new TextEncoder().encode(JSON.stringify(value)).buffer;
}
