import { describe, expect, test, vi } from "vitest";
import { createWorkflowInspectorAdapter } from "../../src/rivetkit/inspector";

describe("workflow Inspector adapter", () => {
	test("retains, publishes, and unsubscribes encoded history", () => {
		const inspector = createWorkflowInspectorAdapter();
		const listener = vi.fn();
		const unsubscribe = inspector.adapter.onHistoryUpdated(listener);
		const snapshot = {
			nameRegistry: [],
			entries: [],
			entryMetadata: new Map(),
		};

		inspector.update(snapshot);
		expect(listener).toHaveBeenCalledOnce();
		expect(inspector.adapter.getHistory()).toBeInstanceOf(ArrayBuffer);

		unsubscribe();
		inspector.update(snapshot);
		expect(listener).toHaveBeenCalledOnce();
	});

	test("delegates state and replay through actor-bound callbacks", async () => {
		const inspector = createWorkflowInspectorAdapter();
		const history = new Uint8Array([1, 2, 3]).buffer;
		const replay = vi.fn(async () => history);
		inspector.setGetState(async () => "sleeping");
		inspector.setReplayFromStep(replay);

		await expect(inspector.adapter.getState()).resolves.toBe("sleeping");
		await expect(inspector.adapter.replayFromStep("step-id")).resolves.toBe(
			history,
		);
		expect(replay).toHaveBeenCalledWith("step-id");
	});
});
