import { createVersionedDataHandler } from "vbare";
import {
	decodeWorkflowFixture,
	encodeWorkflowFixture,
	type WorkflowFixture,
} from "./generated/rivetkit-workflow-fixture-v1";

export type { WorkflowFixture };

const FIXTURE_VERSION = 1;
const FIXTURE_CODEC = createVersionedDataHandler<WorkflowFixture>({
	deserializeVersion: (bytes, version) => {
		if (version !== FIXTURE_VERSION) {
			throw new Error(`Unsupported workflow fixture version ${version}`);
		}
		return decodeWorkflowFixture(bytes);
	},
	serializeVersion: (fixture, version) => {
		if (version !== FIXTURE_VERSION) {
			throw new Error(`Unsupported workflow fixture version ${version}`);
		}
		return encodeWorkflowFixture(fixture);
	},
	deserializeConverters: () => [],
	serializeConverters: () => [],
});

export function encodeFixture(fixture: WorkflowFixture): Uint8Array {
	return FIXTURE_CODEC.serializeWithEmbeddedVersion(fixture, FIXTURE_VERSION);
}

export function decodeFixture(bytes: Uint8Array): WorkflowFixture {
	// Node's `Buffer#slice()` aliases its backing allocation, while the BARE data
	// decoder expects `Uint8Array#slice()` copy semantics for each data field.
	return FIXTURE_CODEC.deserializeWithEmbeddedVersion(Uint8Array.from(bytes));
}
