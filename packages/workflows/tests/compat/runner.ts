import { webcrypto } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import * as cbor from "cbor-x";
import type { WorkflowContextInterface } from "../../src/index.js";
import {
	decodeFixture,
	encodeFixture,
	type WorkflowFixture,
} from "./fixture-codec";
import {
	CompatibilityDriver,
	decodeActorState,
	encodeActorState,
} from "./fixture-driver";

type Engine = typeof import("../../src/index.js");
type Implementation = "old" | "new";
type Operation = "generate" | "resume";
type Scenario = "pending-sleep" | "pending-message" | "replay-retry";

const LEGACY_REVISION = "b70c717c1a8eef2fe018f803a4ba160547296432";
const FIXED_CLOCKS: Record<Scenario, number> = {
	"pending-sleep": 1_723_456_789_000,
	"pending-message": 1_723_556_789_000,
	"replay-retry": 1_723_656_789_000,
};
const ID_SEEDS: Record<Scenario, number> = {
	"pending-sleep": 100,
	"pending-message": 200,
	"replay-retry": 300,
};

function readArg(name: string): string {
	const index = process.argv.indexOf(`--${name}`);
	const value = process.argv[index + 1];
	if (index === -1 || !value) throw new Error(`Missing --${name}`);
	return value;
}

async function loadEngine(implementation: Implementation): Promise<Engine> {
	if (implementation === "old") {
		return (await import("legacy-workflow-engine")) as unknown as Engine;
	}
	return await import("../../src/index.js");
}

function installDeterminism(scenario: Scenario): {
	setNow(value: number): void;
} {
	let now = FIXED_CLOCKS[scenario];
	Date.now = () => now++;
	let nextId = ID_SEEDS[scenario];
	Object.defineProperty(globalThis, "crypto", {
		configurable: true,
		value: Object.create(webcrypto, {
			randomUUID: {
				configurable: true,
				value: () => {
					const suffix = String(nextId++).padStart(12, "0");
					return `00000000-0000-4000-8000-${suffix}`;
				},
			},
		}),
	});
	return {
		setNow(value: number) {
			now = value;
		},
	};
}

function metadata(
	implementation: Implementation,
	scenario: Scenario,
): WorkflowFixture["metadata"] {
	return {
		fixtureName: scenario,
		sourceRivetkitVersion: implementation === "old" ? "2.3.7" : "2.4.0-local",
		sourceWorkflowVersion: implementation === "old" ? "2.3.7" : "2.3.10-local",
		sourceRevision:
			implementation === "old" ? LEGACY_REVISION : "local-working-copy",
		actorId: `compat-${scenario}`,
		registryKey: "workflowCompatibilityFixture",
		internalSchemaVersion: 1n,
		fakeClockSeed: BigInt(FIXED_CLOCKS[scenario]),
		generatedIdSeed: BigInt(ID_SEEDS[scenario]),
	};
}

function fixtureRows(fixture: WorkflowFixture): string[] {
	return fixture.workflowRows.map(
		(row) =>
			`${Buffer.from(row.key).toString("hex")}:${Buffer.from(row.value).toString("hex")}`,
	);
}

async function generateSleep(
	engine: Engine,
	implementation: Implementation,
): Promise<{ fixture: WorkflowFixture; summary: object }> {
	const scenario = "pending-sleep";
	const actorId = `compat-${scenario}`;
	const driver = new CompatibilityDriver(undefined, {
		persistRunWakeMetadata: implementation === "new",
	});
	driver.workerPollInterval = 0;
	let mutations = 0;
	const workflow = async (ctx: WorkflowContextInterface) => {
		await ctx.step("mutate-once", async () => {
			mutations++;
			driver.actorState = encodeActorState({ mutations });
			return mutations;
		});
		await ctx.sleep("pending-sleep", 10_000);
		return "woke";
	};
	const result = await engine.runWorkflow(
		actorId,
		workflow,
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	if (result.state !== "sleeping" || result.sleepUntil === undefined) {
		throw new Error(`Expected sleeping fixture, received ${result.state}`);
	}
	const scheduledAt = result.sleepUntil + 5_000;
	driver.scheduleEvents = [
		{
			eventId: "unrelated-schedule",
			triggerAt: BigInt(scheduledAt),
			action: "tick",
			args: null,
			kind: 0n,
			cronExpression: null,
			timezone: null,
			intervalMs: null,
			lastStartedAt: null,
			maxHistory: 0n,
		},
	];
	const fixture = driver.toFixture(metadata(implementation, scenario), {
		runWakeAt: implementation === "new" ? BigInt(result.sleepUntil) : null,
		lastPushedAlarm: BigInt(Math.min(result.sleepUntil, scheduledAt)),
	});
	return {
		fixture,
		summary: {
			state: result.state,
			mutations,
			workflowDeadline: result.sleepUntil,
			scheduledDeadline: scheduledAt,
			effectiveDeadline: Number(fixture.runtime?.lastPushedAlarm),
			rows: fixtureRows(fixture),
		},
	};
}

async function resumeSleep(
	engine: Engine,
	implementation: Implementation,
	fixture: WorkflowFixture,
) {
	const driver = new CompatibilityDriver(fixture, {
		persistRunWakeMetadata: implementation === "new",
	});
	const state = decodeActorState<{ mutations: number }>(driver.actorState);
	const workflow = async (ctx: WorkflowContextInterface) => {
		await ctx.step("mutate-once", async () => {
			throw new Error("completed state-mutating step executed twice");
		});
		await ctx.sleep("pending-sleep", 10_000);
		return "woke";
	};
	const result = await engine.runWorkflow(
		fixture.metadata.actorId,
		workflow,
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	const storage = await engine.loadStorage(driver);
	return {
		fixture: driver.toFixture(fixture.metadata),
		summary: {
			state: result.state,
			output: result.output,
			mutations: state.mutations,
			historyEntries: storage.history.entries.size,
			rows: fixtureRows(driver.toFixture(fixture.metadata)),
		},
	};
}

async function generateMessage(
	engine: Engine,
	implementation: Implementation,
): Promise<{ fixture: WorkflowFixture; summary: object }> {
	const scenario = "pending-message";
	const actorId = `compat-${scenario}`;
	const driver = new CompatibilityDriver(undefined, {
		persistRunWakeMetadata: implementation === "new",
	});
	const workflow = async (ctx: WorkflowContextInterface) => {
		const message = await ctx.queue.next<string>("wait-for-signal", {
			names: ["signal"],
		});
		return message.body;
	};
	const result = await engine.runWorkflow(
		actorId,
		workflow,
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	await driver.messageDriver.addMessage({
		id: "7",
		name: "approval",
		data: { request: "release" },
		sentAt: FIXED_CLOCKS[scenario] + 1,
	});
	const fixture = driver.toFixture(metadata(implementation, scenario));
	return {
		fixture,
		summary: {
			state: result.state,
			waitingForMessages: result.waitingForMessages,
			queueSize: driver.queueSize(),
			rows: fixtureRows(fixture),
		},
	};
}

async function resumeMessage(
	engine: Engine,
	implementation: Implementation,
	fixture: WorkflowFixture,
) {
	const driver = new CompatibilityDriver(fixture, {
		persistRunWakeMetadata: implementation === "new",
	});
	const workflow = async (ctx: WorkflowContextInterface) => {
		const message = await ctx.queue.next<string>("wait-for-signal", {
			names: ["signal"],
		});
		return message.body;
	};
	await driver.messageDriver.addMessage({
		id: "8",
		name: "signal",
		data: "continue",
		sentAt: Number(fixture.metadata.fakeClockSeed) + 10,
	});
	const result = await engine.runWorkflow(
		fixture.metadata.actorId,
		workflow,
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	const [approval] = (await driver.messageDriver.receiveMessages({
		names: ["approval"],
		count: 1,
		completable: true,
	})) as Array<{ complete?: (response?: unknown) => Promise<void> }>;
	if (!approval?.complete) throw new Error("Missing persisted completable row");
	await approval.complete({ accepted: true });
	const storage = await engine.loadStorage(driver);
	return {
		fixture: driver.toFixture(fixture.metadata),
		summary: {
			state: result.state,
			output: result.output,
			completion: driver.completions[0],
			queueSize: driver.queueSize(),
			historyEntries: storage.history.entries.size,
			rows: fixtureRows(driver.toFixture(fixture.metadata)),
		},
	};
}

function replayWorkflow(
	engine: Engine,
	options: {
		replaying: boolean;
		attempts: { value: number };
		timeline: string[];
	},
) {
	return async (ctx: WorkflowContextInterface) => {
		await ctx.loop({
			name: "loop",
			state: 0,
			run: async (loopCtx, state) => {
				await loopCtx.step(`loop-${state}`, async () => {
					if (options.replaying) {
						throw new Error("history before replay boundary reran");
					}
					options.timeline.push(`loop-${state}`);
				});
				return state === 1
					? engine.Loop.break(undefined)
					: engine.Loop.continue(1);
			},
		});
		await ctx.join("branches", {
			left: {
				run: async (branchCtx) =>
					await branchCtx.step("left-step", async () => {
						options.timeline.push("left-step");
						return "left";
					}),
			},
			right: {
				run: async (branchCtx) =>
					await branchCtx.step("right-step", async () => {
						options.timeline.push("right-step");
						return "right";
					}),
			},
		});
		await ctx.step({
			name: "flaky-step",
			maxRetries: 2,
			retryBackoffBase: 100,
			retryBackoffMax: 100,
			run: async () => {
				options.attempts.value++;
				options.timeline.push(`flaky-${options.attempts.value}`);
				if (!options.replaying && options.attempts.value === 1) {
					throw new Error("fixture retry");
				}
				return "recovered";
			},
		});
		await ctx.step("after", async () => {
			options.timeline.push("after");
			return "complete";
		});
		return "complete";
	};
}

function findStepId(
	storage: Awaited<ReturnType<Engine["loadStorage"]>>,
	name: string,
) {
	const nameIndex = storage.nameRegistry.indexOf(name);
	return [...storage.history.entries.values()].find(
		(entry) =>
			entry.kind.type === "step" &&
			entry.location[entry.location.length - 1] === nameIndex,
	)?.id;
}

async function generateReplay(
	engine: Engine,
	implementation: Implementation,
	clock: { setNow(value: number): void },
): Promise<{ fixture: WorkflowFixture; summary: object }> {
	const scenario = "replay-retry";
	const actorId = `compat-${scenario}`;
	const driver = new CompatibilityDriver(undefined, {
		persistRunWakeMetadata: implementation === "new",
	});
	const options = {
		replaying: false,
		attempts: { value: 0 },
		timeline: [] as string[],
	};
	const first = await engine.runWorkflow(
		actorId,
		replayWorkflow(engine, options),
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	if (first.state !== "sleeping") {
		throw new Error(`Expected retry sleep, received ${first.state}`);
	}
	clock.setNow(FIXED_CLOCKS[scenario] + 1_000);
	const second = await engine.runWorkflow(
		actorId,
		replayWorkflow(engine, options),
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	if (second.state !== "completed") {
		throw new Error(
			`Expected completed retry fixture, received ${second.state}`,
		);
	}
	const storage = await engine.loadStorage(driver);
	const targetStepId = findStepId(storage, "flaky-step");
	if (!targetStepId) throw new Error("Missing replay target step");
	driver.scheduleHistory = [
		{
			id: 1n,
			scheduleId: "fixture-history",
			action: "retry-observer",
			scheduledAt: BigInt(FIXED_CLOCKS[scenario]),
			firedAt: BigInt(FIXED_CLOCKS[scenario] + 1),
			finishedAt: BigInt(FIXED_CLOCKS[scenario] + 2),
			result: 1n,
			errorGroup: "workflow",
			errorCode: "fixture_retry",
			errorMessage: "fixture retry",
			errorMetadata: Uint8Array.from(cbor.encode({ attempt: 1 })).buffer,
		},
	];
	const fixture = driver.toFixture(metadata(implementation, scenario));
	return {
		fixture,
		summary: {
			state: second.state,
			attempts: options.attempts.value,
			timeline: options.timeline,
			replayTarget: targetStepId,
			historyEntries: storage.history.entries.size,
			rows: fixtureRows(fixture),
		},
	};
}

async function resumeReplay(
	engine: Engine,
	implementation: Implementation,
	fixture: WorkflowFixture,
) {
	const driver = new CompatibilityDriver(fixture, {
		persistRunWakeMetadata: implementation === "new",
	});
	const storage = await engine.loadStorage(driver);
	const targetStepId = findStepId(storage, "flaky-step");
	if (!targetStepId)
		throw new Error("Missing replay target in restored fixture");
	await engine.replayWorkflowFromStep(
		fixture.metadata.actorId,
		driver,
		targetStepId,
		{ scheduleAlarm: false },
	);
	const options = {
		replaying: true,
		attempts: { value: 0 },
		timeline: [] as string[],
	};
	const result = await engine.runWorkflow(
		fixture.metadata.actorId,
		replayWorkflow(engine, options),
		undefined,
		driver,
		{ mode: "yield" },
	).result;
	const replayed = await engine.loadStorage(driver);
	return {
		fixture: driver.toFixture(fixture.metadata),
		summary: {
			state: result.state,
			output: result.output,
			timeline: options.timeline,
			historyEntries: replayed.history.entries.size,
			rows: fixtureRows(driver.toFixture(fixture.metadata)),
		},
	};
}

async function main() {
	const implementation = readArg("implementation") as Implementation;
	const operation = readArg("operation") as Operation;
	const scenario = readArg("scenario") as Scenario;
	const output = readArg("output");
	const clock = installDeterminism(scenario);
	const engine = await loadEngine(implementation);
	let result: { fixture: WorkflowFixture; summary: object };

	if (operation === "generate") {
		if (scenario === "pending-sleep") {
			result = await generateSleep(engine, implementation);
		} else if (scenario === "pending-message") {
			result = await generateMessage(engine, implementation);
		} else {
			result = await generateReplay(engine, implementation, clock);
		}
	} else {
		const input = readArg("input");
		const fixture = decodeFixture(await readFile(input));
		clock.setNow(Number(fixture.metadata.fakeClockSeed) + 20_000);
		if (scenario === "pending-sleep") {
			result = await resumeSleep(engine, implementation, fixture);
		} else if (scenario === "pending-message") {
			result = await resumeMessage(engine, implementation, fixture);
		} else {
			result = await resumeReplay(engine, implementation, fixture);
		}
	}

	await writeFile(output, encodeFixture(result.fixture));
	process.stdout.write(`RESULT ${JSON.stringify(result.summary)}\n`);
}

await main();
