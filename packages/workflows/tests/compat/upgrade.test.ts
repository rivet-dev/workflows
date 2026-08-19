import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import * as cbor from "cbor-x";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { decodeFixture } from "./fixture-codec";

const execFileAsync = promisify(execFile);
const here = dirname(fileURLToPath(import.meta.url));
const workspaceRoot = join(here, "../../../..");
const runner = join(here, "runner.ts");
const tsx = join(workspaceRoot, "node_modules/.bin/tsx");
const fixtureDirectory = join(here, "fixtures");
const scenarios = ["pending-sleep", "pending-message", "replay-retry"] as const;
type Scenario = (typeof scenarios)[number];
type Implementation = "old" | "new";

let scratchDirectory: string;

beforeAll(async () => {
	scratchDirectory = await mkdtemp(join(tmpdir(), "rivet-workflow-compat-"));
});

afterAll(async () => {
	await rm(scratchDirectory, { recursive: true, force: true });
});

async function runFixtureProcess(options: {
	implementation: Implementation;
	operation: "generate" | "resume";
	scenario: Scenario;
	input?: string;
}): Promise<{ output: string; summary: Record<string, unknown> }> {
	const output = join(
		scratchDirectory,
		`${options.scenario}-${options.implementation}-${options.operation}-${crypto.randomUUID()}.vbare`,
	);
	const args = [
		runner,
		"--implementation",
		options.implementation,
		"--operation",
		options.operation,
		"--scenario",
		options.scenario,
		"--output",
		output,
	];
	if (options.input) args.push("--input", options.input);
	const { stdout, stderr } = await execFileAsync(tsx, args, {
		cwd: workspaceRoot,
		env: { ...process.env, NO_COLOR: "1" },
		maxBuffer: 8 * 1024 * 1024,
	});
	if (stderr.trim()) throw new Error(stderr);
	const resultLine = stdout
		.trim()
		.split("\n")
		.findLast((line) => line.startsWith("RESULT "));
	if (!resultLine) throw new Error(`Missing runner result:\n${stdout}`);
	return {
		output,
		summary: JSON.parse(resultLine.slice("RESULT ".length)),
	};
}

describe.sequential("persisted workflow package compatibility", () => {
	for (const scenario of scenarios) {
		test(`${scenario}: old write -> new resume and new write -> old resume`, async () => {
			const committedPath = join(fixtureDirectory, `${scenario}.vbare`);
			const oldGenerated = await runFixtureProcess({
				implementation: "old",
				operation: "generate",
				scenario,
			});
			expect(await readFile(oldGenerated.output)).toEqual(
				await readFile(committedPath),
			);

			const legacyFixture = decodeFixture(await readFile(committedPath));
			expect([
				...new Uint8Array(await readFile(committedPath)).slice(0, 2),
			]).toEqual([1, 0]);
			expect(legacyFixture.metadata).toMatchObject({
				fixtureName: scenario,
				sourceRivetkitVersion: "2.3.7",
				sourceWorkflowVersion: "2.3.7",
				sourceRevision: "b70c717c1a8eef2fe018f803a4ba160547296432",
				registryKey: "workflowCompatibilityFixture",
				internalSchemaVersion: 1n,
			});
			for (const row of legacyFixture.workflowRows) {
				expect([...new Uint8Array(row.key).slice(0, 2)]).toEqual([6, 1]);
			}

			const newResumed = await runFixtureProcess({
				implementation: "new",
				operation: "resume",
				scenario,
				input: committedPath,
			});
			expect(newResumed.summary.state).toBe("completed");

			const newGenerated = await runFixtureProcess({
				implementation: "new",
				operation: "generate",
				scenario,
			});
			expect(newGenerated.summary.rows).toEqual(oldGenerated.summary.rows);
			const newFixture = decodeFixture(await readFile(newGenerated.output));
			expect(newFixture.metadata.internalSchemaVersion).toBe(1n);
			const newRunWake = newFixture.metaRows.find(
				(row) => row.key === "run_wake_at",
			);
			if (!newRunWake) throw new Error("Missing new logical run-wake row");
			expect([...new Uint8Array(newRunWake.value).slice(0, 2)]).toEqual([1, 0]);
			const oldResumed = await runFixtureProcess({
				implementation: "old",
				operation: "resume",
				scenario,
				input: newGenerated.output,
			});
			expect(oldResumed.summary.state).toBe("completed");
			const oldResultFixture = decodeFixture(await readFile(oldResumed.output));
			expect(
				oldResultFixture.metaRows.find((row) => row.key === "run_wake_at")
					?.value,
			).toEqual(newRunWake?.value);

			if (scenario === "pending-sleep") {
				expect(oldGenerated.summary).toMatchObject({
					state: "sleeping",
					mutations: 1,
				});
				expect(oldGenerated.summary.effectiveDeadline).toBe(
					oldGenerated.summary.workflowDeadline,
				);
				expect(Number(oldGenerated.summary.scheduledDeadline)).toBeGreaterThan(
					Number(oldGenerated.summary.workflowDeadline),
				);
				expect(newResumed.summary).toMatchObject({
					output: "woke",
					mutations: 1,
				});
				expect(oldResumed.summary).toMatchObject({
					output: "woke",
					mutations: 1,
				});
				if (!legacyFixture.actorState) {
					throw new Error("Missing persisted actor state");
				}
				expect(cbor.decode(new Uint8Array(legacyFixture.actorState))).toEqual({
					mutations: 1,
				});
				expect(legacyFixture.scheduleEvents[0]?.triggerAt).toBe(
					BigInt(oldGenerated.summary.scheduledDeadline as number),
				);
			}

			if (scenario === "pending-message") {
				expect(oldGenerated.summary).toMatchObject({
					state: "sleeping",
					waitingForMessages: ["signal"],
					queueSize: 1,
				});
				expect(legacyFixture.queueRows).toHaveLength(1);
				expect(legacyFixture.queueRows[0]).toMatchObject({
					id: 7n,
					name: "approval",
				});
				expect(
					cbor.decode(new Uint8Array(legacyFixture.queueRows[0].body)),
				).toEqual({ request: "release" });
				expect(newResumed.summary).toMatchObject({
					output: "continue",
					queueSize: 0,
					completion: {
						id: "7",
						name: "approval",
						response: { accepted: true },
					},
				});
				expect(oldResumed.summary).toMatchObject({
					output: "continue",
					queueSize: 0,
				});
			}

			if (scenario === "replay-retry") {
				expect(oldGenerated.summary).toMatchObject({
					state: "completed",
					attempts: 2,
					timeline: [
						"loop-0",
						"loop-1",
						"left-step",
						"right-step",
						"flaky-1",
						"flaky-2",
						"after",
					],
				});
				expect(legacyFixture.scheduleHistory[0]).toMatchObject({
					errorGroup: "workflow",
					errorCode: "fixture_retry",
					errorMessage: "fixture retry",
				});
				expect(newResumed.summary).toMatchObject({
					output: "complete",
					timeline: ["flaky-1", "after"],
				});
				expect(oldResumed.summary).toMatchObject({
					output: "complete",
					timeline: ["flaky-1", "after"],
				});
			}
		}, 30_000);
	}
});
