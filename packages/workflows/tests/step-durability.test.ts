import { describe, expect, it, vi } from "vitest";
import {
	flush,
	InMemoryDriver,
	JoinError,
	type KVWrite,
	loadStorage,
	runWorkflow,
	StepExhaustedError,
	type WorkflowContextInterface,
	type WorkflowRunMode,
} from "../src/testing.js";

class CrashingDriver extends InMemoryDriver {
	crashed = false;

	override async batch(writes: KVWrite[]): Promise<void> {
		if (this.crashed) {
			throw new Error("process stopped");
		}
		await super.batch(writes);
	}
}

class HeldWriteDriver extends InMemoryDriver {
	holdNext = false;
	#started = Promise.withResolvers<void>();
	#release = Promise.withResolvers<void>();
	readonly writeStarted = this.#started.promise;

	releaseWrite(): void {
		this.#release.resolve();
	}

	override async batch(writes: KVWrite[]): Promise<void> {
		if (this.holdNext) {
			this.holdNext = false;
			this.#started.resolve();
			await this.#release.promise;
		}
		await super.batch(writes);
	}
}

function newHeldDriver(): HeldWriteDriver {
	const driver = new HeldWriteDriver();
	driver.latency = 0;
	driver.workerPollInterval = 0;
	return driver;
}

function newDriver(): CrashingDriver {
	const driver = new CrashingDriver();
	driver.latency = 0;
	driver.workerPollInterval = 0;
	return driver;
}

async function savedStepMetadata(driver: InMemoryDriver, key: string) {
	const storage = await loadStorage(driver);
	const entry = storage.history.entries.get(key);
	if (entry?.kind.type !== "step") throw new Error(`no step at ${key}`);
	return storage.entryMetadata.get(entry.id);
}

async function setSavedLastAttemptAt(
	driver: InMemoryDriver,
	key: string,
	lastAttemptAt: number,
) {
	const storage = await loadStorage(driver);
	const entry = storage.history.entries.get(key);
	if (entry?.kind.type !== "step") throw new Error(`no step at ${key}`);
	const metadata = storage.entryMetadata.get(entry.id);
	if (!metadata) throw new Error(`no metadata for ${key}`);
	metadata.lastAttemptAt = lastAttemptAt;
	metadata.dirty = true;
	await flush(storage, driver);
}

async function savedStepMetadataList(driver: InMemoryDriver) {
	const storage = await loadStorage(driver);
	return [...storage.history.entries.values()]
		.filter((entry) => entry.kind.type === "step")
		.map((entry) => storage.entryMetadata.get(entry.id));
}

describe.each<WorkflowRunMode>(["yield", "live"])("%s mode", (mode) => {
	describe.each([0, 2])("maxRetries %i", (maxRetries) => {
		it("counts a crash after the callback as an attempt and exhausts the step from persisted data", async () => {
			const driver = newDriver();
			let executions = 0;
			const workflow = (ctx: WorkflowContextInterface) =>
				ctx.step({
					name: "work",
					maxRetries,
					retryBackoffBase: 0,
					retryBackoffMax: 0,
					run: async () => {
						executions++;
						driver.crashed = true;
						return "ok";
					},
				});

			for (let restart = 0; restart <= maxRetries; restart++) {
				await expect(
					runWorkflow("wf", workflow, undefined, driver, { mode }).result,
				).rejects.toThrow("process stopped");
				driver.crashed = false;

				expect(executions).toBe(restart + 1);
				expect(await savedStepMetadata(driver, "work")).toMatchObject({
					status: "running",
					attempts: executions,
				});
			}

			await expect(
				runWorkflow("wf", workflow, undefined, driver, { mode }).result,
			).rejects.toBeInstanceOf(StepExhaustedError);
			expect(executions).toBe(maxRetries + 1);

			await expect(
				runWorkflow("wf", workflow, undefined, driver, { mode }).result,
			).rejects.toBeInstanceOf(StepExhaustedError);
			expect(executions).toBe(maxRetries + 1);
		});
	});
});

describe.each<WorkflowRunMode>([
	"yield",
	"live",
])("%s mode interrupted callback", (mode) => {
	it("counts the crashed attempt, succeeds on the next run, and replays the result without the callback", async () => {
		const driver = newDriver();
		const observed: Array<{ status?: string; attempts?: number }> = [];
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.step({
				name: "work",
				maxRetries: 1,
				retryBackoffBase: 0,
				retryBackoffMax: 0,
				run: async () => {
					const saved = await savedStepMetadata(driver, "work");
					observed.push({ status: saved?.status, attempts: saved?.attempts });
					if (observed.length === 1) driver.crashed = true;
					return `done ${observed.length}`;
				},
			});
		const run = () =>
			runWorkflow("wf", workflow, undefined, driver, { mode }).result;

		await expect(run()).rejects.toThrow("process stopped");
		driver.crashed = false;
		expect(observed).toEqual([{ status: "running", attempts: 1 }]);
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "running",
			attempts: 1,
		});

		await expect(run()).resolves.toMatchObject({
			state: "completed",
			output: "done 2",
		});
		expect(observed).toEqual([
			{ status: "running", attempts: 1 },
			{ status: "running", attempts: 2 },
		]);
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "completed",
			attempts: 2,
		});

		await expect(run()).resolves.toMatchObject({
			state: "completed",
			output: "done 2",
		});
		expect(observed).toHaveLength(2);
	});
});

describe("recovered running attempt backoff", () => {
	it("sleeps until lastAttemptAt plus the configured backoff before retrying in yield mode", async () => {
		const driver = newDriver();
		let executions = 0;
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.step({
				name: "work",
				maxRetries: 1,
				retryBackoffBase: 1000,
				retryBackoffMax: 60_000,
				run: async () => {
					if (executions++ === 0) driver.crashed = true;
					return "ok";
				},
			});
		const run = () =>
			runWorkflow("wf", workflow, undefined, driver, { mode: "yield" }).result;

		await expect(run()).rejects.toThrow("process stopped");
		driver.crashed = false;
		expect(executions).toBe(1);

		const lastAttemptAt = Date.now() + 1_000_000;
		await setSavedLastAttemptAt(driver, "work", lastAttemptAt);
		await expect(run()).resolves.toEqual({
			state: "sleeping",
			sleepUntil: lastAttemptAt + 2000,
		});
		expect(executions).toBe(1);
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "running",
			attempts: 1,
			lastAttemptAt,
		});

		await setSavedLastAttemptAt(driver, "work", 0);
		await expect(run()).resolves.toMatchObject({
			state: "completed",
			output: "ok",
		});
		expect(executions).toBe(2);
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "completed",
			attempts: 2,
		});
	});
});

describe("admission save failure", () => {
	it.each([
		"before commit",
		"after commit",
		"history notification",
	] as const)("fails closed when the admission checkpoint rejects (%s) and recover() resets the step", async (failure) => {
		const driver = newDriver();
		let callbacks = 0;
		const writeError = new Error("storage unavailable");
		const batch = driver.batch.bind(driver);
		let committedAdmission: unknown;

		if (failure !== "history notification") {
			vi.spyOn(driver, "batch").mockImplementationOnce(async (writes) => {
				if (failure === "after commit") {
					await batch(writes);
					committedAdmission = await savedStepMetadata(driver, "work");
				}
				throw writeError;
			});
		}
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.step({
				name: "work",
				maxRetries: 0,
				retryBackoffBase: 0,
				retryBackoffMax: 0,
				run: async () => {
					callbacks++;
					return "ok";
				},
			});

		const failed = runWorkflow("wf", workflow, undefined, driver, {
			onHistoryUpdated:
				failure === "history notification"
					? (snapshot) => {
							for (const metadata of snapshot.entryMetadata.values()) {
								if (metadata.status === "running") throw writeError;
							}
						}
					: undefined,
		});
		await expect(failed.result).rejects.toBe(writeError);
		expect(callbacks).toBe(0);
		if (failure === "after commit") {
			expect(committedAdmission).toMatchObject({
				status: "running",
				attempts: 1,
			});
		}
		expect((await loadStorage(driver)).state).toBe("failed");
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "failed",
			attempts: 1,
			error: String(writeError),
		});

		let recoverable = failed;
		if (failure !== "history notification") {
			const restarted = runWorkflow("wf", workflow, undefined, driver);
			await expect(restarted.result).rejects.toBeInstanceOf(StepExhaustedError);
			expect(callbacks).toBe(0);
			expect(await savedStepMetadata(driver, "work")).toMatchObject({
				status: "exhausted",
				attempts: 1,
			});
			recoverable = restarted;
		}

		await recoverable.recover();
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "pending",
			attempts: 0,
			lastAttemptAt: 0,
			error: undefined,
		});
		expect((await loadStorage(driver)).state).toBe("sleeping");

		const fresh = runWorkflow("wf", workflow, undefined, driver);
		await expect(fresh.result).resolves.toMatchObject({
			state: "completed",
			output: "ok",
		});
		expect(callbacks).toBe(1);
		expect(await savedStepMetadata(driver, "work")).toMatchObject({
			status: "completed",
			attempts: 1,
		});
	});
});

describe("admission save interrupted", () => {
	const stepConfig = {
		name: "work",
		maxRetries: 1,
		retryBackoffBase: 0,
		retryBackoffMax: 0,
	};

	it.each([
		"root",
		"race branch",
	] as const)("eviction during a %s step admission keeps the attempt counted, skips the callback, and lets a fresh run complete", async (location) => {
		const driver = newHeldDriver();
		let executions = 0;
		let starts = 0;
		const runStep = (ctx: WorkflowContextInterface) => {
			if (starts++ === 0) driver.holdNext = true;
			return ctx.step({
				...stepConfig,
				run: async () => {
					executions++;
					return "ok";
				},
			});
		};

		const workflow = (ctx: WorkflowContextInterface): Promise<unknown> =>
			location === "root"
				? runStep(ctx)
				: ctx.race("race", [{ name: "only", run: runStep }]);

		const evicted = runWorkflow("wf", workflow, undefined, driver);
		await driver.writeStarted;
		evicted.evict();
		driver.releaseWrite();
		await expect(evicted.result).resolves.toEqual({ state: "running" });
		expect(executions).toBe(0);
		expect(await savedStepMetadataList(driver)).toMatchObject([
			{ status: "running", attempts: 1 },
		]);

		const fresh = runWorkflow("wf", workflow, undefined, driver);
		await expect(fresh.result).resolves.toMatchObject({ state: "completed" });
		expect(executions).toBe(1);
		expect(driver.holdNext).toBe(false);
		expect(await savedStepMetadataList(driver)).toMatchObject([
			{ status: "completed", attempts: 2 },
		]);
	});

	it("race winner cancels a loser whose admission save is still in flight", async () => {
		const driver = newHeldDriver();
		let loserExecutions = 0;
		const loserAborted = Promise.withResolvers<void>();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.race("race", [
				{
					name: "loser",
					run: async (branch) => {
						branch.abortSignal.addEventListener(
							"abort",
							() => loserAborted.resolve(),
							{ once: true },
						);
						driver.holdNext = true;
						return branch.step({
							...stepConfig,
							maxRetries: 0,
							run: async () => {
								loserExecutions++;
								return "loser";
							},
						});
					},
				},
				{
					name: "winner",
					run: async () => {
						await driver.writeStarted;
						return "winner";
					},
				},
			]);

		const handle = runWorkflow("wf", workflow, undefined, driver);
		await loserAborted.promise;
		expect(loserExecutions).toBe(0);
		driver.releaseWrite();

		await expect(handle.result).resolves.toMatchObject({
			state: "completed",
			output: { winner: "winner", value: "winner" },
		});
		expect(loserExecutions).toBe(0);

		const storage = await loadStorage(driver);
		expect(
			[...storage.history.entries.values()].filter(
				(entry) => entry.kind.type === "step",
			),
		).toEqual([]);
		expect(storage.entryMetadata.size).toBe(0);
	});
});

describe("concurrent admission", () => {
	const stepConfig = {
		maxRetries: 0,
		retryBackoffBase: 0,
		retryBackoffMax: 0,
	};

	it("admits a late join branch step behind a held sibling completion save before running its callback", async () => {
		const driver = newHeldDriver();
		const observed: Array<{ status?: string; attempts?: number }> = [];
		let lateCalls = 0;
		const lateSubmitted = Promise.withResolvers<void>();
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				first: {
					run: (branch) =>
						branch.step({
							...stepConfig,
							name: "first",
							run: async () => {
								driver.holdNext = true;
								return "first";
							},
						}),
				},
				late: {
					run: async (branch) => {
						await driver.writeStarted;
						const result = branch.step({
							...stepConfig,
							name: "late",
							run: async () => {
								const saved = await savedStepMetadata(driver, "jobs/late/late");
								observed.push({
									status: saved?.status,
									attempts: saved?.attempts,
								});
								lateCalls++;
								return "late";
							},
						});
						lateSubmitted.resolve();
						return result;
					},
				},
			});

		const handle = runWorkflow("wf", workflow, undefined, driver);
		await lateSubmitted.promise;
		expect(lateCalls).toBe(0);
		driver.releaseWrite();

		await expect(handle.result).resolves.toMatchObject({
			state: "completed",
			output: { first: "first", late: "late" },
		});
		expect(lateCalls).toBe(1);
		expect(observed).toEqual([{ status: "running", attempts: 1 }]);
		expect(await savedStepMetadata(driver, "jobs/late/late")).toMatchObject({
			status: "completed",
			attempts: 1,
		});
	});

	it("rejects both join branch steps whose coalesced admission save fails without running either callback", async () => {
		const driver = newDriver();
		let aCalls = 0;
		let bCalls = 0;
		const admissionError = new Error("storage unavailable");
		const workflow = (ctx: WorkflowContextInterface) =>
			ctx.join("jobs", {
				a: {
					run: (branch) => {
						vi.spyOn(driver, "batch").mockRejectedValueOnce(admissionError);
						return branch.step({
							...stepConfig,
							name: "a",
							run: async () => {
								aCalls++;
								return "a";
							},
						});
					},
				},
				b: {
					run: (branch) =>
						branch.step({
							...stepConfig,
							name: "b",
							run: async () => {
								bCalls++;
								return "b";
							},
						}),
				},
			});

		const handle = runWorkflow("wf", workflow, undefined, driver);
		await expect(handle.result).rejects.toBeInstanceOf(JoinError);
		expect(aCalls).toBe(0);
		expect(bCalls).toBe(0);
		expect(await savedStepMetadata(driver, "jobs/a/a")).toMatchObject({
			status: "failed",
			attempts: 1,
			error: String(admissionError),
		});
		expect(await savedStepMetadata(driver, "jobs/b/b")).toMatchObject({
			status: "failed",
			attempts: 1,
			error: String(admissionError),
		});
	});
});
