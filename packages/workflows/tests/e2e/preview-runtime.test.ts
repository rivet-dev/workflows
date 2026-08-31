import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setupTest } from "rivetkit/test";
import { expect, test } from "vitest";
import { setup, workflow } from "../../src/mod";

const sleepAcrossWake = workflow({
	state: { completed: [] as string[] },
	run: async (ctx) => {
		await ctx.step("before-sleep", async (step) => {
			step.state.completed.push("before-sleep");
		});
		await ctx.sleep("sleep", 100);
		await ctx.step("after-sleep", async (step) => {
			step.state.completed.push("after-sleep");
		});
	},
	actions: {
		getCompleted: (ctx) => ctx.state.completed,
	},
	options: {
		sleepTimeout: 20,
	},
});
async function findAvailablePort(): Promise<number> {
	const server = createServer();
	return await new Promise<number>((resolve, reject) => {
		server.once("error", reject);
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			if (!address || typeof address === "string") {
				server.close();
				reject(new Error("failed to allocate a local engine port"));
				return;
			}
			server.close((error) => {
				if (error) reject(error);
				else resolve(address.port);
			});
		});
	});
}

async function stopTestEngine(storagePath: string): Promise<void> {
	let enginePid: number;
	try {
		const runtime = JSON.parse(
			await readFile(
				join(storagePath, ".rivetkit/var/engine/runtime.json"),
				"utf8",
			),
		) as { pid?: unknown };
		if (!Number.isSafeInteger(runtime.pid) || Number(runtime.pid) <= 0) return;
		enginePid = Number(runtime.pid);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
		throw error;
	}

	try {
		process.kill(enginePid, "SIGTERM");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
		throw error;
	}

	for (let attempt = 0; attempt < 50; attempt++) {
		await new Promise((resolve) => setTimeout(resolve, 100));
		try {
			process.kill(enginePid, 0);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
			throw error;
		}
	}

	throw new Error(`test engine ${enginePid} did not stop after SIGTERM`);
}

test("published runtime resumes a sleeping workflow exactly once", async (context) => {
	const enginePort = await findAvailablePort();
	const storagePath = await mkdtemp(join(tmpdir(), "rivet-workflows-e2e-"));
	const previousStoragePath = process.env.RIVETKIT_STORAGE_PATH;
	process.env.RIVETKIT_STORAGE_PATH = storagePath;
	const registry = setup({
		use: { sleepAcrossWake },
		startEngine: true,
		engineHost: "127.0.0.1",
		enginePort,
		shutdown: {
			disableSignalHandlers: true,
			gracePeriodMs: 5_000,
		},
	});
	let cleanedUp = false;
	const cleanup = async () => {
		if (cleanedUp) return;
		cleanedUp = true;
		await registry.shutdown();
		await stopTestEngine(storagePath);
		if (previousStoragePath === undefined) {
			delete process.env.RIVETKIT_STORAGE_PATH;
		} else {
			process.env.RIVETKIT_STORAGE_PATH = previousStoragePath;
		}
		await rm(storagePath, { recursive: true, force: true });
	};
	context.onTestFinished(cleanup);

	try {
		const { client } = await setupTest(context, registry);
		const handle = client.sleepAcrossWake.getOrCreate(["preview-e2e"]);
		const deadline = Date.now() + 10_000;
		let completed: string[] = [];

		while (Date.now() < deadline) {
			completed = await handle.getCompleted();
			if (completed.includes("after-sleep")) break;
			await new Promise((resolve) => setTimeout(resolve, 25));
		}

		expect(completed).toEqual(["before-sleep", "after-sleep"]);
		await new Promise((resolve) => setTimeout(resolve, 150));
		expect(await handle.getCompleted()).toEqual([
			"before-sleep",
			"after-sleep",
		]);
	} finally {
		await cleanup();
	}
});
