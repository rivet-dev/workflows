// Runs one step that finishes well before its 30-day timeout, then prints the
// result. The process must exit on its own once the workflow completes.
import { InMemoryDriver, runWorkflow } from "../../src/testing.js";

const mode = process.argv[2] === "live" ? "live" : "yield";
const driver = new InMemoryDriver();
driver.latency = 0;

const result = await runWorkflow(
	"wf-1",
	async (ctx) =>
		await ctx.step({
			name: "quick-step",
			timeout: 30 * 24 * 60 * 60 * 1000,
			run: async () => "done",
		}),
	undefined,
	driver,
	{ mode },
).result;

process.stdout.write(JSON.stringify(result));
