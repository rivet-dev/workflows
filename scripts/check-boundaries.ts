import { readdir, readFile } from "node:fs/promises";
import { extname, join, relative } from "node:path";

const root = new URL("../packages/workflows", import.meta.url).pathname;
const forbidden = [
	"rivetkit/src",
	"@/",
	"ACTOR_CONTEXT_INTERNAL_SYMBOL",
	"RUN_FUNCTION_CONFIG_SYMBOL",
	"AnyStaticActorInstance",
	"@rivetkit/workflow-engine",
	"rivetkit/storage",
	"rivetkit/inspector/workflow",
	"ctx.storage",
	"WORKFLOW_STORAGE_V1",
	"flushWithState",
	"ctx.sql",
	"runCtx.sql",
	"_rivet_runtime",
	"_rivet_meta",
	"_rivet_queue",
	"CREATE TABLE _rivet_wf_kv",
	"ALTER TABLE _rivet_wf_kv",
];

async function files(path: string): Promise<string[]> {
	const output: string[] = [];
	for (const entry of await readdir(path, { withFileTypes: true })) {
		const child = join(path, entry.name);
		if (entry.isDirectory()) {
			output.push(...(await files(child)));
		} else if ([".ts", ".tsx", ".js", ".mjs"].includes(extname(child))) {
			output.push(child);
		}
	}
	return output;
}

const violations: string[] = [];
for (const file of await files(join(root, "src"))) {
	const source = await readFile(file, "utf8");
	for (const token of forbidden) {
		if (source.includes(token)) {
			violations.push(
				`${relative(root, file)}: forbidden token ${JSON.stringify(token)}`,
			);
		}
	}
}

if (violations.length > 0) {
	throw new Error(
		`Private RivetKit boundary violations:\n${violations.join("\n")}`,
	);
}
