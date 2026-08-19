import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";

const { values } = parseArgs({
	options: { "skip-install": { type: "boolean" } },
});
const root = new URL("..", import.meta.url).pathname;
const packageDir = join(root, "packages/workflows");
const temp = await mkdtemp(join(tmpdir(), "rivet-workflows-pack-"));

try {
	const tarballName = execFileSync(
		"npm",
		["pack", "--silent", "--pack-destination", temp],
		{
			cwd: packageDir,
			encoding: "utf8",
		},
	).trim();
	const tarball = join(temp, tarballName);
	const manifest = JSON.parse(
		await readFile(join(packageDir, "package.json"), "utf8"),
	);
	const rivetkitSpecifier = manifest.devDependencies?.rivetkit;
	if (typeof rivetkitSpecifier !== "string") {
		throw new Error("package must declare a RivetKit development dependency");
	}
	const packedFiles = JSON.parse(
		execFileSync("npm", ["pack", "--dry-run", "--json"], {
			cwd: packageDir,
			encoding: "utf8",
		}),
	)[0].files.map((entry: { path: string }) => entry.path);

	for (const required of [
		"dist/tsup/index.js",
		"dist/tsup/index.cjs",
		"dist/tsup/index.d.ts",
		"dist/tsup/index.d.cts",
		"dist/tsup/index.js.map",
		"dist/tsup/index.cjs.map",
		"dist/tsup/testing.js",
		"dist/tsup/testing.cjs",
		"dist/tsup/testing.d.ts",
		"dist/tsup/testing.d.cts",
		"dist/tsup/testing.js.map",
		"dist/tsup/testing.cjs.map",
		"schemas/v1.bare",
	]) {
		if (!packedFiles.includes(required))
			throw new Error(`packed tarball is missing ${required}`);
	}
	const serialized = JSON.stringify(manifest);
	if (serialized.includes("workspace:") || serialized.includes("catalog:")) {
		throw new Error(
			"packed manifest contains an unresolved workspace/catalog specifier",
		);
	}
	if (manifest.peerDependencies?.rivetkit !== ">=2.4.0 <3") {
		throw new Error("packed manifest has the wrong RivetKit peer range");
	}
	if (manifest.dependencies?.rivetkit) {
		throw new Error("RivetKit must remain a peer dependency");
	}

	const declarations = ["dist/tsup/index.d.ts", "dist/tsup/index.d.cts"];
	for (const declaration of declarations) {
		const source = await readFile(join(packageDir, declaration), "utf8");
		for (const token of [
			"rivetkit/src",
			"ACTOR_CONTEXT_INTERNAL_SYMBOL",
			"RUN_FUNCTION_CONFIG_SYMBOL",
			"AnyStaticActorInstance",
			"@rivetkit/workflow-engine",
		]) {
			if (source.includes(token)) {
				throw new Error(`${declaration} exposes private token ${token}`);
			}
		}
	}

	if (!values["skip-install"]) {
		const fixture = join(temp, "fixture");
		await mkdir(fixture);
		execFileSync("npm", ["init", "-y"], { cwd: fixture, stdio: "ignore" });
		const installArgs = ["install", "--ignore-scripts"];
		if (rivetkitSpecifier.startsWith("0.0.0-")) {
			installArgs.push("--legacy-peer-deps");
		}
		installArgs.push(tarball, `rivetkit@${rivetkitSpecifier}`);
		execFileSync("npm", installArgs, {
			cwd: fixture,
			stdio: "inherit",
		});
		execFileSync(
			"node",
			[
				"--input-type=module",
				"-e",
				"await import('@rivet-dev/workflows'); await import('@rivet-dev/workflows/testing')",
			],
			{
				cwd: fixture,
				stdio: "inherit",
			},
		);
		execFileSync(
			"node",
			[
				"-e",
				"require('@rivet-dev/workflows'); require('@rivet-dev/workflows/testing')",
			],
			{
				cwd: fixture,
				stdio: "inherit",
			},
		);
		await writeFile(
			join(fixture, "smoke.ts"),
			[
				'import { actor } from "rivetkit";',
				'import { workflow } from "@rivet-dev/workflows";',
				'import { InMemoryDriver } from "@rivet-dev/workflows/testing";',
				"const definition = actor({",
				"  run: workflow(async (ctx) => {",
				'    await ctx.step("typed", async (step) => {',
				'      step.log.info("compiled");',
				"      return 1;",
				"    });",
				"  }),",
				"});",
				"void definition; void new InMemoryDriver();",
			].join("\n"),
		);
		await writeFile(
			join(fixture, "tsconfig.json"),
			JSON.stringify({
				compilerOptions: {
					module: "NodeNext",
					moduleResolution: "NodeNext",
					noEmit: true,
					skipLibCheck: true,
					strict: true,
					target: "ES2022",
				},
				include: ["smoke.ts"],
			}),
		);
		execFileSync(
			"pnpm",
			[
				"--filter",
				"@rivet-dev/workflows",
				"exec",
				"tsc",
				"--project",
				join(fixture, "tsconfig.json"),
			],
			{ cwd: root, stdio: "inherit" },
		);
	}
} finally {
	await rm(temp, { recursive: true, force: true });
}
