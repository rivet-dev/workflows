import { availableParallelism, cpus } from "node:os";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const maxConcurrency = (() => {
	try {
		return availableParallelism();
	} catch {
		return cpus().length;
	}
})();

export default defineConfig({
	resolve: {
		alias: [
			{
				find: /^rivetkit$/,
				replacement: fileURLToPath(
					new URL("./tests/fixtures/rivetkit.ts", import.meta.url),
				),
			},
			{
				find: "rivetkit/storage",
				replacement: fileURLToPath(
					new URL("./tests/fixtures/rivetkit-storage.ts", import.meta.url),
				),
			},
			{
				find: "rivetkit/inspector/workflow",
				replacement: fileURLToPath(
					new URL(
						"./tests/fixtures/rivetkit-inspector-workflow.ts",
						import.meta.url,
					),
				),
			},
		],
	},
	test: {
		include: ["tests/**/*.test.ts"],
		exclude: ["tests/e2e/**"],
		testTimeout: 10_000,
		hookTimeout: 10_000,
		maxConcurrency,
		sequence: { concurrent: true },
	},
});
