import { defineConfig } from "tsup";

export default defineConfig({
	entry: {
		index: "src/mod.ts",
		testing: "src/testing.ts",
	},
	target: "node18",
	platform: "node",
	format: ["cjs", "esm"],
	sourcemap: true,
	clean: true,
	dts: {
		compilerOptions: {
			skipLibCheck: true,
			resolveJsonModule: true,
		},
	},
	minify: false,
	splitting: true,
	skipNodeModulesBundle: true,
	external: [/^node:.*/, /^rivetkit(?:\/.*)?$/],
	shims: true,
	outDir: "dist/tsup",
});
