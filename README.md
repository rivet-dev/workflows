# Rivet Workflows

Durable, replayable multi-step operations for Rivet Actors.

**[Documentation](https://rivet.dev/workflows/docs)** · **[Website](https://rivet.dev/workflows)** · **[Discord](https://rivet.dev/discord)**

```sh
pnpm add @rivet-dev/workflows
```

```ts
import { setup, workflow } from "@rivet-dev/workflows";

export const report = workflow({
	state: { status: "pending" as "pending" | "complete" },
	run: async (ctx) => {
		await ctx.step("generate", async (step) => {
			step.log.info("generating report");
			step.state.status = "complete";
		});
	},
	actions: {
		status: (ctx) => ctx.state,
	},
});

export const registry = setup({ use: { report } });
```

The package preserves the existing workflow history encoding and uses only
RivetKit's public workflow-host capabilities. It re-exports RivetKit, and package
managers install its compatible peer automatically, so workflow actors and
regular `actor(...)` definitions can share the same registry without another
direct dependency. RivetKit continues to own the internal SQLite schema and its
migrations.
