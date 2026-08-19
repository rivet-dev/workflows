# Rivet Workflows

Durable, replayable multi-step operations for Rivet Actors.

**[Documentation](https://rivet.dev/workflows/docs)** · **[Website](https://rivet.dev/workflows)** · **[Discord](https://rivet.dev/discord)**

```sh
pnpm add @rivet-dev/workflows rivetkit
```

```ts
import { actor } from "rivetkit";
import { workflow } from "@rivet-dev/workflows";

export const report = actor({
	run: workflow(async (ctx) => {
		await ctx.step("generate", async (step) => {
			step.log.info("generating report");
		});
	}),
});
```

The package preserves the existing workflow history encoding and uses only
RivetKit's public workflow-host capabilities. RivetKit continues to own the
internal SQLite schema and its migrations.
