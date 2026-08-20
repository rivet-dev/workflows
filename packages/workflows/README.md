# @rivet-dev/workflows

Durable, replayable workflows for Rivet Actors.

[Documentation](https://rivet.dev/workflows/docs)

```ts
import { workflow } from "@rivet-dev/workflows";

export const example = workflow({
	run: async (ctx) => {
		await ctx.step("hello", async () => "world");
	},
});
```

The workflow storage format is owned and migrated by RivetKit. This package is
a format-compatible client and never creates or migrates RivetKit's internal
SQLite tables.
