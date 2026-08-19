# @rivet-dev/workflows

Durable, replayable workflows for Rivet Actors.

```ts
import { actor } from "rivetkit";
import { workflow } from "@rivet-dev/workflows";

export const example = actor({
	run: workflow(async (ctx) => {
		await ctx.step("hello", async () => "world");
	}),
});
```

The workflow storage format is owned and migrated by RivetKit. This package is
a format-compatible client and never creates or migrates RivetKit's internal
SQLite tables.

