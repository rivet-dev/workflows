import {
	event,
	queue,
	type WorkflowStepContextOf,
	workflow,
} from "@rivet-dev/workflows";
import { callModel } from "./model";
import { type Company, lookupCompany, publishPost } from "./tools";

export type JobInput = { title: string; transcript: string };
type Extracted = { companies: string[]; actionItems: string[] };
type Stage =
	| "queued"
	| "extracting"
	| "enriching"
	| "summarizing"
	| "publishing"
	| "published"
	| "failed";
export type Progress = { stage: Stage; detail: string | null };

// docs:start retries
// Model calls get a long timeout and slow backoff, because rate limits clear in seconds to minutes.
const MODEL_STEP = {
	timeout: 120_000,
	retryOnTimeout: true,
	maxRetries: 6,
	retryBackoffBase: 2_000,
	retryBackoffMax: 60_000,
};
// docs:end retries

// One workflow per job, keyed by the job id.
export const aiJob = workflow(
	{
		state: {
			stage: "queued" as Stage,
			extracted: null as Extracted | null,
			companies: [] as Company[],
			summary: null as string | null,
			url: null as string | null,
		},
		queues: {
			start: queue<JobInput>(),
		},
		events: {
			progress: event<Progress>(),
		},
		run: async (ctx) => {
			const jobId = ctx.key[0];
			const { body: input } = await ctx.queue.next("wait-start", {
				names: ["start"],
			});

			// docs:start extract
			const extracted = await ctx.step({
				name: "extract",
				...MODEL_STEP,
				run: async (step) => {
					setStage(step, "extracting");
					const text = await callModel({
						system: "You read meeting transcripts and reply with JSON only.",
						prompt: `Extract the companies mentioned and the action items. Reply as {"companies": string[], "actionItems": string[]}.\n\n${input.transcript}`,
						json: true,
					});
					// Bad JSON throws, so the step asks the model again.
					const result = JSON.parse(text) as Extracted;
					step.state.extracted = result;
					return result;
				},
			});
			// docs:end extract

			// docs:start enrich
			// One step per company, so a failed lookup retries alone and finished lookups are not repeated.
			const companies: Company[] = [];
			for (const [index, name] of extracted.companies.entries()) {
				const company = await ctx.step({
					name: `enrich-${index}`,
					maxRetries: 5,
					run: async (step) => {
						setStage(step, "enriching", name);
						const found = await lookupCompany(name);
						step.state.companies.push(found);
						return found;
					},
				});
				companies.push(company);
			}
			// docs:end enrich

			// docs:start summarize
			const summary = await ctx.step({
				name: "summarize",
				...MODEL_STEP,
				run: async (step) => {
					setStage(step, "summarizing");
					const text = await callModel({
						system: "You write short meeting briefs for a sales team.",
						prompt: `Write a brief for "${input.title}".\n\nCRM data: ${JSON.stringify(companies)}\nAction items: ${extracted.actionItems.join("; ")}\n\nTranscript:\n${input.transcript}`,
					});
					step.state.summary = text;
					return text;
				},
			});
			// docs:end summarize

			// docs:start publish
			await ctx.step({
				name: "publish",
				maxRetries: 5,
				run: async (step) => {
					setStage(step, "publishing");
					const url = await publishPost({
						jobId,
						title: input.title,
						body: summary,
					});
					step.state.url = url;
					setStage(step, "published", url);
				},
			});
			// docs:end publish
		},
		actions: {
			getStatus: (c) => c.state,
		},
	},
	{
		// docs:start on-error
		// Tell the app about retries and failures. This hook only observes. It does not change the run.
		onError: (c, event) => {
			if (!("step" in event)) return;
			const { stepName, willRetry, retryDelay, error } = event.step;
			if (!willRetry) c.state.stage = "failed";
			const detail = willRetry
				? `${stepName} failed (${error.message}), retrying in ${retryDelay}ms`
				: `${stepName} failed: ${error.message}`;
			c.broadcast("progress", { stage: c.state.stage, detail });
		},
		// docs:end on-error
	},
);

// docs:start progress
// Saves the stage in state, so a client that connects later can read it, and pushes it to connected clients.
function setStage(
	step: WorkflowStepContextOf<typeof aiJob>,
	stage: Stage,
	detail: string | null = null,
) {
	step.state.stage = stage;
	step.broadcast("progress", { stage, detail });
}
// docs:end progress
