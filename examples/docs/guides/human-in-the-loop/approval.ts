import { event, queue, workflow } from "@rivet-dev/workflows";
import { approvalBlocks, postMessage } from "./slack";
import { runTool, type ToolCall } from "./tools";

// What is being asked, and who must answer. `requestedBy` and `approver` are Slack user or channel ids.
export type ApprovalRequest = ToolCall & {
	summary: string;
	requestedBy: string;
	approver: string;
};

export type Decision = { approved: boolean; decidedBy: string };

export type Outcome = {
	status: "approved" | "rejected" | "expired" | "failed";
	decidedBy: string | null;
	result: unknown;
	error: string | null;
};

// How long the approver has to answer before the request expires.
const DECISION_TIMEOUT_MS = 3 * 24 * 60 * 60 * 1000;

// One approval workflow per request, keyed by the request id.
export const approval = workflow({
	state: {
		request: null as ApprovalRequest | null,
		status: "new" as "new" | "pending" | Outcome["status"],
		outcome: null as Outcome | null,
	},
	queues: {
		request: queue<ApprovalRequest>(),
		decision: queue<Decision>(),
	},
	events: {
		finished: event<Outcome>(),
	},
	run: async (ctx) => {
		const requestId = ctx.key[0];

		// docs:start request
		const { body: request } = await ctx.queue.next("wait-request", {
			names: ["request"],
		});
		await ctx.step("record-request", async (step) => {
			step.state.request = request;
			step.state.status = "pending";
		});
		await ctx.step("notify-approver", async () => {
			const text = `Run \`${request.tool}\`? ${request.summary}`;
			await postMessage(
				request.approver,
				`Approval needed for request ${requestId}`,
				approvalBlocks(requestId, text),
			);
		});
		// docs:end request

		// docs:start wait
		// Wait for a decision message, or give up after the timeout. Both are durable.
		const [decision] = await ctx.queue.nextBatch("wait-decision", {
			names: ["decision"],
			timeout: DECISION_TIMEOUT_MS,
		});
		// docs:end wait

		// docs:start outcome
		// No decision means the timeout fired first.
		let outcome: Outcome = {
			status: decision ? "rejected" : "expired",
			decidedBy: decision?.body.decidedBy ?? null,
			result: null,
			error: null,
		};
		if (decision?.body.approved) {
			// The tool runs only here, after a person approved it.
			const run = await ctx.tryStep({
				name: "run-tool",
				maxRetries: 5,
				retryBackoffBase: 1000,
				run: async () => await runTool(request, requestId),
			});
			outcome = run.ok
				? { ...outcome, status: "approved", result: run.value }
				: { ...outcome, status: "failed", error: run.failure.error.message };
		}

		await ctx.step("record-outcome", async (step) => {
			step.state.status = outcome.status;
			step.state.outcome = outcome;
			step.broadcast("finished", outcome);
		});
		await ctx.step("notify-requester", async () => {
			await postMessage(
				request.requestedBy,
				`Your request to run \`${request.tool}\` ended as ${outcome.status}.`,
			);
		});
		// docs:end outcome
	},
	actions: {
		getStatus: (c) => c.state,
	},
});
