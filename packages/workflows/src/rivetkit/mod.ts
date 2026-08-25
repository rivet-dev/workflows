import {
	type Actions,
	type ActorConfigInput,
	type ActorDefinition,
	actor,
	defineRunHandler,
	type EventSchemaConfig,
	type QueueSchemaConfig,
	RivetError,
	type RunContext,
	type RunControl,
} from "rivetkit";
import { isActorAbortedError } from "rivetkit/errors";
import { stringifyError } from "rivetkit/utils";
import {
	CriticalError,
	EntryInProgressError,
	HistoryDivergedError,
	JoinError,
	RaceError,
	RollbackCheckpointError,
	RollbackError,
	type RunWorkflowOptions,
	replayWorkflowFromStep,
	runWorkflow,
	StepExhaustedError,
	type WorkflowErrorEvent,
} from "../index.js";
import { WorkflowContext } from "./context";
import { ActorWorkflowControlDriver, ActorWorkflowDriver } from "./driver";
import { createWorkflowInspectorAdapter } from "./inspector";

export type {
	TryBlockCatchKind,
	TryBlockConfig,
	TryBlockFailure,
	TryBlockResult,
	TryStepCatchKind,
	TryStepConfig,
	TryStepFailure,
	TryStepResult,
	WorkflowError,
	WorkflowErrorEvent,
} from "../index.js";
export { Loop } from "../index.js";
export {
	type WorkflowBranchConfig,
	type WorkflowBranchContextOf,
	WorkflowContext,
	type WorkflowContextOf,
	type WorkflowLoopConfig,
	type WorkflowLoopContextOf,
	type WorkflowStepConfig,
	WorkflowStepContext,
	type WorkflowStepContextOf,
	type WorkflowTryConfig,
	type WorkflowTryStepConfig,
} from "./context";

function shouldRethrowWorkflowError(error: unknown): boolean {
	if (
		error instanceof CriticalError ||
		error instanceof JoinError ||
		error instanceof RaceError ||
		error instanceof RollbackError ||
		error instanceof StepExhaustedError
	) {
		return false;
	}

	if (
		error instanceof EntryInProgressError ||
		error instanceof HistoryDivergedError ||
		error instanceof RollbackCheckpointError
	) {
		return true;
	}

	return true;
}

function workflowReplayInFlightError(): RivetError {
	return new RivetError(
		"actor",
		"workflow_in_flight",
		"Workflow replay is unavailable while the workflow is currently in flight.",
		{
			public: true,
			statusCode: 409,
		},
	);
}

function isWorkflowReplayBlockedByRunningEntry(error: unknown): boolean {
	return (
		error instanceof Error &&
		error.message ===
			"Cannot replay a workflow while a step is currently running"
	);
}

function isRunHandlerUnavailable(error: unknown): boolean {
	return (
		error instanceof RivetError &&
		error.group === "actor" &&
		error.code === "run_handler_unavailable"
	);
}

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
	? Omit<T, K>
	: never;

export type WorkflowActorConfig<
	TState = undefined,
	TConnParams = undefined,
	TConnState = undefined,
	TVars = undefined,
	TInput = undefined,
	TEvents extends EventSchemaConfig = Record<never, never>,
	TQueues extends QueueSchemaConfig = Record<never, never>,
	TActions extends Actions<
		TState,
		TConnParams,
		TConnState,
		TVars,
		TInput,
		undefined,
		TEvents,
		TQueues
	> = Record<never, never>,
> = DistributiveOmit<
	ActorConfigInput<
		TState,
		TConnParams,
		TConnState,
		TVars,
		TInput,
		undefined,
		TEvents,
		TQueues,
		TActions
	>,
	"run" | "db"
> & {
	run: (
		ctx: WorkflowContext<
			TState,
			TConnParams,
			TConnState,
			TVars,
			TInput,
			undefined,
			TEvents,
			TQueues
		>,
	) => Promise<unknown>;
	onError?: (
		ctx: RunContext<
			TState,
			TConnParams,
			TConnState,
			TVars,
			TInput,
			undefined,
			TEvents,
			TQueues
		>,
		event: WorkflowErrorEvent,
	) => void | Promise<void>;
};

export function workflow<
	TState = undefined,
	TConnParams = undefined,
	TConnState = undefined,
	TVars = undefined,
	TInput = undefined,
	TEvents extends EventSchemaConfig = Record<never, never>,
	TQueues extends QueueSchemaConfig = Record<never, never>,
	TActions extends Actions<
		TState,
		TConnParams,
		TConnState,
		TVars,
		TInput,
		undefined,
		TEvents,
		TQueues
	> = Record<never, never>,
>(
	config: WorkflowActorConfig<
		TState,
		TConnParams,
		TConnState,
		TVars,
		TInput,
		TEvents,
		TQueues,
		Actions<
			TState,
			TConnParams,
			TConnState,
			TVars,
			TInput,
			undefined,
			TEvents,
			TQueues
		>
	> & { actions?: TActions },
): ActorDefinition<
	TState,
	TConnParams,
	TConnState,
	TVars,
	TInput,
	undefined,
	TEvents,
	TQueues,
	TActions
> {
	if (Object.hasOwn(config, "db")) {
		throw new TypeError(
			"workflow() does not support a custom database provider",
		);
	}

	const { run: workflowRun, onError, ...actorConfig } = config;
	const workflowInspectors = new Map<
		string,
		ReturnType<typeof createWorkflowInspectorAdapter>
	>();
	const workflowControls = new Map<string, RunControl>();

	function getWorkflowInspector(actorId: string) {
		let workflowInspector = workflowInspectors.get(actorId);
		if (!workflowInspector) {
			workflowInspector = createWorkflowInspectorAdapter();
			workflowInspectors.set(actorId, workflowInspector);
		}
		return workflowInspector;
	}
	async function run(
		runCtx: RunContext<
			TState,
			TConnParams,
			TConnState,
			TVars,
			TInput,
			undefined,
			TEvents,
			TQueues
		>,
	): Promise<void> {
		const workflowInspector = getWorkflowInspector(runCtx.actorId);

		const driver = new ActorWorkflowDriver(runCtx);
		const controlDriver = new ActorWorkflowControlDriver(runCtx);
		workflowInspector.setReplayFromStep(async (entryId) => {
			const control = workflowControls.get(runCtx.actorId);
			if (!control) {
				throw new Error("Workflow Inspector control is not initialized");
			}
			try {
				return await control.run.withInactive(
					{ restartOnSuccess: true },
					async () => {
						const workflowState = await workflowInspector.adapter.getState();
						if (workflowState === "pending" || workflowState === "running") {
							throw workflowReplayInFlightError();
						}

						const snapshot = await replayWorkflowFromStep(
							runCtx.actorId,
							controlDriver,
							entryId,
							{ scheduleAlarm: false },
						);
						workflowInspector.update(snapshot);
						return workflowInspector.adapter.getHistory();
					},
				);
			} catch (error) {
				if (
					isWorkflowReplayBlockedByRunningEntry(error) ||
					isRunHandlerUnavailable(error)
				) {
					throw workflowReplayInFlightError();
				}
				throw error;
			}
		});

		const handle = runWorkflow(
			runCtx.actorId,
			async (ctx) => await workflowRun(new WorkflowContext(ctx, runCtx)),
			undefined,
			driver,
			{
				mode: "live",
				// The actor logger and the engine's pino logger are runtime
				// compatible but not structurally assignable.
				logger: runCtx.log as RunWorkflowOptions["logger"],
				onHistoryUpdated: workflowInspector.update,
				onError: onError
					? async (event) => await onError(runCtx, event)
					: undefined,
			},
		);
		workflowInspector.setGetState(async () => await handle.getState());

		const onAbort = () => {
			handle.evict();
		};
		if (runCtx.abortSignal.aborted) {
			onAbort();
		} else {
			runCtx.abortSignal.addEventListener("abort", onAbort, {
				once: true,
			});
		}

		try {
			await handle.result;
		} catch (error) {
			// `abortSignal.aborted` is delivered on a separate async hop and
			// races the rejection, so detect the sleep abort structurally too.
			if (runCtx.abortSignal.aborted || isActorAbortedError(error)) {
				return;
			}

			if (shouldRethrowWorkflowError(error)) {
				runCtx.log.error({
					msg: "workflow run failed",
					error: stringifyError(error),
				});
				throw error;
			}

			runCtx.log.warn({
				msg: "workflow failed and will sleep until woken",
				error: stringifyError(error),
			});
		} finally {
			runCtx.abortSignal.removeEventListener("abort", onAbort);
		}
	}

	const runHandler = defineRunHandler(run, {
		icon: "diagram-project",
		inspectorKind: "workflow",
		createInspector: ({ actorId, control }) => {
			workflowControls.set(actorId, control);
			const workflowInspector = getWorkflowInspector(actorId);
			return {
				inspector: {
					workflow: workflowInspector.adapter,
				},
				dispose: () => {
					if (workflowInspectors.get(actorId) === workflowInspector) {
						workflowControls.delete(actorId);
						workflowInspectors.delete(actorId);
					}
				},
			};
		},
	});

	return actor<
		TState,
		TConnParams,
		TConnState,
		TVars,
		TInput,
		undefined,
		TEvents,
		TQueues,
		TActions
	>({
		...actorConfig,
		run: runHandler,
	});
}
