type AnyFunction = (...args: any[]) => any;

const optionsByRun = new WeakMap<AnyFunction, unknown>();

export class RivetError extends Error {
	group: string;
	code: string;
	public?: boolean;
	statusCode?: number;

	constructor(
		group: string,
		code: string,
		message: string,
		options: { public?: boolean; statusCode?: number } = {},
	) {
		super(message);
		this.group = group;
		this.code = code;
		this.public = options.public;
		this.statusCode = options.statusCode;
	}
}

export function isActorAbortedError(error: unknown): boolean {
	return (
		error instanceof RivetError &&
		error.group === "actor" &&
		error.code === "aborted"
	);
}

export function defineRunHandler<TRun extends AnyFunction>(
	run: TRun,
	options: unknown,
): TRun {
	optionsByRun.set(run, options);
	return run;
}

export function getDefinedRunHandlerOptions(run: AnyFunction): any {
	return optionsByRun.get(run);
}
