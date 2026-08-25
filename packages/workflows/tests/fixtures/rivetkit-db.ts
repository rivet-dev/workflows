import { vi } from "vitest";

function compareBytes(a: Uint8Array, b: Uint8Array): number {
	for (let index = 0; index < Math.min(a.length, b.length); index++) {
		if (a[index] !== b[index]) return a[index] - b[index];
	}
	return a.length - b.length;
}

function keyOf(key: Uint8Array): string {
	return Buffer.from(key).toString("hex");
}

export function createTestDatabase() {
	const rows = new Map<string, { key: Uint8Array; value: Uint8Array }>();

	const execute = vi.fn(
		async (
			sql: string,
			...args: unknown[]
		): Promise<Record<string, unknown>[]> => {
			if (sql.startsWith("INSERT INTO _rivet_wf_kv")) {
				const [key, value] = args as [Uint8Array, Uint8Array];
				rows.set(keyOf(key), { key, value });
				return [];
			}

			if (sql === "SELECT value FROM _rivet_wf_kv WHERE key = ?") {
				const row = rows.get(keyOf(args[0] as Uint8Array));
				return row ? [{ value: row.value }] : [];
			}

			if (sql.startsWith("SELECT key, value FROM _rivet_wf_kv")) {
				const [start, end] = args as [Uint8Array, Uint8Array];
				return [...rows.values()]
					.filter(
						(row) =>
							compareBytes(row.key, start) >= 0 &&
							compareBytes(row.key, end) < 0,
					)
					.sort((a, b) => compareBytes(a.key, b.key));
			}

			if (sql === "DELETE FROM _rivet_wf_kv WHERE key = ?") {
				rows.delete(keyOf(args[0] as Uint8Array));
				return [];
			}

			if (sql === "DELETE FROM _rivet_wf_kv WHERE key >= ? AND key < ?") {
				const [start, end] = args as [Uint8Array, Uint8Array];
				for (const [mapKey, row] of rows) {
					if (
						compareBytes(row.key, start) >= 0 &&
						compareBytes(row.key, end) < 0
					) {
						rows.delete(mapKey);
					}
				}
				return [];
			}

			throw new Error(`Unexpected workflow SQL: ${sql}`);
		},
	);

	const db = {
		execute,
		transaction: vi.fn(
			async (
				callback: (tx: {
					execute: typeof execute;
				}) => unknown | Promise<unknown>,
				_options?: unknown,
			) => await callback(db),
		),
		close: vi.fn(async () => {}),
	};

	return { db, rows };
}
