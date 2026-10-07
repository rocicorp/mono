import SQLite3Database from '@rocicorp/zero-sqlite3';
import type {Statement} from './db.ts';

let uses = new Map<string, number>();

/**
 * The table-valued function that the SQL of `IN` and `NOT IN` conditions reads
 * its values from (see `simpleConditionToSQL()` in `query-builder.ts`).
 */
const JSON_EACH = 'json_each';

/**
 * Counts a run of `stmt`, a read of `table`, against each index of the table
 * that SQLite's plan for it read. Loops that read the table itself (a scan,
 * or a lookup by rowid), or the values of an `IN` (from `json_each`), are not
 * counted, and an index that more than one loop of the plan reads (e.g. for
 * an `OR` of two of its ranges) is counted once.
 *
 * The plan is read after every run rather than once per statement: SQLite
 * plans a statement again when a new binding can change the best plan, as it
 * can with `sqlite_stat4` statistics or for `LIKE`.
 */
export function recordIndexUsage(stmt: Statement, table: string): void {
  let counted: string[] | undefined;
  // Without SQLITE_SCANSTAT_COMPLEX, scanstatus lists only the loops, each of
  // which has the name of the index or table it reads, so the first missing
  // name is the end of the list.
  for (let i = 0; ; i++) {
    const name = stmt.scanStatus(i, SQLite3Database.SQLITE_SCANSTAT_NAME, 0);
    if (name === undefined) {
      return;
    }
    if (
      name === table ||
      counted?.includes(name) ||
      (name === JSON_EACH && readsJsonEach(stmt, i))
    ) {
      continue;
    }
    (counted ??= []).push(name);
    uses.set(name, (uses.get(name) ?? 0) + 1);
  }
}

/**
 * Whether loop `i` of `stmt` reads the values of `json_each`, rather than an
 * index that happens to have the same name.
 */
function readsJsonEach(stmt: Statement, i: number): boolean {
  const explain = stmt.scanStatus(
    i,
    SQLite3Database.SQLITE_SCANSTAT_EXPLAIN,
    0,
  );
  return explain?.startsWith(`SCAN ${JSON_EACH} VIRTUAL TABLE`) ?? false;
}

/**
 * Returns the runs recorded by {@link recordIndexUsage} in this process since
 * the last call, by the name of the index they read.
 */
export function takeIndexUsage(): Map<string, number> {
  const taken = uses;
  uses = new Map();
  return taken;
}
