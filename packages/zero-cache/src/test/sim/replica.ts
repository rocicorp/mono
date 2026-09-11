import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {BACKFILLING_TABLE} from '../../services/replicator/schema/backfilling.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../services/replicator/schema/constants.ts';
import {getSubscriptionState} from '../../services/replicator/schema/replication-state.ts';
import {id} from '../../types/sql.ts';
import {majorVersionOf} from '../../types/state-version.ts';
import type {Violation} from './oracles.ts';
import type {SimPG, SimRow, SimState, SimValue} from './sim-pg.ts';

/** The replica's state version, which may be a backfill minor. */
export function replicaStateVersion(db: Database): string {
  return getSubscriptionState(new StatementRunner(db)).watermark;
}

/** Upstream values as the replica stores them. */
export function asStored(value: SimValue): unknown {
  return typeof value === 'boolean' ? (value ? 1 : 0) : value;
}

/** The columns the replica is backfilling, by table. */
export function backfillingColumns(db: Database): Map<string, Set<string>> {
  const columns = new Map<string, Set<string>>();
  for (const {table, column} of db
    .prepare(/*sql*/ `SELECT "table", "column" FROM "${BACKFILLING_TABLE}"`)
    .all<{table: string; column: string}>()) {
    columns.set(table, (columns.get(table) ?? new Set()).add(column));
  }
  return columns;
}

/**
 * The value that each column of `table` with a default gets from it. A row a
 * backfill inserts gets every column it leaves out from its default, as any
 * INSERT does.
 */
export function columnDefaults(
  db: Database,
  table: string,
): Map<string, unknown> {
  return new Map(
    db
      .prepare(/*sql*/ `SELECT name, dflt_value AS dflt FROM pragma_table_info(?)
          WHERE dflt_value IS NOT NULL`)
      .all<{name: string; dflt: string}>(table)
      .map(({name, dflt}) => [
        name,
        db.prepare(`SELECT ${dflt} AS v`).get<{v: unknown}>().v,
      ]),
  );
}

/**
 * Oracles 7 and 8 on a replica, as of its state version:
 *
 * - 7: the columns that are not being backfilled equal upstream's at the
 *   replica's major. A run's snapshot can be ahead of the replica, so a
 *   backfilling table may hold a phantom: a row from upstream's future whose
 *   other columns are still empty. Never a hole.
 * - 8: never stale: a backfilling column is empty or holds a value upstream
 *   has at the replica's major or later.
 *
 * @param onPhantom called for each phantom row allowed
 */
export function checkReplicaContent(
  db: Database,
  pg: SimPG,
  onPhantom: () => void = () => {},
): Violation[] {
  const violations: Violation[] = [];
  const version = majorVersionOf(replicaStateVersion(db));
  const {state} = pg.commitAt(version);
  const later = [pg.commitAt(version), ...pg.commitsAfter(version)].map(
    c => c.state,
  );
  const backfilling = backfillingColumns(db);
  const isBackfilling = (table: string, column: string) =>
    backfilling.get(table)?.has(column) ?? false;
  const laterRows = (oid: number | undefined, key: string): SimRow[] =>
    later.flatMap(s => {
      const row = [...s.tables.values()]
        .find(t => t.oid === oid)
        ?.rows.get(key);
      return row ? [row] : [];
    });

  // 7, with phantoms allowed in a backfilling table.
  for (const message of diffReplica(
    db,
    state,
    isBackfilling,
    10,
    (table, key, row) => {
      const upstream = state.tables.get(table);
      if (!upstream || !backfilling.has(table)) {
        return false;
      }
      const defaults = columnDefaults(db, table);
      const phantom =
        upstream.columns.every(
          c =>
            upstream.rowKey.includes(c.name) ||
            isBackfilling(table, c.name) ||
            row[c.name] === null ||
            row[c.name] === defaults.get(c.name),
        ) && laterRows(upstream.oid, key).length > 0;
      if (phantom) {
        onPhantom();
      }
      return phantom;
    },
  )) {
    violations.push({oracle: '7 (replica = upstream)', message});
  }

  // 8: never stale.
  for (const [table, columns] of backfilling) {
    const upstream = state.tables.get(table);
    if (!upstream) {
      continue;
    }
    const replicaColumns = new Set(
      db
        .prepare(/*sql*/ `SELECT name FROM pragma_table_info(?)`)
        .all<{name: string}>(table)
        .map(({name}) => name),
    );
    const cols = [...columns].filter(
      c => replicaColumns.has(c) && upstream.columns.some(u => u.name === c),
    );
    if (cols.length === 0) {
      continue;
    }
    const rows = db
      .prepare(
        `SELECT ${[...upstream.rowKey, ...cols].map(c => id(c)).join(', ')} FROM ${id(table)}`,
      )
      .all<Record<string, unknown>>();
    for (const row of rows) {
      const key = JSON.stringify(upstream.rowKey.map(c => row[c] ?? null));
      const candidates = laterRows(upstream.oid, key);
      for (const col of cols) {
        if (
          row[col] !== null &&
          !candidates.some(c => asStored(c[col] ?? null) === row[col])
        ) {
          violations.push({
            oracle: '8 (never stale)',
            message:
              `${table} row ${key}: ${col} is ${JSON.stringify(row[col])}, ` +
              `which upstream never holds at ${version} or later`,
          });
        }
      }
    }
  }
  return violations;
}

/**
 * How the replica's tables, columns, and rows differ from upstream as of
 * `state`. Empty when they agree. Columns for which `skip` returns true are
 * compared by name only, not by value: a backfilling column holds values that
 * no single upstream version does. A row that upstream does not have is a
 * difference unless `extraRow` accepts it.
 */
export function diffReplica(
  db: Database,
  state: SimState,
  skip: (table: string, column: string) => boolean = () => false,
  limit = 10,
  extraRow: (
    table: string,
    key: string,
    row: Record<string, unknown>,
  ) => boolean = () => false,
): string[] {
  const diffs: string[] = [];
  const replicaTables = new Set(
    db
      .prepare(/*sql*/ `
        SELECT name FROM sqlite_master
          WHERE type = 'table'
            AND substr(name, 1, 6) <> '_zero.'
            AND substr(name, 1, 7) <> 'sqlite_'`)
      .all<{name: string}>()
      .map(({name}) => name),
  );

  for (const table of state.tables.values()) {
    if (diffs.length >= limit) {
      break;
    }
    if (!replicaTables.has(table.name)) {
      diffs.push(`table ${table.name} is missing`);
      continue;
    }
    const replicaColumns = db
      .prepare(/*sql*/ `SELECT name FROM pragma_table_info(?)`)
      .all<{name: string}>(table.name)
      .map(({name}) => name)
      .filter(name => name !== ZERO_VERSION_COLUMN_NAME)
      .sort();
    const upstreamColumns = table.columns.map(c => c.name).sort();
    if (replicaColumns.join() !== upstreamColumns.join()) {
      diffs.push(
        `${table.name} has columns [${replicaColumns}], upstream [${upstreamColumns}]`,
      );
      continue;
    }

    const compared = table.columns.filter(c => !skip(table.name, c.name));
    const rows = db
      .prepare(
        `SELECT ${compared.map(c => id(c.name)).join(', ')} FROM ${id(table.name)}`,
      )
      .all<Record<string, unknown>>();
    // Keyed as SimPG keys its rows (`rowKeyString`).
    const actual = new Map(
      rows.map(row => [
        JSON.stringify(table.rowKey.map(col => row[col] ?? null)),
        row,
      ]),
    );
    for (const [key, upstream] of table.rows) {
      const row = actual.get(key);
      actual.delete(key);
      if (!row) {
        diffs.push(`${table.name} is missing row ${key}`);
        continue;
      }
      for (const {name} of compared) {
        const expected = asStored(upstream[name] ?? null);
        if (row[name] !== expected) {
          diffs.push(
            `${table.name} row ${key}: ${name} is ${JSON.stringify(row[name])}, ` +
              `upstream ${JSON.stringify(expected)}`,
          );
        }
      }
    }
    for (const [key, row] of actual) {
      if (!extraRow(table.name, key, row)) {
        diffs.push(`${table.name} has row ${key}, which upstream does not`);
      }
    }
  }

  for (const name of replicaTables) {
    if (!state.tables.has(name)) {
      diffs.push(`table ${name} exists on the replica but not upstream`);
    }
  }
  return diffs.slice(0, limit);
}
