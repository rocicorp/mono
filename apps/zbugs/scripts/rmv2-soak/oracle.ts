import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {
  listIndexes,
  listTables,
} from '../../../../packages/zero-cache/src/db/lite-tables.ts';
import type {LiteTableSpec} from '../../../../packages/zero-cache/src/db/specs.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../../../../packages/zero-cache/src/services/replicator/schema/constants.ts';
import {
  JSON_STRINGIFIED,
  liteRow,
} from '../../../../packages/zero-cache/src/types/lite.ts';
import type {RowValue} from '../../../../packages/zero-cache/src/types/row-key.ts';
import {Database} from '../../../../packages/zqlite/src/db.ts';
import {sleep} from './infra.ts';

/**
 * The correctness gate (plan section 7.1):
 *
 *   for every replica R and replicated table T, at a defined transaction
 *   bound,  pi(R.T) === liteRow(PG.T)  as multisets keyed by primary key
 *
 * `liteRow` is the replicator's own PG->SQLite value mapping, so both sides
 * are canonicalized identically; `pi` drops `_0_version` and the `_zero.*`
 * schema (`listTables` already excludes the latter).
 *
 * This is the only gate that survives the cutover: the dark comparator needs
 * a PG change log to compare against, and once PG is retired it has nothing
 * to say. It has two stated blind spots -- a bug *inside* `liteRow` is
 * invisible to it, and equality at a bound cannot see a transient wrong state
 * that heals.
 */

export type RowDiff = {
  readonly key: string;
  readonly column?: string | undefined;
  readonly pg?: string | undefined;
  readonly replica?: string | undefined;
};

export type TableDiff = {
  readonly table: string;
  /** The columns the multiset is keyed by, or `*` for whole-row keying. */
  readonly key: string;
  readonly pgRows: number;
  readonly replicaRows: number;
  readonly missingFromReplica: RowDiff[];
  readonly extraInReplica: RowDiff[];
  readonly mismatched: RowDiff[];
};

export type ReplicaComparison = {
  readonly node: string;
  readonly stateVersion: string;
  readonly replicaVersion: string;
  readonly tables: number;
  readonly rows: number;
  readonly diffs: TableDiff[];
  readonly ok: boolean;
  readonly error?: string | undefined;
};

export type OracleResult = {
  readonly label: string;
  readonly atMs: number;
  readonly quiesceMs: number;
  readonly comparisons: ReplicaComparison[];
  readonly ok: boolean;
  /** The section 7.3 bisection verdict. */
  readonly verdict: string;
};

const MAX_REPORTED_DIFFS = 10;

export type ReplicaHandle = {
  readonly node: string;
  readonly replicaFile: string;
};

function openReplica(lc: LogContext, file: string): Database {
  try {
    return new Database(lc, file, {readonly: true});
  } catch {
    // A WAL database can refuse a read-only connection when its `-shm` is
    // not usable; a read/write connection that never writes is equivalent
    // for our purposes.
    return new Database(lc, file);
  }
}

/** Canonical string for one column value, from either side. */
function canon(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'bigint') {
    return `i:${value}`;
  }
  if (typeof value === 'number') {
    // A PG `numeric` of 1.0 and a SQLite INTEGER 1 are the same value; the
    // integral check is what keeps type affinity from reading as divergence.
    return Number.isInteger(value) ? `i:${BigInt(value)}` : `f:${value}`;
  }
  if (typeof value === 'string') {
    return `s:${value}`;
  }
  if (typeof value === 'boolean') {
    return `i:${value ? 1n : 0n}`;
  }
  if (value instanceof Uint8Array) {
    return `b:${Buffer.from(value).toString('base64')}`;
  }
  return `j:${JSON.stringify(value)}`;
}

/**
 * The replica has no PRIMARY KEY declarations -- `mapPostgresToLite` drops
 * them and relies on the UNIQUE indexes, including the one upstream created
 * for its primary key. So the multiset key is derived from the unique indexes
 * rather than from `LiteTableSpec.primaryKey`, which is always undefined here.
 *
 * Candidates are tried shortest first; one is accepted only if it is
 * *observably* a key of the replica's rows -- no nulls and no duplicates.
 * When none is, the whole row becomes the key and the comparison degrades to
 * a plain multiset with counts, which is still exactly the stated gate, only
 * with less pointed diagnostics.
 */
type KeyedRows = {
  key: string;
  columns: readonly string[] | undefined;
  rows: Map<string, {row: Record<string, string>; count: number}>;
  total: number;
};

function keyOf(
  row: Record<string, string>,
  keyColumns: readonly string[] | undefined,
  allColumns: readonly string[],
): string {
  return (keyColumns ?? allColumns).map(col => row[col]).join(' ');
}

function keyRows(
  rows: readonly Record<string, string>[],
  columns: readonly string[],
  candidates: readonly (readonly string[])[],
): KeyedRows {
  for (const candidate of candidates) {
    const map = new Map<string, {row: Record<string, string>; count: number}>();
    let usable = true;
    for (const row of rows) {
      if (candidate.some(col => row[col] === 'null')) {
        usable = false;
        break;
      }
      const key = keyOf(row, candidate, columns);
      if (map.has(key)) {
        usable = false;
        break;
      }
      map.set(key, {row, count: 1});
    }
    if (usable) {
      return {
        key: candidate.join('+'),
        columns: candidate,
        rows: map,
        total: rows.length,
      };
    }
  }
  const map = new Map<string, {row: Record<string, string>; count: number}>();
  for (const row of rows) {
    const key = keyOf(row, undefined, columns);
    const existing = map.get(key);
    if (existing) {
      existing.count++;
    } else {
      map.set(key, {row, count: 1});
    }
  }
  return {key: '*', columns: undefined, rows: map, total: rows.length};
}

function pgTableName(liteName: string): {schema: string; table: string} {
  const dot = liteName.indexOf('.');
  return dot < 0
    ? {schema: 'public', table: liteName}
    : {schema: liteName.slice(0, dot), table: liteName.slice(dot + 1)};
}

function comparableColumns(spec: LiteTableSpec): string[] {
  return Object.keys(spec.columns).filter(c => c !== ZERO_VERSION_COLUMN_NAME);
}

function canonRow(
  row: Record<string, unknown>,
  columns: readonly string[],
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const column of columns) {
    out[column] = canon(row[column]);
  }
  return out;
}

/**
 * Establishes the transaction bound by quiescence: no writes in flight, one
 * sentinel transaction round-tripped through every replica, and every
 * replica's `stateVersion` equal.
 *
 * PG cannot be read at a historical LSN, so quiescence is what pins both
 * sides to the same transaction set without any LSN bookkeeping. The sentinel
 * is inserted and then deleted, so it leaves the database exactly as it found
 * it -- and its *absence* everywhere proves that every replica consumed a
 * transaction later than all of the traffic.
 */
export async function quiesce(
  lc: LogContext,
  sql: postgres.Sql,
  replicas: readonly ReplicaHandle[],
  fixture: {projectID: string; creatorID: string},
  sentinelID: string,
  timeoutMs = 180_000,
): Promise<{stateVersion: string; elapsedMs: number}> {
  const start = Date.now();
  const deadline = start + timeoutMs;

  await sql`
    INSERT INTO issue
      (id, title, open, "projectID", "creatorID", description, visibility)
    VALUES
      (${sentinelID}, 'rmv2 soak quiesce sentinel', true,
       ${fixture.projectID}, ${fixture.creatorID}, 'sentinel', 'public')`;
  await waitForSentinel(lc, replicas, sentinelID, true, deadline);

  await sql`DELETE FROM issue WHERE id = ${sentinelID}`;
  await waitForSentinel(lc, replicas, sentinelID, false, deadline);

  // Every replica applied the delete, so they are all at or past its
  // watermark and nothing further is being written. Converging on one
  // stateVersion is then a matter of the last in-flight notification.
  let versions: string[] = [];
  while (Date.now() < deadline) {
    versions = replicas.map(r => readStateVersion(lc, r.replicaFile));
    if (new Set(versions).size === 1) {
      return {stateVersion: versions[0], elapsedMs: Date.now() - start};
    }
    await sleep(200);
  }
  throw new Error(
    `replicas did not converge on a stateVersion: ${replicas
      .map((r, i) => `${r.node}=${versions[i]}`)
      .join(', ')}`,
  );
}

async function waitForSentinel(
  lc: LogContext,
  replicas: readonly ReplicaHandle[],
  sentinelID: string,
  present: boolean,
  deadline: number,
): Promise<void> {
  const pending = new Set(replicas.map(r => r.node));
  while (Date.now() < deadline) {
    for (const replica of replicas) {
      if (!pending.has(replica.node)) {
        continue;
      }
      if (sentinelIs(lc, replica.replicaFile, sentinelID, present)) {
        pending.delete(replica.node);
      }
    }
    if (pending.size === 0) {
      return;
    }
    await sleep(100);
  }
  throw new Error(
    `timed out waiting for the quiesce sentinel to be ${
      present ? 'present' : 'absent'
    } on: ${[...pending].join(', ')}`,
  );
}

/**
 * Whether `replicaFile` currently agrees with `want` about the sentinel.
 *
 * A replica that cannot be read -- missing, or mid-restore -- has not
 * observed anything, so it never agrees: reporting "absent" there would end
 * the wait on a replica that has not caught up at all.
 */
function sentinelIs(
  lc: LogContext,
  replicaFile: string,
  sentinelID: string,
  want: boolean,
): boolean {
  let db: Database | undefined;
  try {
    db = openReplica(lc, replicaFile);
    const row = db
      .prepare(`SELECT 1 AS found FROM "issue" WHERE id = ? LIMIT 1`)
      .get<{found: number} | undefined>(sentinelID);
    return (row !== undefined) === want;
  } catch {
    return false;
  } finally {
    db?.close();
  }
}

export function readStateVersion(lc: LogContext, replicaFile: string): string {
  let db: Database | undefined;
  try {
    db = openReplica(lc, replicaFile);
    const row = db
      .prepare(`SELECT stateVersion FROM "_zero.replicationState"`)
      .get<{stateVersion: string} | undefined>();
    return row?.stateVersion ?? '';
  } catch {
    return '';
  } finally {
    db?.close();
  }
}

export function readReplicaVersion(
  lc: LogContext,
  replicaFile: string,
): string {
  let db: Database | undefined;
  try {
    db = openReplica(lc, replicaFile);
    const row = db
      .prepare(`SELECT replicaVersion FROM "_zero.replicationConfig"`)
      .get<{replicaVersion: string} | undefined>();
    return row?.replicaVersion ?? '';
  } catch {
    return '';
  } finally {
    db?.close();
  }
}

/** Reads one PG table, canonicalized through `liteRow`. */
async function readPGTable(
  sql: postgres.Sql,
  spec: LiteTableSpec,
  columns: readonly string[],
): Promise<Record<string, string>[]> {
  const {schema, table} = pgTableName(spec.name);
  const select = columns.map(c => `"${c}"`).join(', ');
  const rows = await sql.unsafe<RowValue[]>(
    `SELECT ${select} FROM "${schema}"."${table}"`,
  );
  return rows.map(row => {
    const {row: lite} = liteRow(row, spec, JSON_STRINGIFIED);
    return canonRow(lite, columns);
  });
}

function diffTable(
  table: string,
  columns: readonly string[],
  pg: KeyedRows,
  replica: KeyedRows,
): TableDiff {
  const missingFromReplica: RowDiff[] = [];
  const extraInReplica: RowDiff[] = [];
  const mismatched: RowDiff[] = [];

  for (const [key, pgEntry] of pg.rows) {
    const replicaEntry = replica.rows.get(key);
    if (!replicaEntry) {
      if (missingFromReplica.length < MAX_REPORTED_DIFFS) {
        missingFromReplica.push({key});
      }
      continue;
    }
    if (replicaEntry.count !== pgEntry.count) {
      if (mismatched.length < MAX_REPORTED_DIFFS) {
        mismatched.push({
          key,
          column: '<multiplicity>',
          pg: String(pgEntry.count),
          replica: String(replicaEntry.count),
        });
      }
      continue;
    }
    for (const column of columns) {
      if (pgEntry.row[column] !== replicaEntry.row[column]) {
        if (mismatched.length < MAX_REPORTED_DIFFS) {
          mismatched.push({
            key,
            column,
            pg: pgEntry.row[column],
            replica: replicaEntry.row[column],
          });
        }
        break;
      }
    }
  }
  for (const key of replica.rows.keys()) {
    if (!pg.rows.has(key) && extraInReplica.length < MAX_REPORTED_DIFFS) {
      extraInReplica.push({key});
    }
  }

  return {
    table,
    key: pg.key === replica.key ? pg.key : `${replica.key}/${pg.key}`,
    pgRows: pg.total,
    replicaRows: replica.total,
    missingFromReplica,
    extraInReplica,
    mismatched,
  };
}

async function compareReplica(
  lc: LogContext,
  sql: postgres.Sql,
  replica: ReplicaHandle,
): Promise<ReplicaComparison> {
  let db: Database | undefined;
  let tables = 0;
  let rows = 0;
  const diffs: TableDiff[] = [];
  try {
    db = openReplica(lc, replica.replicaFile);
    const specs = listTables(db);
    const uniqueKeys = new Map<string, string[][]>();
    for (const index of listIndexes(db)) {
      if (index.unique) {
        const keys = uniqueKeys.get(index.tableName) ?? [];
        keys.push(Object.keys(index.columns));
        uniqueKeys.set(index.tableName, keys);
      }
    }
    for (const spec of specs) {
      const columns = comparableColumns(spec);
      const candidates = (uniqueKeys.get(spec.name) ?? [])
        .filter(key => key.every(col => columns.includes(col)))
        .sort(
          (a, b) => a.length - b.length || a.join().localeCompare(b.join()),
        );
      const select = columns.map(c => `"${c}"`).join(', ');
      const replicaRows = db
        .prepare(`SELECT ${select} FROM "${spec.name}"`)
        .all<Record<string, unknown>>()
        .map(row => canonRow(row, columns));
      const pgRows = await readPGTable(sql, spec, columns);
      tables++;
      rows += replicaRows.length;
      const keyedReplica = keyRows(replicaRows, columns, candidates);
      const keyedPG = keyRows(pgRows, columns, candidates);
      const diff = diffTable(spec.name, columns, keyedPG, keyedReplica);
      if (
        diff.missingFromReplica.length > 0 ||
        diff.extraInReplica.length > 0 ||
        diff.mismatched.length > 0 ||
        diff.pgRows !== diff.replicaRows
      ) {
        diffs.push(diff);
      }
    }
  } catch (e) {
    return {
      node: replica.node,
      stateVersion: '',
      replicaVersion: '',
      tables,
      rows,
      diffs,
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    };
  } finally {
    db?.close();
  }

  return {
    node: replica.node,
    stateVersion: readStateVersion(lc, replica.replicaFile),
    replicaVersion: readReplicaVersion(lc, replica.replicaFile),
    tables,
    rows,
    diffs,
    ok: diffs.length === 0,
  };
}

/**
 * Section 7.3: comparing the replication-manager's replica as well as each
 * view-syncer's separates two failure families for free.
 */
function bisect(comparisons: readonly ReplicaComparison[]): string {
  const rm = comparisons.find(c => c.node === 'rm');
  const followers = comparisons.filter(c => c.node !== 'rm');
  const rmOK = rm?.ok ?? true;
  const followersOK = followers.every(c => c.ok);
  if (rmOK && followersOK) {
    return 'clean';
  }
  if (rmOK && !followersOK) {
    return 'the change-log / catchup path -- this harness is pointed at it';
  }
  if (!rmOK && !followersOK) {
    return 'replicator or initial-sync; the change log is downstream and likely innocent';
  }
  return 'contradiction -- suspect the quiescence detection before the code';
}

export async function runOracle(
  lc: LogContext,
  sql: postgres.Sql,
  replicas: readonly ReplicaHandle[],
  fixture: {projectID: string; creatorID: string},
  label: string,
  sentinelID: string,
): Promise<OracleResult> {
  const {elapsedMs} = await quiesce(lc, sql, replicas, fixture, sentinelID);
  const comparisons: ReplicaComparison[] = [];
  for (const replica of replicas) {
    comparisons.push(await compareReplica(lc, sql, replica));
  }
  return {
    label,
    atMs: Date.now(),
    quiesceMs: elapsedMs,
    comparisons,
    ok: comparisons.every(c => c.ok),
    verdict: bisect(comparisons),
  };
}
