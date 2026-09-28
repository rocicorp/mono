import {readFile} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import type postgres from 'postgres';
import type {Row} from '../../../../packages/zero-protocol/src/data.ts';
import {sleep} from '../util.ts';
import type {Recorder} from './recorder.ts';

/**
 * A keyset-paged backfill that rewrites one column from `from` to `to`
 * across a set of tables, one page per transaction.
 *
 * The page is SELECTED on the cheap predicate (cursor + old value) and the
 * guards are evaluated in the UPDATE against exactly the selected keys:
 * putting the guards in the page query can make the planner give up the
 * index scan's early LIMIT exit. Rows the guards reject stay as they are and
 * the cursor steps past them.
 */
export type BackfillTableSpec = {
  /** Upstream table, schema-qualified, e.g. `catalog.work_titles`. */
  readonly table: string;
  /** Keyset column; unique among rows holding the old value. */
  readonly cursorColumn: string;
  /** SQL type the cursor is compared as. Defaults to `uuid`. */
  readonly cursorType?: string | undefined;
  /**
   * SQL predicate over alias `t`, true when renaming the row would collide
   * with an existing row. `{{from}}` and `{{to}}` become quoted literals.
   */
  readonly conflict?: string | undefined;
  /** The table's name as clients see it in pokes. Defaults to `table`. */
  readonly syncedTable?: string | undefined;
};

export type BackfillSpec = {
  readonly column: string;
  readonly from: string;
  readonly to: string;
  /** Extra SET assignments, e.g. `updated_at = now()`. */
  readonly set?: string | undefined;
  readonly tables: readonly BackfillTableSpec[];
};

export type ScheduleSegment = {
  /** Rows per second; 0 pauses the backfill. */
  readonly rowsPerSecond: number;
  readonly seconds: number;
};

export async function loadBackfillSpec(path: string): Promise<BackfillSpec> {
  const spec = JSON.parse(await readFile(path, 'utf8')) as BackfillSpec;
  if (!spec.column || spec.from === undefined || spec.to === undefined) {
    throw new Error(`${path}: a backfill spec needs column, from and to`);
  }
  if (!Array.isArray(spec.tables) || spec.tables.length === 0) {
    throw new Error(`${path}: a backfill spec needs at least one table`);
  }
  return spec;
}

/** Parses `rate:seconds,rate:seconds,...`, e.g. `0:600,24:1200,0:600`. */
export function parseSchedule(text: string): ScheduleSegment[] {
  return text
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(part => {
      const [rate, seconds] = part.split(':').map(Number);
      if (
        !Number.isFinite(rate) ||
        rate < 0 ||
        !Number.isFinite(seconds) ||
        seconds <= 0
      ) {
        throw new Error(
          `Invalid schedule segment "${part}"; expected rowsPerSecond:seconds`,
        );
      }
      return {rowsPerSecond: rate, seconds};
    });
}

export function scheduleSeconds(schedule: readonly ScheduleSegment[]): number {
  return schedule.reduce((total, s) => total + s.seconds, 0);
}

/** The segment in effect `elapsedMs` into the schedule, and when it ends. */
export function segmentAt(
  schedule: readonly ScheduleSegment[],
  elapsedMs: number,
): {readonly segment: ScheduleSegment; readonly endMs: number} | undefined {
  let endMs = 0;
  for (const segment of schedule) {
    endMs += segment.seconds * 1000;
    if (elapsedMs < endMs) {
      return {segment, endMs};
    }
  }
  return undefined;
}

export function sqlLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

export function quoteIdentifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** `catalog.work_titles` → `"catalog"."work_titles"`. */
export function quoteTable(name: string): string {
  return name.split('.').map(quoteIdentifier).join('.');
}

export function pageStatement(
  spec: Pick<BackfillSpec, 'column' | 'set'>,
  table: BackfillTableSpec,
  input: {
    readonly from: string;
    readonly to: string;
    readonly after: string | null;
    readonly pageSize: number;
    /** SQL predicate over `t` for rows whose other columns pass every CHECK. */
    readonly validText?: string | undefined;
  },
): string {
  const cursor = quoteIdentifier(table.cursorColumn);
  const column = quoteIdentifier(spec.column);
  const cursorType = table.cursorType ?? 'uuid';
  const from = sqlLiteral(input.from);
  const to = sqlLiteral(input.to);
  const conflict = (table.conflict ?? 'false')
    .replaceAll('{{from}}', from)
    .replaceAll('{{to}}', to);
  const set = spec.set ? `, ${spec.set}` : '';
  const after =
    input.after === null
      ? 'true'
      : `t.${cursor} > ${sqlLiteral(input.after)}::${cursorType}`;
  return `
    WITH page AS (
      SELECT t.${cursor} AS k
        FROM ${quoteTable(table.table)} t
       WHERE ${after}
         AND t.${column} = ${from}
       ORDER BY t.${cursor}
       LIMIT ${input.pageSize}
    ), updated AS (
      UPDATE ${quoteTable(table.table)} t
         SET ${column} = ${to}${set}
        FROM page p
       WHERE t.${cursor} = p.k
         AND t.${column} = ${from}
         AND NOT (${conflict})
         AND (${input.validText ?? 'true'})
      RETURNING t.${cursor}::text AS k
    )
    SELECT (SELECT count(*) FROM page)::int AS readrows,
           (SELECT count(*) FROM updated)::int AS writtenrows,
           (SELECT k FROM page ORDER BY k DESC LIMIT 1)::text AS lastkey,
           (SELECT coalesce(array_agg(k), '{}') FROM updated) AS keys`;
}

/**
 * The conjunction of a table's CHECK constraints as a predicate over `t`.
 * Postgres re-validates every CHECK on UPDATE, so one row whose stored text
 * predates a constraint would abort the whole page.
 */
export async function checkPredicate(
  sql: postgres.Sql,
  table: string,
): Promise<string> {
  const [schema, name] = table.includes('.')
    ? table.split('.', 2)
    : ['public', table];
  const rows = await sql<{predicate: string}[]>`
    SELECT coalesce(
             string_agg('(' || regexp_replace(
               regexp_replace(pg_get_constraintdef(con.oid), '^CHECK\\s*', ''),
               '\\s+NOT VALID$', '') || ')', ' AND '),
             'true') AS predicate
      FROM pg_constraint con
      JOIN pg_class rel ON rel.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = rel.relnamespace
     WHERE n.nspname = ${schema} AND rel.relname = ${name} AND con.contype = 'c'`;
  return rows[0]?.predicate ?? 'true';
}

/**
 * Times how long a renamed row takes to reach each client that holds it:
 * from the page's commit to the poke that carries the new value.
 */
export class HeldRowTracker {
  readonly #column: string;
  readonly #to: string;
  readonly #cursorColumns: Map<string, string>;
  readonly #committedAt = new Map<string, number>();
  readonly #now: () => number;
  readonly #ttlMs: number;
  readonly #onDelivered: (ms: number) => void;

  constructor(options: {
    readonly spec: BackfillSpec;
    readonly to: string;
    readonly now: () => number;
    readonly ttlMs: number;
    readonly onDelivered: (ms: number) => void;
  }) {
    this.#column = options.spec.column;
    this.#to = options.to;
    this.#cursorColumns = new Map(
      options.spec.tables.map(t => [t.syncedTable ?? t.table, t.cursorColumn]),
    );
    this.#now = options.now;
    this.#ttlMs = options.ttlMs;
    this.#onDelivered = options.onDelivered;
  }

  written(syncedTable: string, keys: readonly string[]): void {
    const now = this.#now();
    for (const key of keys) {
      this.#committedAt.set(`${syncedTable}\u0000${key}`, now);
    }
  }

  observe(tableName: string, row: Row): void {
    const cursorColumn = this.#cursorColumns.get(tableName);
    if (cursorColumn === undefined || row[this.#column] !== this.#to) {
      return;
    }
    const committedAt = this.#committedAt.get(
      `${tableName}\u0000${String(row[cursorColumn])}`,
    );
    if (committedAt !== undefined) {
      this.#onDelivered(this.#now() - committedAt);
    }
  }

  /** Forgets rows committed more than `ttlMs` ago. */
  expire(): void {
    const cutoff = this.#now() - this.#ttlMs;
    for (const [key, t] of this.#committedAt) {
      if (t < cutoff) {
        this.#committedAt.delete(key);
      }
    }
  }
}

export type BackfillDriverOptions = {
  readonly sql: postgres.Sql;
  readonly spec: BackfillSpec;
  readonly tables: readonly BackfillTableSpec[];
  readonly direction: 'forward' | 'reverse';
  readonly pageSize: number;
  readonly schedule: readonly ScheduleSegment[];
  readonly suppressTriggers: boolean;
  /** Skip rows whose other columns fail a CHECK, instead of failing pages. */
  readonly skipInvalidRows: boolean;
  readonly recorder: Recorder;
  readonly tracker: HeldRowTracker;
  readonly log: (message: string) => void;
};

export class BackfillDriver {
  readonly #o: BackfillDriverOptions;
  readonly #from: string;
  readonly #to: string;
  #stopped = false;
  #written = 0;

  constructor(options: BackfillDriverOptions) {
    this.#o = options;
    const {from, to} = options.spec;
    [this.#from, this.#to] =
      options.direction === 'forward' ? [from, to] : [to, from];
  }

  get to(): string {
    return this.#to;
  }

  get rowsWritten(): number {
    return this.#written;
  }

  /** Counts the rows each table still holds with the old value. */
  async remaining(): Promise<{table: string; rows: number | undefined}[]> {
    const {sql, spec, tables} = this.#o;
    const result = [];
    for (const table of tables) {
      try {
        const rows = await sql.begin(async tx => {
          await tx.unsafe(`SET LOCAL statement_timeout = '60s'`);
          return tx.unsafe<{n: number}[]>(
            `SELECT count(*)::int AS n FROM ${quoteTable(table.table)}
              WHERE ${quoteIdentifier(spec.column)} = ${sqlLiteral(this.#from)}`,
          );
        });
        result.push({table: table.table, rows: rows[0]?.n});
      } catch {
        result.push({table: table.table, rows: undefined});
      }
    }
    return result;
  }

  stop(): void {
    this.#stopped = true;
  }

  /** Follows the schedule from now until it ends or every table is done. */
  async run(): Promise<void> {
    const o = this.#o;
    const startedAt = performance.now();
    const validText = new Map<string, string>();
    if (o.skipInvalidRows) {
      for (const table of o.tables) {
        validText.set(table.table, await checkPredicate(o.sql, table.table));
      }
    }
    let tableIndex = 0;
    let after: string | null = null;
    let nextPageAt = startedAt;
    while (!this.#stopped && tableIndex < o.tables.length) {
      const now = performance.now();
      const current = segmentAt(o.schedule, now - startedAt);
      if (current === undefined) {
        break;
      }
      const {rowsPerSecond} = current.segment;
      if (rowsPerSecond === 0) {
        await sleep(Math.min(250, startedAt + current.endMs - now));
        nextPageAt = performance.now();
        continue;
      }
      if (now < nextPageAt) {
        await sleep(Math.min(250, nextPageAt - now));
        continue;
      }
      const table = o.tables[tableIndex];
      const statement = pageStatement(o.spec, table, {
        from: this.#from,
        to: this.#to,
        after,
        pageSize: o.pageSize,
        validText: validText.get(table.table),
      });
      const pageStart = performance.now();
      let page: {
        readrows: number;
        writtenrows: number;
        lastkey: string | null;
        keys: string[];
      };
      try {
        page = await o.sql.begin(async tx => {
          if (o.suppressTriggers) {
            await tx.unsafe('SET LOCAL session_replication_role = replica');
          }
          const [row] = await tx.unsafe<(typeof page)[]>(statement);
          return row;
        });
      } catch (e) {
        o.recorder.serverError('backfill', String(e));
        o.log(`backfill page on ${table.table} failed: ${String(e)}`);
        await sleep(1_000);
        continue;
      }
      const pageMs = performance.now() - pageStart;
      o.recorder.backfillPage(page.readrows, page.writtenrows, pageMs);
      o.tracker.written(table.syncedTable ?? table.table, page.keys);
      this.#written += page.writtenrows;
      if (page.readrows === 0 || page.lastkey === null) {
        o.log(`backfill: ${table.table} has no rows left to rename`);
        tableIndex++;
        after = null;
        continue;
      }
      after = page.lastkey;
      nextPageAt = pageStart + (page.readrows / rowsPerSecond) * 1000;
    }
  }
}
