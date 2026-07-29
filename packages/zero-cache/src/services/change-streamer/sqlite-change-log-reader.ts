import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/schema/change-log-stream.ts';
import {
  reconstructWatermarkedChange,
  type ChangeLogEntry,
} from './change-log-codec.ts';
import type {WatermarkedChange} from './change-streamer.ts';

export type CatchupPlan =
  | {
      readonly kind: 'range';
      readonly minWatermark: string;
      readonly headWatermark: string;
    }
  | {readonly kind: 'ahead'; readonly headWatermark: string}
  | {
      readonly kind: 'too-old';
      readonly minWatermark: string;
      readonly headWatermark: string;
    }
  // The log exists as a file but has no content to serve: the replicator has
  // not created or reconciled it yet. Slice 7E declines and falls back to PG
  // catchup; there is no production caller before then.
  | {readonly kind: 'not-ready'};

type PlanRow = {
  readonly headWatermark: string | null;
  readonly minWatermark: string | null;
  readonly boundaryExists: number;
};

type ChangeLogRow = ChangeLogEntry & {
  readonly pos: number;
};

type PreparedStatements = {
  readonly plan: Statement;
  readonly readBatch: Statement;
};

/**
 * Seeks to the final position so validating a boundary does not scan every row
 * of a large transaction. Exported so the focused query-plan test covers the
 * production subquery.
 */
export const SQLITE_CHANGE_LOG_BOUNDARY_SQL = /*sql*/ `
  SELECT
    "precommit" = @fromWatermark AND
      json_extract("change", '$.tag') = 'commit' AS "boundaryExists"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" = @fromWatermark
  ORDER BY "pos" DESC
  LIMIT 1
`;

/**
 * The log describes its own bounds; nothing here reads the replica.
 * `max("watermark")` is the head because a transaction's rows commit atomically,
 * so the log never ends mid-transaction.
 *
 * `min` and `max` are deliberately kept as *separate* scalar subqueries: SQLite
 * applies its single-aggregate index optimization only when the aggregate is
 * alone in its query, so `SELECT min(x), max(x) FROM t` degrades to a full scan
 * while two scalar subqueries are two index seeks. The query-plan test pins
 * this.
 *
 * Exported so that test can cover the production query.
 */
export const SQLITE_CHANGE_LOG_PLAN_SQL = /*sql*/ `
  SELECT
    (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "headWatermark",
    (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "minWatermark",
    coalesce((${SQLITE_CHANGE_LOG_BOUNDARY_SQL}), 0) AS "boundaryExists"
`;

/** Exported so the focused query-plan test covers the production query. */
export const SQLITE_CHANGE_LOG_READ_BATCH_SQL = /*sql*/ `
  SELECT
    "watermark",
    "pos",
    json_extract("change", '$.tag') AS "tag",
    "change"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE ("watermark", "pos") > (?, ?)
    AND "watermark" <= ?
  ORDER BY "watermark", "pos"
  LIMIT ?
`;

const TABLE_EXISTS_SQL = /*sql*/ `
  SELECT 1 FROM "sqlite_master" WHERE "type" = 'table' AND "name" = ?
`;

/**
 * Reads the change log without retaining a SQLite snapshot while a consumer
 * processes a batch.
 *
 * The log lives in its own database beside the replica
 * (`changeLogFileName(replicaFile)`), which this reader opens read-only; it
 * never opens the replica. The connection therefore keeps the journal mode
 * configured by the change-log writer. Callers pin a head with {@link plan}
 * before passing it to {@link read}.
 *
 * A change-streamer can start before the replicator has created the log, so the
 * stream table may not exist yet. Statements are prepared on first use rather
 * than in the constructor, and {@link plan} reports `not-ready` until they can
 * be; a reader constructed too early becomes usable once the writer catches up.
 */
export class SQLiteChangeLogReader implements Disposable {
  readonly #db: Database;
  #statements: PreparedStatements | undefined;
  #closed = false;

  constructor(lc: LogContext, changeLogFile: string) {
    this.#db = new Database(
      lc.withContext('component', 'sqlite-change-log-reader'),
      changeLogFile,
      {readonly: true},
    );
  }

  plan(fromWatermark: string): CatchupPlan {
    this.#assertOpen();
    const statements = this.#prepare();
    if (statements === undefined) {
      return {kind: 'not-ready'};
    }
    const row = statements.plan.get<PlanRow>({fromWatermark});
    if (row.headWatermark === null || row.minWatermark === null) {
      return {kind: 'not-ready'};
    }
    const {headWatermark, minWatermark} = row;

    if (fromWatermark > headWatermark) {
      return {kind: 'ahead', headWatermark};
    }
    if (fromWatermark < minWatermark || row.boundaryExists === 0) {
      return {kind: 'too-old', minWatermark, headWatermark};
    }
    return {kind: 'range', minWatermark, headWatermark};
  }

  async *read(
    fromWatermark: string,
    throughWatermark: string,
    batchSize: number,
    signal?: AbortSignal | undefined,
  ): AsyncIterable<readonly WatermarkedChange[]> {
    assert(
      Number.isSafeInteger(batchSize) && batchSize > 0,
      'SQLite change log batch size must be a positive safe integer',
    );
    this.#assertOpen();
    const statements = this.#prepare();
    assert(
      statements !== undefined,
      'SQLite change log must be ready before it is read',
    );

    let lastWatermark = fromWatermark;
    // The first query must be strictly after the requested transaction. Every
    // real stream position is far below this sentinel.
    let lastPos = Number.MAX_SAFE_INTEGER;
    let lastTag: string | undefined;

    while (true) {
      this.#throwIfAborted(signal);
      const rows = statements.readBatch.all<ChangeLogRow>(
        lastWatermark,
        lastPos,
        throughWatermark,
        batchSize,
      );
      this.#throwIfAborted(signal);

      if (rows.length === 0) {
        break;
      }

      const changes = rows.map(reconstructWatermarkedChange);
      const last = rows.at(-1);
      assert(last !== undefined, 'non-empty batch must have a final row');
      lastWatermark = last.watermark;
      lastPos = last.pos;
      lastTag = last.tag;

      // all() has exhausted the SELECT and closed its implicit read
      // transaction. This yield therefore cannot pin the WAL while subscriber
      // flow control is pending.
      yield changes;
    }

    // `throughWatermark` was pinned from `max("watermark")` of this same table,
    // so reaching it no longer depends on the log agreeing with the replica's
    // `_zero.replicationState`, which is what this assertion used to guard.
    // What remains is a concurrent purge of the head transaction, which the
    // purge policy (never past the latest durable transaction) does not do.
    if (fromWatermark !== throughWatermark) {
      assert(
        lastWatermark === throughWatermark && lastTag === 'commit',
        () =>
          `SQLite change log did not end at pinned head ${throughWatermark}`,
      );
    }
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#db.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  /**
   * Prepares the statements once the stream table exists, memoizing them.
   * Returns `undefined` while the log has not been created, which is a normal
   * startup state rather than an error.
   */
  #prepare(): PreparedStatements | undefined {
    if (this.#statements === undefined && this.#streamTableExists()) {
      this.#statements = {
        plan: this.#db.prepare(SQLITE_CHANGE_LOG_PLAN_SQL),
        readBatch: this.#db.prepare(SQLITE_CHANGE_LOG_READ_BATCH_SQL),
      };
    }
    return this.#statements;
  }

  #streamTableExists(): boolean {
    return (
      this.#db
        .prepare(TABLE_EXISTS_SQL)
        .get<{1: number} | undefined>(CHANGE_LOG_STREAM_TABLE) !== undefined
    );
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new AbortError('SQLite change log reader is closed');
    }
  }

  #throwIfAborted(signal: AbortSignal | undefined): void {
    this.#assertOpen();
    if (signal?.aborted) {
      throw new AbortError('SQLite change log read aborted');
    }
  }
}
