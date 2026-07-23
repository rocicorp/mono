import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/schema/change-log-stream.ts';
import {
  reconstructWatermarkedChange,
  type ChangeLogEntry,
} from './change-log-codec.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

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
    };

type PlanRow = {
  readonly headWatermark: string;
  readonly minWatermark: string | null;
  readonly subscriberAhead: number;
  readonly boundaryExists: number;
};

type ChangeLogRow = ChangeLogEntry & {
  readonly pos: number;
  readonly tag: string | null;
};

type TransactionCursor = {
  watermark: string;
  nextPos: number;
  complete: boolean;
};

const CHANGE_TAGS: ReadonlySet<string> = new Set<ChangeTag>([
  'begin',
  'insert',
  'update',
  'delete',
  'truncate',
  'backfill',
  'create-table',
  'rename-table',
  'update-table-metadata',
  'add-column',
  'update-column',
  'drop-column',
  'drop-table',
  'create-index',
  'drop-index',
  'backfill-completed',
  'commit',
  'rollback',
]);

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

const PLAN_SQL = /*sql*/ `
  SELECT
    state."stateVersion" AS "headWatermark",
    bounds."minWatermark" AS "minWatermark",
    @fromWatermark > state."stateVersion" AS "subscriberAhead",
    coalesce((${SQLITE_CHANGE_LOG_BOUNDARY_SQL}), 0) AS "boundaryExists"
  FROM "_zero.replicationState" AS state
  CROSS JOIN (
    SELECT min("watermark") AS "minWatermark"
      FROM "${CHANGE_LOG_STREAM_TABLE}"
  ) AS bounds
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

/**
 * Reads the replica-local change log without retaining a SQLite snapshot while
 * a consumer processes a batch.
 *
 * The connection is read-only and therefore keeps the journal mode configured
 * by the replica writer (WAL or WAL2). Callers pin a head with {@link plan}
 * before passing it to {@link read}.
 */
export class SQLiteChangeLogReader implements Disposable {
  readonly #db: Database;
  readonly #plan: Statement;
  readonly #readBatch: Statement;
  #closed = false;

  constructor(lc: LogContext, replicaFile: string) {
    this.#db = new Database(
      lc.withContext('component', 'sqlite-change-log-reader'),
      replicaFile,
      {readonly: true},
    );
    this.#plan = this.#db.prepare(PLAN_SQL);
    this.#readBatch = this.#db.prepare(SQLITE_CHANGE_LOG_READ_BATCH_SQL);
  }

  plan(fromWatermark: string): CatchupPlan {
    this.#assertOpen();
    const row = this.#plan.get<PlanRow | undefined>({fromWatermark});
    assert(row !== undefined, 'replication state must be initialized');
    assert(
      row.minWatermark !== null,
      'SQLite change log must contain a catchup boundary',
    );

    if (row.subscriberAhead !== 0) {
      return {kind: 'ahead', headWatermark: row.headWatermark};
    }
    if (fromWatermark < row.minWatermark || row.boundaryExists === 0) {
      return {
        kind: 'too-old',
        minWatermark: row.minWatermark,
        headWatermark: row.headWatermark,
      };
    }
    return {
      kind: 'range',
      minWatermark: row.minWatermark,
      headWatermark: row.headWatermark,
    };
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

    let lastWatermark = fromWatermark;
    // The first query must be strictly after the requested transaction. Every
    // real stream position is far below this sentinel.
    let lastPos = Number.MAX_SAFE_INTEGER;
    let transaction: TransactionCursor | undefined;

    while (true) {
      this.#throwIfAborted(signal);
      const rows = this.#readBatch.all<ChangeLogRow>(
        lastWatermark,
        lastPos,
        throughWatermark,
        batchSize,
      );
      this.#throwIfAborted(signal);

      if (rows.length === 0) {
        break;
      }

      const changes: WatermarkedChange[] = [];
      for (const row of rows) {
        transaction = validateRow(row, transaction);
        changes.push(reconstructWatermarkedChange(validatedEntry(row)));
      }
      const last = rows.at(-1);
      assert(last !== undefined, 'non-empty batch must have a final row');
      lastWatermark = last.watermark;
      lastPos = last.pos;

      // all() has exhausted the SELECT and closed its implicit read
      // transaction. This yield therefore cannot pin the WAL while subscriber
      // flow control is pending.
      yield changes;
    }

    if (fromWatermark !== throughWatermark) {
      assert(
        transaction?.complete === true &&
          transaction.watermark === throughWatermark,
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

function validatedEntry(row: ChangeLogRow): ChangeLogEntry {
  assert(
    row.tag !== null && CHANGE_TAGS.has(row.tag),
    () => `invalid SQLite change log tag ${String(row.tag)}`,
  );
  return {watermark: row.watermark, tag: row.tag, change: row.change};
}

function validateRow(
  row: ChangeLogRow,
  transaction: TransactionCursor | undefined,
): TransactionCursor {
  assert(
    Number.isSafeInteger(row.pos) && row.pos >= 0,
    () => `invalid SQLite change log position ${row.pos}`,
  );

  if (transaction === undefined || row.watermark !== transaction.watermark) {
    assert(
      transaction === undefined || transaction.complete,
      () =>
        `incomplete SQLite change log transaction ${transaction?.watermark}`,
    );
    assert(
      row.pos === 0 && row.tag === 'begin',
      () =>
        `SQLite change log transaction ${row.watermark} does not start with begin at position 0`,
    );
    return {watermark: row.watermark, nextPos: 1, complete: false};
  }

  assert(
    !transaction.complete,
    () =>
      `SQLite change log transaction ${row.watermark} has rows after commit`,
  );
  assert(
    row.pos === transaction.nextPos,
    () =>
      `non-contiguous SQLite change log transaction ${row.watermark}: expected position ${transaction.nextPos}, got ${row.pos}`,
  );
  assert(
    row.tag !== 'begin',
    () => `SQLite change log transaction ${row.watermark} has multiple begins`,
  );
  assert(
    row.tag !== 'rollback',
    () => `SQLite change log transaction ${row.watermark} contains a rollback`,
  );
  return {
    watermark: transaction.watermark,
    nextPos: transaction.nextPos + 1,
    complete: row.tag === 'commit',
  };
}
