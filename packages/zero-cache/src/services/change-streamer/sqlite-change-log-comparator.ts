import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {h32} from '../../../../shared/src/hash.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import * as Mode from '../../db/mode-enum.ts';
import {runTx} from '../../db/run-transaction.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {PostgresDB, PostgresTransaction} from '../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../types/shards.ts';
import {CHANGE_LOG_STREAM_TABLE} from '../replicator/schema/change-log-stream.ts';
import {reconstructWatermarkedChange} from './change-log-codec.ts';

export const SQLITE_CHANGE_LOG_SCHEMA_VERSION = 14;

export type ChangeLogComparisonBounds = {
  readonly replicaVersion: string;
  readonly headWatermark: string;
  readonly minWatermark: string | null;
  readonly maxWatermark: string | null;
  readonly schemaVersion?: number | undefined;
};

export type ChangeLogComparisonRow = {
  readonly watermark: string;
  readonly pos: number;
  readonly tag: string;
  readonly json: string;
};

export type ChangeLogRangeInspection = {
  readonly bounds: ChangeLogComparisonBounds;
  readonly status: 'complete' | 'incomplete' | 'outside-retention';
};

/**
 * A read view of one change-log store. Postgres implementations pin a
 * REPEATABLE READ snapshot. The SQLite implementation deliberately does not:
 * every method and read batch uses a short implicit snapshot so comparison
 * cannot pin the replica WAL while another process applies or purges changes.
 */
export interface ChangeLogComparisonReader {
  bounds(): Promise<ChangeLogComparisonBounds>;
  inspect(watermark: string): Promise<ChangeLogRangeInspection>;
  read(
    watermark: string,
    batchSize: number,
    signal?: AbortSignal | undefined,
  ): AsyncIterable<readonly ChangeLogComparisonRow[]>;
}

export interface ChangeLogComparisonSource {
  withRead<T>(
    read: (reader: ChangeLogComparisonReader) => Promise<T>,
  ): Promise<T>;
  inspectCurrent(watermark: string): Promise<ChangeLogRangeInspection>;
  close(): void;
}

export type SQLiteChangeLogMismatchReason =
  | 'missing-pg-row'
  | 'missing-sqlite-row'
  | 'tag-mismatch'
  | 'byte-mismatch'
  | 'bound-mismatch';

export type SQLiteChangeLogComparisonReason =
  | 'match'
  | SQLiteChangeLogMismatchReason
  | 'schema-version'
  | 'replica-version'
  | 'warming-up'
  | 'head-skew'
  | 'bounds-changed'
  | 'reader-error';

export type ChangeLogComparisonRowSummary = {
  readonly watermark: string;
  readonly pos: number;
  readonly tag: string;
  readonly jsonBytes: number;
};

export type SQLiteChangeLogComparisonResult = {
  readonly outcome: 'match' | 'divergence' | 'inconclusive' | 'ineligible';
  readonly reason: SQLiteChangeLogComparisonReason;
  readonly targetWatermark: string;
  readonly pgHead?: string | undefined;
  readonly sqliteHead?: string | undefined;
  readonly comparedRows?: number | undefined;
  readonly rowIndex?: number | undefined;
  readonly pgRow?: ChangeLogComparisonRowSummary | undefined;
  readonly sqliteRow?: ChangeLogComparisonRowSummary | undefined;
  readonly retry: boolean;
  readonly errorName?: string | undefined;
};

export type SQLiteChangeLogComparatorOptions = {
  readonly replicaVersion: string;
  readonly shard: ShardID;
  readonly retentionMs: number;
  readonly batchSize: number;
  readonly samplePercent: number;
  /**
   * An explicitly recorded boundary after which the local writer is known to
   * have been enabled. Production records it at the first compare-mode commit;
   * tests and future persistent rollout state can supply an earlier boundary.
   */
  readonly warmupStartedAtMs?: number | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
  readonly onResult?:
    | ((result: SQLiteChangeLogComparisonResult) => void)
    | undefined;
};

const DEFAULT_RETRY_DELAY_MS = 1000;
const SAMPLE_BUCKETS = 10_000;

/** Stable selection for a replica/shard/committed-watermark range. */
export function isSQLiteChangeLogSampled(
  shard: ShardID,
  replicaVersion: string,
  watermark: string,
  samplePercent: number,
): boolean {
  assertPercent(samplePercent);
  if (samplePercent === 0) {
    return false;
  }
  if (samplePercent === 100) {
    return true;
  }
  const key = `${shard.appID}\0${shard.shardNum}\0${replicaVersion}\0${watermark}`;
  return h32(key) % SAMPLE_BUCKETS < samplePercent * 100;
}

/**
 * Schedules and compares sampled complete transactions from the authoritative
 * Postgres change log against the replica-local SQLite stream log.
 *
 * At most one target and one coalesced successor are retained. A retry always
 * uses the same watermark, while newer sampled commits replace only the queued
 * successor. Comparison failures never participate in source ACKs or stop
 * replication.
 */
export class SQLiteChangeLogComparator {
  readonly #lc: LogContext;
  readonly #pg: ChangeLogComparisonSource;
  readonly #sqlite: ChangeLogComparisonSource;
  readonly #replicaVersion: string;
  readonly #shard: ShardID;
  readonly #retentionMs: number;
  readonly #batchSize: number;
  readonly #samplePercent: number;
  readonly #retryDelayMs: number;
  readonly #now: () => number;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #onResult:
    | ((result: SQLiteChangeLogComparisonResult) => void)
    | undefined;
  readonly #results = getOrCreateCounter(
    'replication',
    'sqlite_change_log.compare_result',
    'Results of sampled Postgres to SQLite change-log comparisons.',
  );
  readonly #duration = getOrCreateLatencyHistogram(
    'replication',
    'sqlite_change_log.compare_duration',
    'Time spent comparing a sampled Postgres and SQLite change-log range.',
  );
  readonly #abort = new AbortController();

  #warmupStartedAtMs: number | undefined;
  #target: string | undefined;
  #nextTarget: string | undefined;
  #lastFinishedTarget: string | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inFlight: Promise<number | undefined> | undefined;
  #closed = false;

  constructor(
    lc: LogContext,
    pg: ChangeLogComparisonSource,
    sqlite: ChangeLogComparisonSource,
    opts: SQLiteChangeLogComparatorOptions,
  ) {
    assertPositiveSafeInteger(opts.retentionMs, 'retention');
    assertPositiveSafeInteger(opts.batchSize, 'read batch size');
    assertPercent(opts.samplePercent);
    const retryDelayMs = opts.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    assertPositiveSafeInteger(retryDelayMs, 'retry delay');

    this.#lc = lc.withContext('component', 'sqlite-change-log-comparator');
    this.#pg = pg;
    this.#sqlite = sqlite;
    this.#replicaVersion = opts.replicaVersion;
    this.#shard = opts.shard;
    this.#retentionMs = opts.retentionMs;
    this.#batchSize = opts.batchSize;
    this.#samplePercent = opts.samplePercent;
    this.#retryDelayMs = retryDelayMs;
    this.#now = opts.now ?? Date.now;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;
    this.#onResult = opts.onResult;
    this.#warmupStartedAtMs = opts.warmupStartedAtMs;
    this.#lc.info?.('SQLite change-log dark comparison enabled', {
      sqliteChangeLogComparison: {
        samplePercent: this.#samplePercent,
        retentionMs: this.#retentionMs,
        batchSize: this.#batchSize,
      },
    });
  }

  schedule(watermark: string): void {
    if (this.#closed) {
      return;
    }
    this.#warmupStartedAtMs ??= this.#now();
    if (
      !isSQLiteChangeLogSampled(
        this.#shard,
        this.#replicaVersion,
        watermark,
        this.#samplePercent,
      )
    ) {
      return;
    }
    if (
      watermark <= (this.#lastFinishedTarget ?? '') ||
      watermark === this.#target ||
      watermark <= (this.#nextTarget ?? '')
    ) {
      return;
    }
    if (this.#target === undefined) {
      this.#target = watermark;
    } else {
      this.#nextTarget = watermark;
    }
    this.#scheduleDrain(0);
  }

  /** Runs one explicit range comparison, regardless of the sample selector. */
  async compareWatermark(
    targetWatermark: string,
    signal?: AbortSignal | undefined,
  ): Promise<SQLiteChangeLogComparisonResult> {
    const start = performance.now();
    this.#warmupStartedAtMs ??= this.#now();
    let result: SQLiteChangeLogComparisonResult;
    try {
      throwIfAborted(signal);
      const preliminary = await this.#pg.withRead(pg =>
        this.#sqlite.withRead(sqlite =>
          this.#comparePinned(pg, sqlite, targetWatermark, signal),
        ),
      );
      result =
        preliminary.outcome === 'divergence'
          ? await this.#confirmDivergence(preliminary)
          : preliminary;
    } catch (error) {
      if (signal?.aborted) {
        throw new AbortError('SQLite change-log comparison aborted');
      }
      result = readerError(targetWatermark, error);
    }
    this.#recordResult(result, performance.now() - start);
    return result;
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#abort.abort();
    if (this.#timer !== undefined) {
      this.#clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    await this.#inFlight;
    this.#pg.close();
    this.#sqlite.close();
  }

  async #comparePinned(
    pg: ChangeLogComparisonReader,
    sqlite: ChangeLogComparisonReader,
    targetWatermark: string,
    signal: AbortSignal | undefined,
  ): Promise<SQLiteChangeLogComparisonResult> {
    const [pgBounds, sqliteBounds] = await Promise.all([
      pg.bounds(),
      sqlite.bounds(),
    ]);
    const heads = {
      pgHead: pgBounds.headWatermark,
      sqliteHead: sqliteBounds.headWatermark,
    };

    if ((sqliteBounds.schemaVersion ?? 0) < SQLITE_CHANGE_LOG_SCHEMA_VERSION) {
      return {
        outcome: 'ineligible',
        reason: 'schema-version',
        targetWatermark,
        ...heads,
        retry: false,
      };
    }
    if (
      pgBounds.replicaVersion !== this.#replicaVersion ||
      sqliteBounds.replicaVersion !== this.#replicaVersion
    ) {
      return {
        outcome: 'ineligible',
        reason: 'replica-version',
        targetWatermark,
        ...heads,
        retry: false,
      };
    }
    const warmupStartedAtMs = this.#warmupStartedAtMs;
    assert(
      warmupStartedAtMs !== undefined,
      'SQLite change-log warm-up boundary must be recorded',
    );
    if (this.#now() - warmupStartedAtMs < this.#retentionMs) {
      return {
        outcome: 'ineligible',
        reason: 'warming-up',
        targetWatermark,
        ...heads,
        retry: true,
      };
    }

    const commonHead =
      pgBounds.headWatermark < sqliteBounds.headWatermark
        ? pgBounds.headWatermark
        : sqliteBounds.headWatermark;
    if (targetWatermark > commonHead) {
      return {
        outcome: 'inconclusive',
        reason: 'head-skew',
        targetWatermark,
        ...heads,
        retry: true,
      };
    }

    const [pgRange, sqliteRange] = await Promise.all([
      pg.inspect(targetWatermark),
      sqlite.inspect(targetWatermark),
    ]);
    if (
      pgRange.status === 'outside-retention' ||
      sqliteRange.status === 'outside-retention'
    ) {
      return {
        outcome: 'inconclusive',
        reason: 'bounds-changed',
        targetWatermark,
        ...heads,
        retry: false,
      };
    }
    if (pgRange.status !== 'complete' || sqliteRange.status !== 'complete') {
      return {
        outcome: 'divergence',
        reason: 'bound-mismatch',
        targetWatermark,
        ...heads,
        retry: false,
      };
    }

    const compared = await compareRows(
      pg.read(targetWatermark, this.#batchSize, signal),
      sqlite.read(targetWatermark, this.#batchSize, signal),
    );
    if (compared.difference === undefined) {
      return {
        outcome: 'match',
        reason: 'match',
        targetWatermark,
        ...heads,
        comparedRows: compared.comparedRows,
        retry: false,
      };
    }
    return {
      outcome: 'divergence',
      reason: compared.difference.reason,
      targetWatermark,
      ...heads,
      comparedRows: compared.comparedRows,
      rowIndex: compared.difference.rowIndex,
      pgRow: summarize(compared.difference.pgRow),
      sqliteRow: summarize(compared.difference.sqliteRow),
      retry: false,
    };
  }

  async #confirmDivergence(
    result: SQLiteChangeLogComparisonResult,
  ): Promise<SQLiteChangeLogComparisonResult> {
    try {
      const [pgCurrent, sqliteCurrent] = await Promise.all([
        this.#pg.inspectCurrent(result.targetWatermark),
        this.#sqlite.inspectCurrent(result.targetWatermark),
      ]);
      if (
        pgCurrent.bounds.replicaVersion !== this.#replicaVersion ||
        sqliteCurrent.bounds.replicaVersion !== this.#replicaVersion ||
        pgCurrent.status === 'outside-retention' ||
        sqliteCurrent.status === 'outside-retention' ||
        (result.reason === 'bound-mismatch' &&
          pgCurrent.status === 'complete' &&
          sqliteCurrent.status === 'complete')
      ) {
        return {
          outcome: 'inconclusive',
          reason: 'bounds-changed',
          targetWatermark: result.targetWatermark,
          pgHead: pgCurrent.bounds.headWatermark,
          sqliteHead: sqliteCurrent.bounds.headWatermark,
          retry: false,
        };
      }
      return result;
    } catch (error) {
      return readerError(result.targetWatermark, error);
    }
  }

  #scheduleDrain(delayMs: number): void {
    if (
      this.#closed ||
      this.#timer !== undefined ||
      this.#inFlight !== undefined ||
      this.#target === undefined
    ) {
      return;
    }
    this.#timer = this.#setTimeout(() => {
      this.#timer = undefined;
      const running = this.#drain();
      this.#inFlight = running;
      void running.then(
        nextDelayMs => {
          this.#inFlight = undefined;
          if (nextDelayMs !== undefined) {
            this.#scheduleDrain(nextDelayMs);
          }
        },
        error => {
          this.#inFlight = undefined;
          if (!this.#closed) {
            const result = readerError(this.#target ?? '', error);
            this.#recordResult(result, 0);
            this.#scheduleDrain(this.#retryDelayMs);
          }
        },
      );
    }, delayMs);
  }

  async #drain(): Promise<number | undefined> {
    const target = this.#target;
    if (this.#closed || target === undefined) {
      return undefined;
    }
    let result: SQLiteChangeLogComparisonResult;
    try {
      result = await this.compareWatermark(target, this.#abort.signal);
    } catch (error) {
      if (this.#closed) {
        return undefined;
      }
      result = readerError(target, error);
      this.#recordResult(result, 0);
    }

    if (result.retry) {
      return this.#retryDelayMs;
    }
    this.#lastFinishedTarget = target;
    this.#target = this.#nextTarget;
    this.#nextTarget = undefined;
    return this.#target === undefined ? undefined : 0;
  }

  #recordResult(
    result: SQLiteChangeLogComparisonResult,
    durationMs: number,
  ): void {
    const attributes = {result: result.outcome, reason: result.reason};
    this.#results.add(1, attributes);
    this.#duration.recordMs(durationMs, attributes);
    try {
      this.#onResult?.(result);
    } catch (error) {
      this.#lc.warn?.('SQLite change-log comparison observer failed', {
        errorName: error instanceof Error ? error.name : typeof error,
      });
    }

    const diagnostic = {
      reason: result.reason,
      replicaVersion: this.#replicaVersion,
      targetWatermark: result.targetWatermark,
      range: {
        fromWatermark: result.targetWatermark,
        throughWatermark: result.targetWatermark,
      },
      pgHead: result.pgHead,
      sqliteHead: result.sqliteHead,
      comparedRows: result.comparedRows,
      rowIndex: result.rowIndex,
      pgRow: result.pgRow,
      sqliteRow: result.sqliteRow,
      errorName: result.errorName,
    };
    switch (result.outcome) {
      case 'divergence':
        this.#lc.error?.('SQLite change-log comparison divergence', {
          sqliteChangeLogComparison: diagnostic,
        });
        break;
      case 'match':
        this.#lc.debug?.('SQLite change-log comparison matched', {
          sqliteChangeLogComparison: diagnostic,
        });
        break;
      case 'inconclusive':
        this.#lc[result.reason === 'reader-error' ? 'warn' : 'debug']?.(
          'SQLite change-log comparison inconclusive',
          {
            sqliteChangeLogComparison: diagnostic,
          },
        );
        break;
      case 'ineligible':
        this.#lc.debug?.('SQLite change-log comparison ineligible', {
          sqliteChangeLogComparison: diagnostic,
        });
        break;
    }
  }
}

export function createSQLiteChangeLogComparator(
  lc: LogContext,
  changeDB: PostgresDB,
  replicaFile: string,
  opts: SQLiteChangeLogComparatorOptions,
): SQLiteChangeLogComparator {
  return new SQLiteChangeLogComparator(
    lc,
    new PostgresChangeLogComparisonSource(changeDB, opts.shard),
    new SQLiteChangeLogComparisonSource(lc, replicaFile),
    opts,
  );
}

type RawBoundary = {
  readonly firstPos: string | number | null;
  readonly firstChange: string | null;
  readonly lastChange: string | null;
  readonly lastPrecommit: string | null;
};

type RawPostgresRow = {
  readonly watermark: string;
  readonly pos: string;
  readonly change: string;
};

type RawSQLiteRow = {
  readonly watermark: string;
  readonly pos: number;
  readonly change: string;
};

class PostgresChangeLogComparisonSource implements ChangeLogComparisonSource {
  readonly #db: PostgresDB;
  readonly #schema: string;

  constructor(db: PostgresDB, shard: ShardID) {
    this.#db = db;
    this.#schema = cdcSchema(shard);
  }

  withRead<T>(
    read: (reader: ChangeLogComparisonReader) => Promise<T>,
  ): Promise<T> {
    return runTx(
      this.#db,
      async tx => ({
        value: await read(
          new PostgresChangeLogComparisonReader(tx, this.#schema),
        ),
      }),
      {mode: Mode.READONLY},
    ).then(({value}) => value);
  }

  inspectCurrent(watermark: string): Promise<ChangeLogRangeInspection> {
    return this.withRead(reader => reader.inspect(watermark));
  }

  close(): void {}
}

class PostgresChangeLogComparisonReader implements ChangeLogComparisonReader {
  readonly #tx: PostgresTransaction;
  readonly #changeLog: string;
  readonly #replicationState: string;
  readonly #replicationConfig: string;

  constructor(tx: PostgresTransaction, schema: string) {
    this.#tx = tx;
    this.#changeLog = `${schema}.changeLog`;
    this.#replicationState = `${schema}.replicationState`;
    this.#replicationConfig = `${schema}.replicationConfig`;
  }

  async bounds(): Promise<ChangeLogComparisonBounds> {
    const [row] = await this.#tx<
      {
        replicaVersion: string;
        headWatermark: string;
        minWatermark: string | null;
        maxWatermark: string | null;
      }[]
    > /*sql*/ `
      SELECT config."replicaVersion" AS "replicaVersion",
             state."lastWatermark" AS "headWatermark",
             bounds."minWatermark" AS "minWatermark",
             bounds."maxWatermark" AS "maxWatermark"
        FROM ${this.#tx(this.#replicationConfig)} AS config
        CROSS JOIN ${this.#tx(this.#replicationState)} AS state
        CROSS JOIN (
          SELECT min("watermark") AS "minWatermark",
                 max("watermark") AS "maxWatermark"
            FROM ${this.#tx(this.#changeLog)}
        ) AS bounds
    `;
    assert(row !== undefined, 'Postgres change-log state must be initialized');
    return row;
  }

  async inspect(watermark: string): Promise<ChangeLogRangeInspection> {
    const bounds = await this.bounds();
    const [boundary] = await this.#tx<RawBoundary[]> /*sql*/ `
      SELECT
        (SELECT "pos"::text
           FROM ${this.#tx(this.#changeLog)}
          WHERE "watermark" = ${watermark}
          ORDER BY "pos"
          LIMIT 1) AS "firstPos",
        (SELECT "change"::text
           FROM ${this.#tx(this.#changeLog)}
          WHERE "watermark" = ${watermark}
          ORDER BY "pos"
          LIMIT 1) AS "firstChange",
        (SELECT "change"::text
           FROM ${this.#tx(this.#changeLog)}
          WHERE "watermark" = ${watermark}
          ORDER BY "pos" DESC
          LIMIT 1) AS "lastChange",
        (SELECT "precommit"
           FROM ${this.#tx(this.#changeLog)}
          WHERE "watermark" = ${watermark}
          ORDER BY "pos" DESC
          LIMIT 1) AS "lastPrecommit"
    `;
    assert(boundary !== undefined, 'Postgres boundary query must return a row');
    return inspectBoundary(bounds, watermark, boundary);
  }

  async *read(
    watermark: string,
    batchSize: number,
    signal?: AbortSignal | undefined,
  ): AsyncIterable<readonly ChangeLogComparisonRow[]> {
    throwIfAborted(signal);
    for await (const batch of this.#tx<RawPostgresRow[]> /*sql*/ `
      SELECT "watermark", "pos"::text AS "pos", "change"::text AS "change"
        FROM ${this.#tx(this.#changeLog)}
       WHERE "watermark" = ${watermark}
       ORDER BY "pos"
    `.cursor(batchSize)) {
      throwIfAborted(signal);
      yield batch.map(row => comparisonRow(row, Number(row.pos)));
    }
  }
}

class SQLiteChangeLogComparisonSource implements ChangeLogComparisonSource {
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  #db: Database | undefined;

  constructor(lc: LogContext, replicaFile: string) {
    this.#lc = lc;
    this.#replicaFile = replicaFile;
  }

  withRead<T>(
    read: (reader: ChangeLogComparisonReader) => Promise<T>,
  ): Promise<T> {
    return read(new SQLiteChangeLogComparisonReader(this.#database()));
  }

  inspectCurrent(watermark: string): Promise<ChangeLogRangeInspection> {
    return this.withRead(reader => reader.inspect(watermark));
  }

  close(): void {
    this.#db?.close();
    this.#db = undefined;
  }

  #database(): Database {
    return (this.#db ??= new Database(
      this.#lc.withContext('component', 'sqlite-change-log-compare-reader'),
      this.#replicaFile,
      {readonly: true},
    ));
  }
}

class SQLiteChangeLogComparisonReader implements ChangeLogComparisonReader {
  readonly #db: Database;

  constructor(db: Database) {
    this.#db = db;
  }

  bounds(): Promise<ChangeLogComparisonBounds> {
    const version = this.#db
      .prepare(/*sql*/ `
        SELECT "schemaVersion"
          FROM "_zero.versionHistory"
      `)
      .get<{schemaVersion: number} | undefined>();
    assert(version !== undefined, 'SQLite schema version must be initialized');
    const state = this.#db
      .prepare(/*sql*/ `
        SELECT config."replicaVersion", state."stateVersion" AS "headWatermark"
          FROM "_zero.replicationConfig" AS config,
               "_zero.replicationState" AS state
      `)
      .get<{replicaVersion: string; headWatermark: string} | undefined>();
    assert(state !== undefined, 'SQLite replication state must be initialized');
    if (version.schemaVersion < SQLITE_CHANGE_LOG_SCHEMA_VERSION) {
      return Promise.resolve({
        ...state,
        schemaVersion: version.schemaVersion,
        minWatermark: null,
        maxWatermark: null,
      });
    }
    const bounds = this.#db
      .prepare(/*sql*/ `
        SELECT min("watermark") AS "minWatermark",
               max("watermark") AS "maxWatermark"
          FROM "${CHANGE_LOG_STREAM_TABLE}"
      `)
      .get<{minWatermark: string | null; maxWatermark: string | null}>();
    return Promise.resolve({...state, ...version, ...bounds});
  }

  async inspect(watermark: string): Promise<ChangeLogRangeInspection> {
    const bounds = await this.bounds();
    if ((bounds.schemaVersion ?? 0) < SQLITE_CHANGE_LOG_SCHEMA_VERSION) {
      return {bounds, status: 'outside-retention'};
    }
    const boundary = this.#db
      .prepare(/*sql*/ `
        SELECT
          (SELECT "pos"
             FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = @watermark
            ORDER BY "pos"
            LIMIT 1) AS "firstPos",
          (SELECT "change"
             FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = @watermark
            ORDER BY "pos"
            LIMIT 1) AS "firstChange",
          (SELECT "change"
             FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = @watermark
            ORDER BY "pos" DESC
            LIMIT 1) AS "lastChange",
          (SELECT "precommit"
             FROM "${CHANGE_LOG_STREAM_TABLE}"
            WHERE "watermark" = @watermark
            ORDER BY "pos" DESC
            LIMIT 1) AS "lastPrecommit"
      `)
      .get<RawBoundary>({watermark});
    return inspectBoundary(bounds, watermark, boundary);
  }

  async *read(
    watermark: string,
    batchSize: number,
    signal?: AbortSignal | undefined,
  ): AsyncIterable<readonly ChangeLogComparisonRow[]> {
    const readBatch = this.#db.prepare(/*sql*/ `
      SELECT "watermark", "pos", "change"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
       WHERE "watermark" = ? AND "pos" > ?
       ORDER BY "pos"
       LIMIT ?
    `);
    let lastPos = -1;
    while (true) {
      throwIfAborted(signal);
      const rows = readBatch.all<RawSQLiteRow>(watermark, lastPos, batchSize);
      throwIfAborted(signal);
      if (rows.length === 0) {
        return;
      }
      const last = rows.at(-1);
      assert(last !== undefined, 'non-empty SQLite batch must have a last row');
      lastPos = last.pos;
      // all() has exhausted the statement and ended its implicit read
      // transaction before control is yielded to comparison/flow control.
      yield rows.map(row => comparisonRow(row, row.pos));
    }
  }
}

function inspectBoundary(
  bounds: ChangeLogComparisonBounds,
  watermark: string,
  boundary: RawBoundary,
): ChangeLogRangeInspection {
  if (
    bounds.minWatermark === null ||
    watermark < bounds.minWatermark ||
    watermark > bounds.headWatermark
  ) {
    return {bounds, status: 'outside-retention'};
  }
  const firstPos =
    boundary.firstPos === null ? null : Number(boundary.firstPos);
  const complete =
    firstPos === 0 &&
    tagFromChange(boundary.firstChange) === 'begin' &&
    tagFromChange(boundary.lastChange) === 'commit' &&
    boundary.lastPrecommit === watermark;
  return {bounds, status: complete ? 'complete' : 'incomplete'};
}

function comparisonRow(
  row: Omit<RawPostgresRow, 'pos'> | Omit<RawSQLiteRow, 'pos'>,
  pos: number,
): ChangeLogComparisonRow {
  assert(
    Number.isSafeInteger(pos) && pos >= 0,
    'change-log position must be a non-negative safe integer',
  );
  const tag = tagFromChange(row.change);
  return {
    watermark: row.watermark,
    pos,
    tag,
    json: reconstructWatermarkedChange({
      watermark: row.watermark,
      tag,
      change: row.change,
    })[2],
  };
}

function tagFromChange(change: string | null): string {
  assert(change !== null, 'change-log boundary must contain a change');
  const parsed: unknown = BigIntJSON.parse(change);
  assert(
    typeof parsed === 'object' &&
      parsed !== null &&
      !Array.isArray(parsed) &&
      'tag' in parsed &&
      typeof parsed.tag === 'string',
    'change-log row must contain a string tag',
  );
  return parsed.tag;
}

type RowDifference = {
  readonly reason: Exclude<SQLiteChangeLogMismatchReason, 'bound-mismatch'>;
  readonly rowIndex: number;
  readonly pgRow?: ChangeLogComparisonRow | undefined;
  readonly sqliteRow?: ChangeLogComparisonRow | undefined;
};

async function compareRows(
  pgBatches: AsyncIterable<readonly ChangeLogComparisonRow[]>,
  sqliteBatches: AsyncIterable<readonly ChangeLogComparisonRow[]>,
): Promise<{
  comparedRows: number;
  difference?: RowDifference | undefined;
}> {
  const pg = flatten(pgBatches)[Symbol.asyncIterator]();
  const sqlite = flatten(sqliteBatches)[Symbol.asyncIterator]();
  let comparedRows = 0;
  try {
    let pgResult = await pg.next();
    let sqliteResult = await sqlite.next();
    while (!pgResult.done && !sqliteResult.done) {
      const pgRow = pgResult.value;
      const sqliteRow = sqliteResult.value;
      const position = comparePosition(pgRow, sqliteRow);
      if (position < 0) {
        return {
          comparedRows,
          difference: {
            reason: 'missing-sqlite-row',
            rowIndex: comparedRows,
            pgRow,
            sqliteRow,
          },
        };
      }
      if (position > 0) {
        return {
          comparedRows,
          difference: {
            reason: 'missing-pg-row',
            rowIndex: comparedRows,
            pgRow,
            sqliteRow,
          },
        };
      }
      if (pgRow.tag !== sqliteRow.tag) {
        return {
          comparedRows,
          difference: {
            reason: 'tag-mismatch',
            rowIndex: comparedRows,
            pgRow,
            sqliteRow,
          },
        };
      }
      if (pgRow.json !== sqliteRow.json) {
        return {
          comparedRows,
          difference: {
            reason: 'byte-mismatch',
            rowIndex: comparedRows,
            pgRow,
            sqliteRow,
          },
        };
      }
      comparedRows++;
      pgResult = await pg.next();
      sqliteResult = await sqlite.next();
    }
    if (!pgResult.done) {
      return {
        comparedRows,
        difference: {
          reason: 'missing-sqlite-row',
          rowIndex: comparedRows,
          pgRow: pgResult.value,
        },
      };
    }
    if (!sqliteResult.done) {
      return {
        comparedRows,
        difference: {
          reason: 'missing-pg-row',
          rowIndex: comparedRows,
          sqliteRow: sqliteResult.value,
        },
      };
    }
    return {comparedRows};
  } finally {
    await Promise.all([pg.return?.(), sqlite.return?.()]);
  }
}

async function* flatten(
  batches: AsyncIterable<readonly ChangeLogComparisonRow[]>,
): AsyncIterable<ChangeLogComparisonRow> {
  for await (const batch of batches) {
    yield* batch;
  }
}

function comparePosition(
  pg: ChangeLogComparisonRow,
  sqlite: ChangeLogComparisonRow,
): number {
  if (pg.watermark !== sqlite.watermark) {
    return pg.watermark < sqlite.watermark ? -1 : 1;
  }
  return pg.pos - sqlite.pos;
}

function summarize(
  row: ChangeLogComparisonRow | undefined,
): ChangeLogComparisonRowSummary | undefined {
  return row
    ? {
        watermark: row.watermark,
        pos: row.pos,
        tag: row.tag,
        jsonBytes: Buffer.byteLength(row.json),
      }
    : undefined;
}

function readerError(
  targetWatermark: string,
  error: unknown,
): SQLiteChangeLogComparisonResult {
  return {
    outcome: 'inconclusive',
    reason: 'reader-error',
    targetWatermark,
    retry: true,
    errorName: error instanceof Error ? error.name : typeof error,
  };
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw new AbortError('SQLite change-log comparison aborted');
  }
}

function assertPercent(value: number): void {
  assert(
    Number.isSafeInteger(value) && value >= 0 && value <= 100,
    'SQLite change-log compare percentage must be an integer between 0 and 100',
  );
}

function assertPositiveSafeInteger(value: number, name: string): void {
  assert(
    Number.isSafeInteger(value) && value > 0,
    `SQLite change-log compare ${name} must be a positive safe integer`,
  );
}
