import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {ShardID} from '../../types/shards.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_STREAM_TABLE,
  readChangeLogMeta,
  type ChangeLogIdentity,
  type ChangeLogMeta,
} from '../replicator/change-log-db.ts';
import {
  reconstructWatermarkedChange,
  type ChangeLogEntry,
} from './change-log-codec.ts';
import {
  digestCatchupRange,
  isSampledForCompare,
  type CatchupRangeDigest,
} from './change-log-compare-digest.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';

/** Interval between comparison cycles. */
const DEFAULT_COMPARE_INTERVAL_MS = 30_000;

/** Maximum transactions enumerated per cycle. */
const DEFAULT_MAX_TRANSACTIONS_PER_CYCLE = 256;

/** Maximum catchup rows read from each store per cycle. */
const DEFAULT_MAX_ROWS_PER_SOURCE_PER_CYCLE = 1000;

/**
 * Maximum catchup bytes read from each store per cycle.
 *
 * Rows bound the per-row overhead. Bytes bound the read and hash work,
 * which scales with payload size instead of row count. Normal traffic
 * reaches the row limit first. Wide payloads reach this one.
 */
const DEFAULT_MAX_BYTES_PER_SOURCE_PER_CYCLE = 32 * 1024 * 1024;

/** Rows per SQLite catchup batch. */
const DEFAULT_READ_BATCH_ROWS = 1000;

/**
 * Result for one committed transaction.
 *
 * `mismatch` covers missing, extra, changed, or reordered rows.
 * `inconclusive` means that store bounds changed during the comparison.
 */
export type CompareOutcome =
  | 'match'
  | 'mismatch'
  | 'inconclusive'
  | 'deferred'
  | 'oversized';

export type CompareSkipReason =
  // The comparator was stopped.
  | 'stopped'
  // The writer replaced the file during this cycle.
  | 'log-rebuilt'
  // The file is absent, unreadable, or uninitialized.
  | 'log-unavailable'
  // The schema version does not match this build.
  | 'ineligible-schema'
  // The log belongs to a different replica.
  | 'ineligible-identity'
  // The log is younger than the warm-up period.
  | 'cold-log'
  // The stores have no complete committed range in common.
  | 'nothing-to-compare'
  // The cycle failed. The error was logged and counted.
  | 'error';

export type CompareCycleResult =
  | {readonly kind: 'skipped'; readonly reason: CompareSkipReason}
  | {
      readonly kind: 'compared';
      /** Exclusive lower bound of the compared range. */
      readonly fromWatermark: string;
      /** Inclusive upper bound handled. The next cycle resumes from here. */
      readonly throughWatermark: string;
      /** Committed transactions handled through `throughWatermark`. */
      readonly transactions: number;
      /** Transactions with compared catchup output. */
      readonly sampled: number;
      readonly matched: number;
      readonly mismatched: number;
      /** Suspects rejected because store bounds changed. */
      readonly inconclusive: number;
      /** Sampled transactions deferred to a fresh budget. */
      readonly deferred: number;
      /** Sampled transactions that exceed a fresh budget. */
      readonly oversized: number;
    };

/** Provides the Postgres side of the catchup comparison. */
export interface PGChangeLogRangeReader {
  getCatchupBounds(): Promise<{
    minWatermark: string | null;
    lastWatermark: string;
  }>;
  listCommitWatermarks(
    afterWatermark: string,
    throughWatermark: string,
    limit: number,
  ): Promise<string[]>;
  readCatchupRange(
    afterWatermark: string,
    throughWatermark: string,
    batchRows?: number | undefined,
  ): AsyncIterable<ChangeLogEntry[]>;
}

export type SQLiteChangeLogCompareOptions = {
  /** Stable percentage of transactions selected for payload comparison. */
  readonly comparePercent: number;
  /** Warm-up time after a seed. The comparator skips a younger log. */
  readonly retentionMs: number;
  readonly intervalMs?: number | undefined;
  readonly maxTransactionsPerCycle?: number | undefined;
  /** Maximum catchup rows read from each store in one cycle. */
  readonly maxRowsPerSourcePerCycle?: number | undefined;
  /** Maximum catchup bytes read from each store in one cycle. */
  readonly maxBytesPerSourcePerCycle?: number | undefined;
  readonly readBatchRows?: number | undefined;
  /** Clock for the warm-up period. Defaults to `Date.now`. */
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
  /** Yields between sampled transactions. Defaults to `setImmediate`. */
  readonly yieldFn?: (() => Promise<void>) | undefined;
};

/** Uses separate aggregates so SQLite can use an index for each value. */
const SQLITE_BOUNDS_SQL = /*sql*/ `
  SELECT
    (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "minWatermark",
    (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "headWatermark"
`;

/** Lists committed transaction watermarks in `(@after, @through]`. */
const SQLITE_COMMITS_SQL = /*sql*/ `
  SELECT "watermark" FROM "${CHANGE_LOG_STREAM_TABLE}"
   WHERE "precommit" IS NOT NULL
     AND "watermark" > @after
     AND "watermark" <= @through
   ORDER BY "watermark"
   LIMIT @limit
`;

type SQLiteBounds = {
  readonly minWatermark: string;
  readonly headWatermark: string;
};

type PinnedBounds = {
  readonly meta: ChangeLogMeta;
  readonly sqlite: SQLiteBounds;
  readonly pgMinWatermark: string;
  readonly pgLastWatermark: string;
};

type SampledComparison = {
  readonly outcome: CompareOutcome;
  readonly sqliteRowsRead: number;
  readonly pgRowsRead: number;
  readonly sqliteBytesRead: number;
  readonly pgBytesRead: number;
};

/**
 * Compares SQLite and Postgres catchup output without affecting subscribers.
 *
 * Each cycle compares complete ranges through the lower store head.
 * It checks every transaction for presence and samples payloads.
 * A bounds change makes the affected result `inconclusive`.
 * Postgres remains authoritative.
 */
export class SQLiteChangeLogComparator {
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #changeLogFile: string;
  readonly #identity: ChangeLogIdentity;
  readonly #pg: PGChangeLogRangeReader;
  readonly #opts: SQLiteChangeLogCompareOptions;
  readonly #now: () => number;
  readonly #setTimeoutFn: typeof setTimeout;
  readonly #clearTimeoutFn: typeof clearTimeout;
  readonly #yield: () => Promise<void>;

  readonly #compareResults = getOrCreateCounter(
    'replica',
    'sqlite_change_log.compare_result',
    'Outcomes of the dark comparison between what SQLite catchup serves and ' +
      'what PG catchup serves, per committed transaction. `inconclusive` ' +
      'means a purge or reseed moved the pinned bounds mid-comparison.',
  );
  readonly #compareCycles = getOrCreateCounter(
    'replica',
    'sqlite_change_log.compare_cycles',
    'SQLite change-log comparison cycles, by result.',
  );
  readonly #cycleDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.compare_cycle_duration',
    'Duration of one SQLite change-log comparison cycle.',
  );

  /** Last handled commit watermark. A restart derives it from current bounds. */
  #cursor: string | undefined;
  #running: Promise<CompareCycleResult> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stopped = false;
  #continueWithoutDelay = false;
  #fileGeneration = 0;

  constructor(
    lc: LogContext,
    shard: ShardID,
    changeLogFile: string,
    identity: ChangeLogIdentity,
    pg: PGChangeLogRangeReader,
    opts: SQLiteChangeLogCompareOptions,
  ) {
    this.#lc = lc.withContext('component', 'sqlite-change-log-compare');
    this.#shard = shard;
    this.#changeLogFile = changeLogFile;
    this.#identity = identity;
    this.#pg = pg;
    this.#opts = opts;
    this.#now = opts.now ?? Date.now;
    this.#setTimeoutFn = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeoutFn = opts.clearTimeoutFn ?? clearTimeout;
    this.#yield =
      opts.yieldFn ??
      (() => new Promise<void>(resolve => setImmediate(resolve)));
    this.#scheduleNext();
  }

  /**
   * Runs one cycle. Concurrent calls share the same promise.
   * Errors produce `skipped: 'error'`.
   */
  compareOnce(): Promise<CompareCycleResult> {
    if (this.#stopped) {
      return Promise.resolve({kind: 'skipped', reason: 'stopped'});
    }
    if (this.#running === undefined) {
      this.#running = this.#runCycle()
        .then(result => {
          if (this.#continueWithoutDelay) {
            this.#rescheduleNext(0);
          }
          return result;
        })
        .finally(() => {
          this.#running = undefined;
        });
    }
    return this.#running;
  }

  stop(): void {
    this.#stopped = true;
    if (this.#timer !== undefined) {
      this.#clearTimeoutFn(this.#timer);
      this.#timer = undefined;
    }
  }

  /** Invalidates cycles that can still read a replaced file. */
  invalidate(): void {
    this.#fileGeneration++;
    this.#cursor = undefined;
    this.#continueWithoutDelay = false;
  }

  #scheduleNext(
    delayMs = this.#opts.intervalMs ?? DEFAULT_COMPARE_INTERVAL_MS,
  ): void {
    if (this.#stopped || this.#timer !== undefined) {
      return;
    }
    this.#timer = this.#setTimeoutFn(() => {
      this.#timer = undefined;
      void this.compareOnce().finally(() => this.#scheduleNext());
    }, delayMs);
  }

  #rescheduleNext(delayMs: number): void {
    if (this.#timer !== undefined) {
      this.#clearTimeoutFn(this.#timer);
      this.#timer = undefined;
    }
    this.#scheduleNext(delayMs);
  }

  async #runCycle(): Promise<CompareCycleResult> {
    const startedAt = performance.now();
    const fileGeneration = this.#fileGeneration;
    this.#continueWithoutDelay = false;
    let db: Database | undefined;
    let reader: SQLiteChangeLogReader | undefined;
    try {
      try {
        // A readonly handle does not create a missing file.
        db = new Database(this.#lc, this.#changeLogFile, {readonly: true});
      } catch {
        return this.#skipped('log-unavailable');
      }

      const pinned = await this.#pinBounds(db);
      if (fileGeneration !== this.#fileGeneration) {
        return this.#skipped('log-rebuilt');
      }
      if (typeof pinned === 'string') {
        return this.#skipped(pinned);
      }
      const {meta} = pinned;

      const from = maxWatermark(
        this.#cursor ?? meta.seedWatermark,
        // Exclude the synthetic SQLite seed transaction.
        // The Postgres seed has no `precommit`, so enumeration also excludes it.
        meta.seedWatermark,
        // A store cannot serve the transaction at its minimum watermark.
        pinned.pgMinWatermark,
        pinned.sqlite.minWatermark,
      );
      const through = minWatermark(
        pinned.pgLastWatermark,
        pinned.sqlite.headWatermark,
      );
      if (through <= from) {
        return this.#skipped('nothing-to-compare');
      }

      let transactions = 0;
      let sampled = 0;
      const outcomes: Record<CompareOutcome, number> = {
        match: 0,
        mismatch: 0,
        inconclusive: 0,
        deferred: 0,
        oversized: 0,
      };
      const record = (watermark: string, outcome: CompareOutcome) => {
        this.#compareResults.add(1, {outcome});
        outcomes[outcome]++;
        if (outcome === 'mismatch') {
          // Log only the watermark because payloads contain customer data.
          this.#lc.error?.(
            'SQLite change-log catchup output diverged from Postgres',
            {sqliteChangeLogCompare: {watermark}},
          );
        }
      };

      reader = new SQLiteChangeLogReader(this.#lc, this.#changeLogFile);
      const plan = reader.plan(from);
      if (plan.kind === 'not-ready') {
        return this.#skipped('log-unavailable');
      }
      if (plan.kind === 'ahead') {
        return this.#skipped('nothing-to-compare');
      }
      if (plan.kind === 'too-old') {
        // A fixed lower bound makes this result a mismatch.
        // A changed lower bound makes it inconclusive.
        const reconfirmed = await this.#reconfirm(db, pinned, from);
        const outcome =
          fileGeneration === this.#fileGeneration
            ? reconfirmed
            : 'inconclusive';
        record(from, outcome);
        if (outcome === 'inconclusive') {
          // Keep the cursor unchanged because the bounds moved.
          return this.#compared(from, from, transactions, sampled, outcomes);
        }
        // Positional reads can continue after a missing boundary.
        // Advancing avoids reporting the same boundary in every cycle.
      }

      const limit = Math.max(
        1,
        this.#opts.maxTransactionsPerCycle ??
          DEFAULT_MAX_TRANSACTIONS_PER_CYCLE,
      );
      const union = await this.#enumerateCommits(db, from, through, limit);
      if (fileGeneration !== this.#fileGeneration) {
        return this.#skipped('log-rebuilt');
      }
      const maxRowsPerSource = Math.max(
        1,
        this.#opts.maxRowsPerSourcePerCycle ??
          DEFAULT_MAX_ROWS_PER_SOURCE_PER_CYCLE,
      );
      const maxBytesPerSource = Math.max(
        1,
        this.#opts.maxBytesPerSourcePerCycle ??
          DEFAULT_MAX_BYTES_PER_SOURCE_PER_CYCLE,
      );

      let sqliteRowsRead = 0;
      let pgRowsRead = 0;
      let sqliteBytesRead = 0;
      let pgBytesRead = 0;
      let previousBoundary = from;
      let handledThrough = from;
      for (const {watermark, inSqlite, inPg} of union) {
        if (this.#stopped) {
          break;
        }
        let stopAfterTransaction = false;
        if (!inSqlite || !inPg) {
          // Check every transaction for presence because this needs no payload read.
          const outcome = await this.#reconfirm(db, pinned, watermark);
          record(
            watermark,
            fileGeneration === this.#fileGeneration ? outcome : 'inconclusive',
          );
        } else if (
          isSampledForCompare(this.#shard, watermark, this.#opts.comparePercent)
        ) {
          const sqliteRowsRemaining = maxRowsPerSource - sqliteRowsRead;
          const pgRowsRemaining = maxRowsPerSource - pgRowsRead;
          // A read may pass the byte budget by its last row, so this
          // compares against zero rather than checking for equality.
          const sqliteBytesRemaining = maxBytesPerSource - sqliteBytesRead;
          const pgBytesRemaining = maxBytesPerSource - pgBytesRead;
          if (
            sqliteRowsRemaining === 0 ||
            pgRowsRemaining === 0 ||
            sqliteBytesRemaining <= 0 ||
            pgBytesRemaining <= 0
          ) {
            record(watermark, 'deferred');
            break;
          }
          sampled++;
          const comparison = await this.#compareTransaction(
            db,
            reader,
            pinned,
            previousBoundary,
            watermark,
            sqliteRowsRemaining,
            pgRowsRemaining,
            sqliteBytesRemaining,
            pgBytesRemaining,
            maxRowsPerSource,
            maxBytesPerSource,
          );
          sqliteRowsRead += comparison.sqliteRowsRead;
          pgRowsRead += comparison.pgRowsRead;
          sqliteBytesRead += comparison.sqliteBytesRead;
          pgBytesRead += comparison.pgBytesRead;
          const outcome =
            fileGeneration === this.#fileGeneration
              ? comparison.outcome
              : 'inconclusive';
          record(watermark, outcome);
          if (outcome === 'inconclusive') {
            break;
          }
          if (outcome === 'deferred') {
            break;
          }
          if (outcome === 'oversized') {
            // Advance past a transaction that cannot fit in a fresh budget.
            stopAfterTransaction = true;
          }
          // Yield between sampled reads so other stream work can continue.
          await this.#yield();
        }
        previousBoundary = watermark;
        handledThrough = watermark;
        transactions++;
        if (stopAfterTransaction) {
          break;
        }
      }

      if (
        !this.#stopped &&
        fileGeneration === this.#fileGeneration &&
        outcomes.inconclusive === 0
      ) {
        this.#cursor = handledThrough;
        this.#continueWithoutDelay = handledThrough < through;
      }
      return this.#compared(
        from,
        handledThrough,
        transactions,
        sampled,
        outcomes,
      );
    } catch (e) {
      this.#lc.warn?.('error comparing the SQLite change log', e);
      return this.#skipped('error');
    } finally {
      reader?.close();
      db?.close();
      this.#cycleDuration.recordMs(performance.now() - startedAt);
    }
  }

  /** Checks log eligibility and reads the bounds from both stores. */
  async #pinBounds(db: Database): Promise<PinnedBounds | CompareSkipReason> {
    let meta: ChangeLogMeta;
    try {
      meta = readChangeLogMeta(db);
    } catch {
      // The writer has not initialized the log.
      return 'log-unavailable';
    }
    if (meta.schemaVersion !== CHANGE_LOG_DB_SCHEMA_VERSION) {
      return 'ineligible-schema';
    }
    const {epoch, generation, replicaID} = this.#identity;
    if (
      meta.epoch !== epoch ||
      meta.generation !== generation ||
      meta.replicaID !== replicaID
    ) {
      return 'ineligible-identity';
    }
    if (this.#now() - meta.seededAtMs < this.#opts.retentionMs) {
      return 'cold-log';
    }
    const sqlite = this.#readSQLiteBounds(db);
    if (sqlite === undefined) {
      return 'log-unavailable';
    }
    const {minWatermark, lastWatermark} = await this.#pg.getCatchupBounds();
    if (minWatermark === null) {
      // Only a new replica has an empty Postgres change log.
      return 'nothing-to-compare';
    }
    return {
      meta,
      sqlite,
      pgMinWatermark: minWatermark,
      pgLastWatermark: lastWatermark,
    };
  }

  #readSQLiteBounds(db: Database): SQLiteBounds | undefined {
    const bounds = db
      .prepare(SQLITE_BOUNDS_SQL)
      .get<{minWatermark: string | null; headWatermark: string | null}>();
    return bounds.minWatermark === null || bounds.headWatermark === null
      ? undefined
      : {
          minWatermark: bounds.minWatermark,
          headWatermark: bounds.headWatermark,
        };
  }

  /**
   * Merges committed transaction watermarks from both stores.
   * If a list reaches the limit, the result ends at the last complete common range.
   */
  async #enumerateCommits(
    db: Database,
    from: string,
    through: string,
    limit: number,
  ): Promise<{watermark: string; inSqlite: boolean; inPg: boolean}[]> {
    let sqlite: string[] = db
      .prepare(SQLITE_COMMITS_SQL)
      .all<{watermark: string}>({after: from, through, limit: limit + 1})
      .map(({watermark}) => watermark);
    let pg = await this.#pg.listCommitWatermarks(from, through, limit + 1);

    let effectiveThrough = through;
    if (sqlite.length > limit) {
      sqlite.length = limit;
      effectiveThrough = minWatermark(effectiveThrough, sqlite[limit - 1]);
    }
    if (pg.length > limit) {
      pg.length = limit;
      effectiveThrough = minWatermark(effectiveThrough, pg[limit - 1]);
    }
    if (effectiveThrough !== through) {
      sqlite = sqlite.filter(w => w <= effectiveThrough);
      pg = pg.filter(w => w <= effectiveThrough);
    }

    const union: {watermark: string; inSqlite: boolean; inPg: boolean}[] = [];
    let s = 0;
    let p = 0;
    while (s < sqlite.length || p < pg.length) {
      const sw = s < sqlite.length ? sqlite[s] : undefined;
      const pw = p < pg.length ? pg[p] : undefined;
      if (sw !== undefined && (pw === undefined || sw < pw)) {
        union.push({watermark: sw, inSqlite: true, inPg: false});
        s++;
      } else if (pw !== undefined && (sw === undefined || pw < sw)) {
        union.push({watermark: pw, inSqlite: false, inPg: true});
        p++;
      } else if (sw !== undefined) {
        union.push({watermark: sw, inSqlite: true, inPg: true});
        s++;
        p++;
      }
    }
    return union;
  }

  /**
   * Compares the output in `(previousBoundary, watermark]`.
   * Positional reads do not require either store to contain the boundary row.
   */
  async #compareTransaction(
    db: Database,
    reader: SQLiteChangeLogReader,
    pinned: PinnedBounds,
    previousBoundary: string,
    watermark: string,
    sqliteMaxRows: number,
    pgMaxRows: number,
    sqliteMaxBytes: number,
    pgMaxBytes: number,
    maxRowsPerSource: number,
    maxBytesPerSource: number,
  ): Promise<SampledComparison> {
    let sqlite: CatchupRangeDigest;
    let pg: CatchupRangeDigest;
    try {
      const readBatchRows = this.#opts.readBatchRows ?? DEFAULT_READ_BATCH_ROWS;
      sqlite = await digestCatchupRange(
        reader.read(
          previousBoundary,
          watermark,
          Math.min(readBatchRows, sqliteMaxRows),
          undefined,
          sqliteMaxRows,
        ),
        watermark,
        sqliteMaxRows,
        sqliteMaxBytes,
      );
      pg = await digestCatchupRange(
        reconstructBatches(
          this.#pg.readCatchupRange(previousBoundary, watermark, pgMaxRows),
        ),
        watermark,
        pgMaxRows,
        pgMaxBytes,
      );
    } catch (e) {
      // A failed catchup read makes the transaction suspect.
      this.#lc.warn?.(
        `error reading transaction ${watermark} for comparison`,
        e,
      );
      return {
        outcome: await this.#reconfirm(db, pinned, watermark),
        sqliteRowsRead: 0,
        pgRowsRead: 0,
        sqliteBytesRead: 0,
        pgBytesRead: 0,
      };
    }
    const read = {
      sqliteRowsRead: sqlite.rows,
      pgRowsRead: pg.rows,
      sqliteBytesRead: sqlite.bytes,
      pgBytesRead: pg.bytes,
    };
    if (sqlite.limitReached || pg.limitReached) {
      // Skip a transaction that cannot fit in a fresh budget.
      // Defer one that does not fit only in the remaining budget.
      // A budget is fresh when neither dimension has been spent.
      const sqliteFresh =
        sqliteMaxRows === maxRowsPerSource &&
        sqliteMaxBytes === maxBytesPerSource;
      const pgFresh =
        pgMaxRows === maxRowsPerSource && pgMaxBytes === maxBytesPerSource;
      const outcome =
        (sqlite.limitReached && sqliteFresh) || (pg.limitReached && pgFresh)
          ? 'oversized'
          : 'deferred';
      return {outcome, ...read};
    }
    if (sqlite.digest === pg.digest) {
      return {outcome: 'match', ...read};
    }
    return {
      outcome: await this.#reconfirm(db, pinned, watermark),
      ...read,
    };
  }

  /**
   * Rechecks a suspect result against current store bounds.
   * A moved bound, changed seed, failed read, or lower head makes the result inconclusive.
   */
  async #reconfirm(
    db: Database,
    pinned: PinnedBounds,
    watermark: string,
  ): Promise<'mismatch' | 'inconclusive'> {
    try {
      const meta = readChangeLogMeta(db);
      if (
        meta.seedWatermark !== pinned.meta.seedWatermark ||
        meta.seededAtMs !== pinned.meta.seededAtMs
      ) {
        return 'inconclusive';
      }
      const sqlite = this.#readSQLiteBounds(db);
      const {minWatermark, lastWatermark} = await this.#pg.getCatchupBounds();
      if (sqlite === undefined || minWatermark === null) {
        return 'inconclusive';
      }
      if (
        (sqlite.minWatermark > pinned.sqlite.minWatermark &&
          watermark <= sqlite.minWatermark) ||
        (minWatermark > pinned.pgMinWatermark && watermark <= minWatermark) ||
        watermark > sqlite.headWatermark ||
        watermark > lastWatermark
      ) {
        return 'inconclusive';
      }
      return 'mismatch';
    } catch {
      return 'inconclusive';
    }
  }

  #skipped(reason: CompareSkipReason): CompareCycleResult {
    this.#compareCycles.add(1, {result: reason});
    if (reason === 'error') {
      // The caller logged the error.
    } else if (reason !== 'nothing-to-compare') {
      this.#lc.debug?.(`skipping SQLite change-log comparison: ${reason}`);
    }
    return {kind: 'skipped', reason};
  }

  #compared(
    fromWatermark: string,
    throughWatermark: string,
    transactions: number,
    sampled: number,
    outcomes: Record<CompareOutcome, number>,
  ): CompareCycleResult {
    this.#compareCycles.add(1, {result: 'compared'});
    const result = {
      kind: 'compared',
      fromWatermark,
      throughWatermark,
      transactions,
      sampled,
      matched: outcomes.match,
      mismatched: outcomes.mismatch,
      inconclusive: outcomes.inconclusive,
      deferred: outcomes.deferred,
      oversized: outcomes.oversized,
    } as const;
    this.#lc.debug?.('SQLite change-log comparison cycle', {
      sqliteChangeLogCompare: result,
    });
    return result;
  }
}

/** Applies the same reconstruction as Postgres catchup. */
async function* reconstructBatches(
  batches: AsyncIterable<ChangeLogEntry[]>,
): AsyncIterable<WatermarkedChange[]> {
  for await (const batch of batches) {
    yield batch.map(reconstructWatermarkedChange);
  }
}

function minWatermark(a: string, b: string): string {
  return a < b ? a : b;
}

function maxWatermark(...watermarks: string[]): string {
  let max = watermarks[0];
  for (const w of watermarks) {
    if (w > max) {
      max = w;
    }
  }
  return max;
}
