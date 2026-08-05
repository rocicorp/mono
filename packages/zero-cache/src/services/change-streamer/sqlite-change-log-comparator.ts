import {createHash} from 'node:crypto';
import type {LogContext} from '@rocicorp/logger';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
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
  extractChangeSubstring,
  reconstructWatermarkedChange,
  type ChangeLogEntry,
} from './change-log-codec.ts';
import {ChangeLogTransactionHasher} from './change-log-transaction-hash.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';

/**
 * How often a comparison cycle runs. Each cycle is bounded by commit
 * enumeration and catchup row budgets, so a busy stream is compared in
 * bounded increments rather than all at once.
 */
const DEFAULT_COMPARE_INTERVAL_MS = 30_000;

/**
 * The per-cycle bound on enumerated transactions. This bounds the two commit
 * watermark lists; catchup payload work has a separate per-source row budget.
 */
const DEFAULT_MAX_TRANSACTIONS_PER_CYCLE = 256;

/** Catchup rows read from each store per comparison cycle. */
const DEFAULT_MAX_ROWS_PER_SOURCE_PER_CYCLE = 1000;

/** Rows per SQLite catchup read batch, matching the read path's default. */
const DEFAULT_READ_BATCH_ROWS = 1000;

/**
 * The outcome of one committed transaction's comparison.
 *
 * There is deliberately no taxonomy of *how* the two stores disagreed: a
 * missing transaction, an extra row, a mutated payload, and a reordered range
 * are all `mismatch`, and the watermark is enough to go look. The comparison
 * is advisory, and a finer classification cost more than it paid.
 *
 * `inconclusive` means the common lower bound (or a head) moved after the
 * cycle pinned it — a purge, truncation, or reseed raced the comparison — so
 * the observation says nothing about divergence and is deliberately not
 * reported as any.
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
  // The change-log file is absent, unreadable, or has no meta row yet: the
  // writer has not created it, or has failed soft and deleted it.
  | 'log-unavailable'
  // The log's schema version is not this build's.
  | 'ineligible-schema'
  // The log belongs to a different replica (§3.2's identity triple).
  | 'ineligible-identity'
  // The log was seeded less than a retention window ago (§6.6's predicate).
  | 'cold-log'
  // The complete committed range common to both stores is empty. Head skew in
  // either direction lands here; it is expected — the log leads — and is
  // deliberately not reported as divergence.
  | 'nothing-to-compare'
  // The cycle failed; the error was logged and counted.
  | 'error';

export type CompareCycleResult =
  | {readonly kind: 'skipped'; readonly reason: CompareSkipReason}
  | {
      readonly kind: 'compared';
      /** Exclusive lower bound of the compared range. */
      readonly fromWatermark: string;
      /** Inclusive upper bound handled; the next cycle resumes from here. */
      readonly throughWatermark: string;
      /** Committed transactions handled through `throughWatermark`. */
      readonly transactions: number;
      /** How many had their catchup output read and digested. */
      readonly sampled: number;
      readonly matched: number;
      readonly mismatched: number;
      /** Suspects a concurrent purge or reseed disqualified. */
      readonly inconclusive: number;
      /** Sampled transactions deferred until a fresh cycle row budget. */
      readonly deferred: number;
      /** Sampled transactions skipped because they exceed a fresh row budget. */
      readonly oversized: number;
    };

/**
 * The PG change log's side of the comparison. Implemented by `Storer`, whose
 * `readCatchupRange` shares its statement shape with the subscriber catchup
 * query — the thing this comparison exists to validate.
 */
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
  /**
   * `sqliteChangeLogComparePercent`: the stable percentage of committed
   * transactions whose catchup output is digest-compared. Presence (a
   * transaction one store holds and the other does not) is checked for every
   * transaction in the range regardless, since it needs no payload reads.
   */
  readonly comparePercent: number;
  /**
   * `sqliteChangeLogRetentionMs`, reused as the warm-up window: a log seeded
   * less than this long ago is declined (§6.6), since its covered range is
   * still filling.
   */
  readonly retentionMs: number;
  readonly intervalMs?: number | undefined;
  readonly maxTransactionsPerCycle?: number | undefined;
  /** Catchup payload rows read from each source in one cycle. */
  readonly maxRowsPerSourcePerCycle?: number | undefined;
  readonly readBatchRows?: number | undefined;
  /** The clock the warm-up window is measured against. Defaults to `Date.now`. */
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
  /**
   * Awaited between sampled transactions so fan-out and catchup make progress.
   * Defaults to a macrotask (`setImmediate`).
   */
  readonly yieldFn?: (() => Promise<void>) | undefined;
};

/**
 * Whether `watermark`'s transaction is in the compared sample.
 *
 * Stable by shard and watermark — a pure function of the two — so a retried
 * or restarted comparison selects exactly the same transactions and therefore
 * compares the same data.
 */
export function isSampledForCompare(
  shard: ShardID,
  watermark: string,
  percent: number,
): boolean {
  if (percent >= 100) {
    return true;
  }
  if (percent <= 0) {
    return false;
  }
  const digest = createHash('sha256')
    .update(`${shard.appID}/${shard.shardNum}:${watermark}`)
    .digest();
  return digest.readUInt32BE(0) % 100 < percent;
}

/**
 * Normalizes a stored `change` to one text form before hashing.
 *
 * The two stores hand the text back differently: SQLite serves the exact
 * serialized substring it stored, while PG holds it in a `json` column and
 * serves `change::text` — a round trip through the client's JSON parameter
 * encoding and `json_to_recordset`. Any formatting difference from that round
 * trip would report as a mismatch that is *not* divergence, which would be
 * indistinguishable from the finding this comparison exists to make. Both
 * sides are therefore re-serialized through the same codec; a value that does
 * not parse is hashed as-is, identically on both sides.
 */
export function normalizeChangeJSON(change: string): string {
  try {
    return BigIntJSON.stringify(BigIntJSON.parse(change));
  } catch {
    return change;
  }
}

export type CatchupRangeDigest = {
  readonly digest: string;
  readonly rows: number;
  /** The row cap was reached before the range's closing commit was served. */
  readonly limitReached: boolean;
};

/**
 * Digests everything a store serves over a catchup range, as one rolling hash
 * in served order.
 *
 * Position is the row's index *within the served range*, so the digest is a
 * pure function of what the store hands a subscriber: a missing row, an extra
 * row at any watermark, a mutated payload, and a reordering all change it.
 * That is what lets the comparison be a single equality check — there is no
 * per-watermark bookkeeping to decide which served rows "count".
 *
 * `precommit` is not hashed: for a commit row it equals that row's own
 * watermark, which is hashed already.
 */
export async function digestCatchupRange(
  batches: AsyncIterable<readonly WatermarkedChange[]>,
  throughWatermark: string,
  maxRows: number,
): Promise<CatchupRangeDigest> {
  const hasher = new ChangeLogTransactionHasher();
  let rows = 0;
  let servedClosingCommit = false;

  for await (const batch of batches) {
    for (const [watermark, tag, json] of batch) {
      if (rows === maxRows) {
        return {digest: hasher.digest(), rows, limitReached: true};
      }
      hasher.add({
        watermark,
        pos: rows,
        tag,
        change: normalizeChangeJSON(extractChangeSubstring(json, tag)),
        precommit: null,
      });
      rows++;
      if (tag === 'commit' && watermark === throughWatermark) {
        servedClosingCommit = true;
      }
    }
  }
  return {
    digest: hasher.digest(),
    rows,
    limitReached: rows === maxRows && !servedClosingCommit,
  };
}

/**
 * `min` and `max` as separate scalar subqueries for the single-aggregate index
 * optimization, matching the reader's plan query.
 */
const SQLITE_BOUNDS_SQL = /*sql*/ `
  SELECT
    (SELECT min("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "minWatermark",
    (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
      AS "headWatermark"
`;

/**
 * Commit watermarks in `(@after, @through]`. Every row of a transaction is
 * stored at its commit watermark and only commit rows carry `precommit`, so
 * this enumerates the committed transactions in the range.
 */
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
};

/**
 * Compares what SQLite catchup *serves* against what PG catchup serves, over
 * complete committed ranges, without serving either to a subscriber.
 *
 * "SQLite content equals PG content" is already established by construction —
 * both stores hold the same serialized substrings — plus the continuous
 * per-transaction hash check from 7H. What is *not* covered, and what this
 * validates, is the read path: reader reconstruction, `plan()` bounds, and
 * purge interacting with a pinned range. Each cycle enumerates the committed
 * transactions both stores hold, samples a stable subset, reads each sampled
 * range through both stores' catchup statements, and compares one rolling
 * digest of what each served.
 *
 * The comparison is dark and advisory: it changes nothing about what
 * subscribers receive, PG remains authoritative, and every anomaly is a
 * metric and a (redacted) log line rather than an action.
 *
 * Ranges are compared only through `min(pgHead, sqliteHead)` — head skew in
 * either direction is expected, the log leads, and is never reported. A purge,
 * truncation, or reseed that moves the pinned bounds mid-cycle reports
 * `inconclusive`, not divergence.
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

  /**
   * The commit watermark through which output has been compared. Ranges are
   * compared once and never re-read; a restart re-derives it from the common
   * lower bound, and the stable sample selects the same transactions again.
   */
  #cursor: string | undefined;
  #running: Promise<CompareCycleResult> | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #stopped = false;
  #continueWithoutDelay = false;

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
   * Runs one comparison cycle. Concurrent calls coalesce into the running
   * cycle's completion. Never rejects; a failed cycle is logged and reported
   * as `skipped: 'error'`.
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
    this.#continueWithoutDelay = false;
    let db: Database | undefined;
    let reader: SQLiteChangeLogReader | undefined;
    try {
      try {
        // A readonly handle cannot create the file, so an absent log — the
        // writer has not reconciled yet, or failed soft and deleted it — is a
        // routine decline rather than an error.
        db = new Database(this.#lc, this.#changeLogFile, {readonly: true});
      } catch {
        return this.#skipped('log-unavailable');
      }

      const pinned = await this.#pinBounds(db);
      if (typeof pinned === 'string') {
        return this.#skipped(pinned);
      }
      const {meta} = pinned;

      const from = maxWatermark(
        this.#cursor ?? meta.seedWatermark,
        // The log's seed transaction is synthetic — its content matches
        // nothing PG holds — so the exclusive lower bound keeps it out of
        // every comparison. (PG's own synthetic seed, from
        // ensureReplicationConfig, carries no `precommit` and is already
        // invisible to the commit enumeration.)
        meta.seedWatermark,
        // The transaction *at* a store's minimum is a catchup boundary, not
        // output that store can serve, so comparison starts above it.
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
          // The watermark only. The payloads are customer data and are never
          // logged; the watermark is enough to find them.
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
        // `from` was computed at or above the log's own minimum, so the log
        // cannot serve a boundary it should hold — unless the bounds moved
        // after they were pinned, which is a purge race, not a finding.
        const outcome = await this.#reconfirm(db, pinned, from);
        record(from, outcome);
        if (outcome === 'inconclusive') {
          // The pinned bounds moved; nothing this cycle established about the
          // range still holds. The cursor stays put and the next cycle re-pins.
          return this.#compared(from, from, transactions, sampled, outcomes);
        }
        // A *confirmed* mismatch is a finding about the boundary itself —
        // typically a commit this store never held, already reported as
        // missing by the cycle that enumerated it. The reads below are
        // positional and never dereference the boundary row, so the cycle
        // still compares the range and advances the cursor past it. Returning
        // here instead would re-report the same boundary — and compare
        // nothing else — every cycle, forever.
      }

      const limit = Math.max(
        1,
        this.#opts.maxTransactionsPerCycle ??
          DEFAULT_MAX_TRANSACTIONS_PER_CYCLE,
      );
      const union = await this.#enumerateCommits(db, from, through, limit);
      const maxRowsPerSource = Math.max(
        1,
        this.#opts.maxRowsPerSourcePerCycle ??
          DEFAULT_MAX_ROWS_PER_SOURCE_PER_CYCLE,
      );

      let sqliteRowsRead = 0;
      let pgRowsRead = 0;
      let previousBoundary = from;
      let handledThrough = from;
      for (const {watermark, inSqlite, inPg} of union) {
        if (this.#stopped) {
          break;
        }
        let stopAfterTransaction = false;
        if (!inSqlite || !inPg) {
          // Presence needs no payload read, so it is checked for every
          // transaction in the range, not just the sampled ones — the only
          // signal this comparison has over the unsampled majority.
          record(watermark, await this.#reconfirm(db, pinned, watermark));
        } else if (
          isSampledForCompare(this.#shard, watermark, this.#opts.comparePercent)
        ) {
          const sqliteRowsRemaining = maxRowsPerSource - sqliteRowsRead;
          const pgRowsRemaining = maxRowsPerSource - pgRowsRead;
          if (sqliteRowsRemaining === 0 || pgRowsRemaining === 0) {
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
            maxRowsPerSource,
          );
          sqliteRowsRead += comparison.sqliteRowsRead;
          pgRowsRead += comparison.pgRowsRead;
          record(watermark, comparison.outcome);
          if (comparison.outcome === 'deferred') {
            break;
          }
          if (comparison.outcome === 'oversized') {
            // The rows it did read came out of this cycle's budget, and it can
            // never fit one; count it handled so the cursor moves past it.
            stopAfterTransaction = true;
          }
          // Let fan-out, catchup, and the stream loop make progress between
          // sampled reads.
          await this.#yield();
        }
        previousBoundary = watermark;
        handledThrough = watermark;
        transactions++;
        if (stopAfterTransaction) {
          break;
        }
      }

      if (!this.#stopped && outcomes.inconclusive === 0) {
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

  /**
   * Eligibility and the pinned bounds, in one pass: schema v2, identity
   * matches (a leftover log from another replica is never compared), the log
   * is warm (§6.6), and both stores have content.
   */
  async #pinBounds(db: Database): Promise<PinnedBounds | CompareSkipReason> {
    let meta: ChangeLogMeta;
    try {
      meta = readChangeLogMeta(db);
    } catch {
      // Tables or the meta row do not exist yet: the writer has not
      // reconciled the log.
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
      // Only a completely new replica has an empty PG change log.
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
   * Enumerates the committed transactions in `(from, through]` from both
   * stores and merges them by watermark. When either store's enumeration hits
   * the per-cycle limit, `through` is lowered to the highest watermark both
   * enumerations completely cover, so a truncated cycle still compares a
   * complete common range and the next cycle resumes above it.
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
   * Runs both catchup paths over `(previousBoundary, watermark]` — one
   * transaction plus anything else either store serves in the gap, since
   * `previousBoundary` is the immediately preceding committed watermark in
   * either store — and compares one rolling digest of each store's output.
   *
   * Both reads are positional (`watermark > previousBoundary`), so neither
   * dereferences the boundary row and a boundary only one store holds is not
   * itself a finding here.
   */
  async #compareTransaction(
    db: Database,
    reader: SQLiteChangeLogReader,
    pinned: PinnedBounds,
    previousBoundary: string,
    watermark: string,
    sqliteMaxRows: number,
    pgMaxRows: number,
    maxRowsPerSource: number,
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
      );
      pg = await digestCatchupRange(
        reconstructBatches(
          this.#pg.readCatchupRange(previousBoundary, watermark, pgMaxRows),
        ),
        watermark,
        pgMaxRows,
      );
    } catch (e) {
      // A read that cannot complete is a read a subscriber could not be
      // served either, so it is a suspect like any other.
      this.#lc.warn?.(
        `error reading transaction ${watermark} for comparison`,
        e,
      );
      return {
        outcome: await this.#reconfirm(db, pinned, watermark),
        sqliteRowsRead: 0,
        pgRowsRead: 0,
      };
    }
    const rowsRead = {sqliteRowsRead: sqlite.rows, pgRowsRead: pg.rows};
    if (sqlite.limitReached || pg.limitReached) {
      // A transaction that does not fit a *fresh* budget never will; one that
      // only failed to fit what was left is retried by the next cycle.
      const outcome =
        (sqlite.limitReached && sqliteMaxRows === maxRowsPerSource) ||
        (pg.limitReached && pgMaxRows === maxRowsPerSource)
          ? 'oversized'
          : 'deferred';
      return {outcome, ...rowsRead};
    }
    if (sqlite.digest === pg.digest) {
      return {outcome: 'match', ...rowsRead};
    }
    return {
      outcome: await this.#reconfirm(db, pinned, watermark),
      ...rowsRead,
    };
  }

  /**
   * Decides whether a suspect observation is a finding or a race. The bounds
   * are re-read from both stores: if the common lower bound has moved to or
   * past the watermark, a head has fallen below it, or the log was reseeded
   * since the cycle pinned them, the observation is `inconclusive` — a purge,
   * truncation, or reseed raced the comparison — and is not reported as
   * divergence. Anything that fails to re-read is likewise inconclusive.
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
      // Already logged with the thrown error.
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

/** Reconstructs PG catchup batches exactly as the PG catchup path does. */
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
