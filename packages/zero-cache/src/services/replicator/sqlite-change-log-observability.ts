/**
 * Observability for the SQLite change log.
 *
 * Everything here now runs in the **change-streamer** process, which is where
 * the writer lives. The module stays beside the rest of the change-log code
 * under `services/replicator/` for the same reason `change-log-db.ts` and
 * `change-log-stream-writer.ts` do: moving files would obscure the topology
 * diff, and the writer's home is a property of who calls it rather than of
 * where it sits. Metric names are unchanged from 7H — only their emitting
 * process, and therefore their resource attributes, moved.
 */

import {stat} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../../shared/src/asserts.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
  getOrCreateLatencyHistogram,
  getOrCreateValueHistogram,
} from '../../observability/metrics.ts';
import {versionFromLexi} from '../../types/lexi-version.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {extractChangeSubstring} from '../change-streamer/change-log-codec.ts';
import {ChangeLogTransactionHasher} from '../change-streamer/change-log-transaction-hash.ts';
import {
  readCookieRowCounts,
  type CookieOpTag,
  type CookieRowCounts,
} from './change-log-cookies.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  readChangeLogBounds,
  readChangeLogMeta,
  type ChangeLogMeta,
  type ReconcileResult,
} from './change-log-db.ts';
import type {ChangeLogStreamTransactionStats} from './change-log-stream-writer.ts';

/**
 * Reports the reconciliation of the change-log database against the watermark
 * the stream connection resumes from. This runs once per stream connection, so
 * a reconnect storm is visible here as well as a restart.
 *
 * A wipe is not an error — it is how every abnormal change-log state is
 * collapsed into a correct one — but each wipe costs the retention window, so
 * every reconnecting subscriber below the head gets `too-old`. The `gap` reason
 * in particular means the log neither held the resume watermark nor could be
 * truncated to it, so a nonzero rate in steady state is an alert, not a
 * statistic.
 */
export function recordSQLiteChangeLogReconcile(
  lc: LogContext,
  result: ReconcileResult,
): void {
  switch (result.action) {
    case 'truncated':
      getOrCreateCounter(
        'replica',
        'sqlite_change_log.reconcile_truncated_rows',
        'Phantom change-log rows removed at reconciliation, i.e. rows above ' +
          'the watermark the change stream resumed from.',
      ).add(result.rows);
      break;
    case 'reseeded':
      getOrCreateCounter(
        'replica',
        'sqlite_change_log.reconcile_wipes',
        'Change-log wipes at reconciliation, by reason. Each one resets the ' +
          'retention window, so subscribers below the head get `too-old`.',
      ).add(1, {reason: result.reason});
      break;
    case 'none':
      break;
    default:
      unreachable(result);
  }
  if (result.cookiesStale) {
    // The invariant-17 counter: after any reconcile that moved the head, the
    // log's cookie set has to be replaced with the one that belongs to the
    // resume watermark. A nonzero `truncated` series in steady state means
    // phantom truncation is happening on a shard where it should not.
    getOrCreateCounter(
      'replica',
      'sqlite_change_log.cookie_reseed',
      'Change-log cookie sets invalidated at reconciliation, by the ' +
        'reconcile action that invalidated them.',
    ).add(1, {action: result.action});
  }
  lc.debug?.('SQLite change-log reconciliation', {
    sqliteChangeLogReconcile: result,
  });
}

/**
 * What the log says about itself. Nothing here reads the replica: after the
 * writer moved to the change-streamer, the replica's state version is a
 * consequence of this log rather than a fact about it.
 */
export type SQLiteChangeLogStartupInfo = ChangeLogMeta & {
  readonly minWatermark: string;
  readonly headWatermark: string;
};

export type SQLiteChangeLogInfo = SQLiteChangeLogStartupInfo & {
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly cookieRows: CookieRowCounts;
};

/**
 * Reads the meta row and covered range. Unlike {@link getSQLiteChangeLogInfo}
 * this does not scan the log; see {@link readChangeLogBounds}.
 *
 * Call this after reconciliation, which is the only point at which the head is
 * known to be the resume watermark.
 */
export function getSQLiteChangeLogStartupInfo(
  changeLog: Database,
): SQLiteChangeLogStartupInfo {
  const meta = readChangeLogMeta(changeLog);
  const {minWatermark, headWatermark} = readChangeLogBounds(changeLog);
  if (minWatermark === null || headWatermark === null) {
    throw new Error('the SQLite change log must contain its seed transaction');
  }
  return {...meta, minWatermark, headWatermark};
}

/**
 * Extends {@link getSQLiteChangeLogStartupInfo} with the retained row and
 * estimated byte totals. Computing these scans the entire change-log table, so
 * only call it when the totals are consumed, i.e. when the writer (and thus the
 * observer) is enabled.
 */
export function getSQLiteChangeLogInfo(
  changeLog: Database,
): SQLiteChangeLogInfo {
  const startup = getSQLiteChangeLogStartupInfo(changeLog);
  const aggregate = changeLog
    .prepare(/*sql*/ `
      SELECT count(*) AS "rows",
             coalesce(sum("estimatedBytes"), 0) AS "estimatedBytes"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<{rows: number; estimatedBytes: number}>();

  return {
    ...startup,
    rows: aggregate.rows,
    estimatedBytes: aggregate.estimatedBytes,
    cookieRows: readCookieRowCounts(changeLog),
  };
}

export function logSQLiteChangeLogStartup(
  lc: LogContext,
  file: string,
  info: SQLiteChangeLogStartupInfo,
): void {
  lc.info?.('SQLite change-log startup', {
    sqliteChangeLog: {
      file,
      epoch: info.epoch,
      generation: info.generation,
      replicaID: info.replicaID,
      schemaVersion: info.schemaVersion,
      seedWatermark: info.seedWatermark,
      seededAtMs: info.seededAtMs,
      minWatermark: info.minWatermark,
      headWatermark: info.headWatermark,
    },
  });
}

/**
 * The on-disk footprint of a SQLite database: its main file plus its `-wal` and
 * `-wal2` sidecars, whichever exist. `-shm` is excluded — it is a
 * shared-memory index rather than retained bytes.
 *
 * A missing file counts as zero rather than warning. Which sidecars exist
 * depends on the journal mode, and the change-log database is absent entirely
 * until the writer creates it.
 */
export async function sqliteFileBytes(
  lc: LogContext,
  file: string,
): Promise<number> {
  const sizes = await Promise.all(
    [file, `${file}-wal`, `${file}-wal2`].map(async path => {
      try {
        return (await stat(path)).size;
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== 'ENOENT') {
          lc.warn?.(`unable to stat ${path} for size metrics`, e);
        }
        return 0;
      }
    }),
  );
  return sizes.reduce((total, size) => total + size, 0);
}

/** What the writer reports back for a committed transaction. */
export type ChangeLogCommit = {
  readonly watermark: string;
  readonly stats: ChangeLogStreamTransactionStats;
  /**
   * Appending the commit row and committing the log's transaction. This is the
   * cost that sits on the forward path, once per upstream transaction.
   */
  readonly logCommitMs: number;
};

export type SQLiteChangeLogObservabilityState = {
  readonly receivedHead: string;
  readonly sqliteHead: string;
  readonly headLag: number | undefined;
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly cookieRows: CookieRowCounts;
  readonly cookieMutations: number;
  readonly rollbacks: number;
  readonly invariantFailures: number;
  readonly hashMatches: number;
  readonly hashMismatches: number;
  readonly hashUnpaired: number;
};

const TRANSACTION_ROW_BUCKETS = [2, 3, 5, 10, 25, 50, 100, 250, 1000, 5000];

/**
 * Records writer metrics in the change-streamer process. State changes only
 * after the corresponding write succeeds, except receivedHead, which advances
 * when a commit message arrives so that transient lag behind the log is
 * observable.
 */
export class SQLiteChangeLogObserver {
  readonly #lc: LogContext;
  readonly #messageProcessing = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.message_processing_duration',
    'Time to write a replication message to the SQLite change log.',
  );
  // The commit that sits on the forward path. There is no second commit to
  // order against any more: the replica's belongs to a subscriber downstream of
  // this loop, so `sqlite_change_log.replica_commit_duration` and the phantom
  // window it measured are gone. A failed commit is still timed, by
  // `message_processing_duration` keyed `tag="commit"`.
  readonly #logCommitDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.log_commit_duration',
    'Time to append the commit row to the SQLite change log and commit its ' +
      'transaction. This is on the forward path: it precedes the forward of ' +
      "that transaction's commit message.",
  );
  readonly #transactionRows = getOrCreateValueHistogram(
    'replica',
    'sqlite_change_log.transaction_rows',
    {
      description: 'Rows stored per committed SQLite change-log transaction.',
      unit: '{row}',
      bucketBoundaries: TRANSACTION_ROW_BUCKETS,
    },
  );
  readonly #rollbackCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.rollbacks',
    'SQLite change-log transactions rolled back before commit.',
  );
  readonly #invariantFailureCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.invariant_failures',
    'Detected SQLite change-log writer invariant failures.',
  );
  readonly #hashComparisonCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.hash_comparisons',
    'Ephemeral transaction hash comparisons between the received change stream and SQLite change-log writes.',
  );
  readonly #hashUnpairedCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.hash_unpaired',
    'Committed change-log transactions without both ephemeral hashes available for comparison.',
  );
  // Near zero at steady state: cookie state only moves on schema changes.
  readonly #cookieMutationCounter = getOrCreateCounter(
    'replica',
    'sqlite_change_log.cookie_mutations',
    'Change-log cookie mutations folded from schema changes, by operation.',
  );

  #receivedHead: string;
  #sqliteHead: string;
  #rows: number;
  #estimatedBytes: number;
  #cookieRows: CookieRowCounts;
  #cookieMutations = 0;
  #transactionWatermark: string | undefined;
  #sourceHasher: ChangeLogTransactionHasher | undefined;
  #sourcePos = 0;
  #sourceCommitHash: string | undefined;
  #rollbacks = 0;
  #invariantFailures = 0;
  #hashMatches = 0;
  #hashMismatches = 0;
  #hashUnpaired = 0;

  constructor(lc: LogContext, info: SQLiteChangeLogInfo) {
    this.#lc = lc.withContext('component', 'sqlite-change-log-observer');
    // Reconciliation has just put the head at the resume watermark, which is
    // also the point the stream is about to be received from.
    this.#receivedHead = info.headWatermark;
    this.#sqliteHead = info.headWatermark;
    this.#rows = info.rows;
    this.#estimatedBytes = info.estimatedBytes;
    this.#cookieRows = info.cookieRows;

    getOrCreateGauge(
      'replica',
      'sqlite_change_log.rows',
      'Rows retained in the SQLite change log.',
    ).addCallback(result => result.observe(this.#rows));
    // Nonzero with no backfill in flight is a leaked cookie, i.e. a backfill
    // that will be re-requested forever.
    getOrCreateGauge(
      'replica',
      'sqlite_change_log.cookie_rows',
      'Cookie rows retained in the SQLite change log, by cookie table.',
    ).addCallback(result => {
      result.observe(this.#cookieRows.tableMetadata, {table: 'tableMetadata'});
      result.observe(this.#cookieRows.backfilling, {table: 'backfilling'});
    });
    getOrCreateGauge('replica', 'sqlite_change_log.retained_bytes', {
      description:
        'Estimated UTF-8 payload bytes retained in the SQLite change log.',
      unit: 'By',
    }).addCallback(result => result.observe(this.#estimatedBytes));
    getOrCreateGauge(
      'replica',
      'sqlite_change_log.head',
      'SQLite change-log head converted from its lexicographic watermark.',
    ).addCallback(result => {
      const head = watermarkValue(this.#sqliteHead);
      if (head !== undefined) {
        result.observe(head);
      }
    });
    getOrCreateGauge(
      'replica',
      'sqlite_change_log.head_lag',
      'Distance from the latest received commit to the SQLite change-log ' +
        'head. The log commits before the forward, so this is zero except ' +
        'within a transaction.',
    ).addCallback(result => {
      const lag = watermarkDistance(this.#receivedHead, this.#sqliteHead);
      if (lag !== undefined) {
        result.observe(lag);
      }
    });
  }

  /**
   * Called as each message arrives, before it is written. Hashes the *received*
   * stream, which is compared against the hash the writer computes over what it
   * *stored*.
   */
  messageReceived(data: ChangeStreamData, json: string): void {
    const tag = data[1].tag;
    if (data[0] === 'begin') {
      if (this.#sourceHasher !== undefined) {
        this.#invariantFailure(
          'Received change-log begin while a source hash is already open',
          {receivedWatermark: data[2].commitWatermark},
        );
      }
      const watermark = data[2].commitWatermark;
      this.#sourceHasher = new ChangeLogTransactionHasher();
      this.#sourcePos = 0;
      this.#sourceCommitHash = undefined;
      this.#sourceHasher.add({
        watermark,
        pos: this.#sourcePos,
        tag,
        change: extractChangeSubstring(json, tag),
        precommit: null,
      });
      return;
    }

    if (tag === 'rollback') {
      this.#resetSourceHash();
      return;
    }

    if (data[0] === 'commit') {
      this.#receivedHead = data[2].watermark;
    }

    const watermark = this.#transactionWatermark;
    if (this.#sourceHasher === undefined || watermark === undefined) {
      this.#invariantFailure(
        'Received change-log message without an observed transaction',
        {tag},
      );
      return;
    }

    const commitWatermark =
      data[0] === 'commit' ? data[2].watermark : watermark;
    this.#sourceHasher.add({
      watermark: commitWatermark,
      pos: ++this.#sourcePos,
      tag,
      change: extractChangeSubstring(json, tag),
      precommit: data[0] === 'commit' ? watermark : null,
    });
    if (data[0] === 'commit') {
      this.#sourceCommitHash = this.#sourceHasher.digest();
      this.#sourceHasher = undefined;
    }
  }

  /** Called after the message has been written to the log. */
  messageProcessed(
    data: ChangeStreamData,
    commit: ChangeLogCommit | null,
    durationMs: number,
  ): void {
    const tag = data[1].tag;
    this.#messageProcessing.recordMs(durationMs, {tag, outcome: 'success'});

    if (data[0] === 'begin') {
      if (this.#transactionWatermark !== undefined) {
        this.#invariantFailure('SQLite change-log transaction already open', {
          openWatermark: this.#transactionWatermark,
          receivedWatermark: data[2].commitWatermark,
        });
      }
      this.#transactionWatermark = data[2].commitWatermark;
      return;
    }

    if (tag === 'rollback') {
      this.#recordRollback('upstream');
      this.#transactionWatermark = undefined;
      this.#resetSourceHash();
      return;
    }

    if (data[0] !== 'commit') {
      return;
    }

    const watermark = data[2].watermark;
    if (commit !== null) {
      this.#logCommitDuration.recordMs(commit.logCommitMs);
    }
    if (this.#transactionWatermark !== watermark) {
      this.#invariantFailure(
        'SQLite change-log commit does not match the observed begin',
        {openWatermark: this.#transactionWatermark, commitWatermark: watermark},
      );
    }
    if (commit === null) {
      this.#invariantFailure(
        'SQLite change-log commit result is missing writer statistics',
        {commitWatermark: watermark},
      );
      this.#recordUnpaired(watermark, 'missing-sqlite-hash');
    } else {
      if (commit.watermark !== watermark) {
        this.#invariantFailure(
          'SQLite change-log commit result has an unexpected watermark',
          {commitWatermark: watermark, resultWatermark: commit.watermark},
        );
      }
      this.#rows += commit.stats.rows;
      this.#estimatedBytes += commit.stats.estimatedBytes;
      this.#transactionRows.record(commit.stats.rows);
      this.#compareHashes(watermark, this.#sourceCommitHash, commit.stats.hash);
    }
    if (watermark < this.#sqliteHead) {
      this.#invariantFailure('SQLite change-log head regressed', {
        previousHead: this.#sqliteHead,
        commitWatermark: watermark,
      });
    }
    this.#sqliteHead = watermark;
    this.#transactionWatermark = undefined;
    this.#resetSourceHash();
  }

  messageFailed(
    data: ChangeStreamData,
    error: unknown,
    durationMs: number,
  ): void {
    const tag = data[1].tag;
    this.#messageProcessing.recordMs(durationMs, {tag, outcome: 'error'});
    if (data[0] === 'commit') {
      // No commit duration here: a failed commit produces no statistics, so
      // there is nothing to attribute. The attempt is timed by
      // `message_processing_duration{tag="commit",outcome="error"}` above.
      this.#recordUnpaired(data[2].watermark, 'sqlite-commit-failed');
    }
    if (
      error instanceof Error &&
      error.name === 'ChangeLogStreamInvariantError'
    ) {
      this.#invariantFailure(error.message);
    }
    if (this.#transactionWatermark !== undefined || data[0] === 'begin') {
      this.#recordRollback('processing-error');
    }
    this.#transactionWatermark = undefined;
    this.#resetSourceHash();
  }

  /**
   * A committed transaction folded schema changes into the cookie jar. Reported
   * separately from {@link messageProcessed} because it costs a read of the
   * cookie tables, which is only worth taking when they actually moved.
   */
  cookiesMutated(ops: readonly CookieOpTag[], rows: CookieRowCounts): void {
    for (const op of ops) {
      this.#cookieMutations++;
      this.#cookieMutationCounter.add(1, {op});
    }
    this.#cookieRows = rows;
  }

  /**
   * A missed truncate-above surfaces as a constraint violation on the writer's
   * plain `INSERT`, which the fail-soft policy would otherwise absorb quietly.
   * Classifying it as an invariant failure is what keeps a reconciliation bug
   * from looking like a lost cache.
   */
  invariantFailure(message: string, details?: Record<string, unknown>): void {
    this.#invariantFailure(message, details);
  }

  /** The source interrupted the stream mid-transaction. */
  abort(): void {
    if (this.#transactionWatermark !== undefined) {
      this.#recordRollback('source-interruption');
      this.#transactionWatermark = undefined;
    }
    this.#resetSourceHash();
  }

  /** Reconciliation moved the head, so the observer's totals are restated. */
  reconciled(info: SQLiteChangeLogInfo): void {
    this.#receivedHead = info.headWatermark;
    this.#sqliteHead = info.headWatermark;
    this.#rows = info.rows;
    this.#estimatedBytes = info.estimatedBytes;
    this.#cookieRows = info.cookieRows;
    this.#transactionWatermark = undefined;
    this.#resetSourceHash();
  }

  state(): SQLiteChangeLogObservabilityState {
    return {
      receivedHead: this.#receivedHead,
      sqliteHead: this.#sqliteHead,
      headLag: watermarkDistance(this.#receivedHead, this.#sqliteHead),
      rows: this.#rows,
      estimatedBytes: this.#estimatedBytes,
      cookieRows: this.#cookieRows,
      cookieMutations: this.#cookieMutations,
      rollbacks: this.#rollbacks,
      invariantFailures: this.#invariantFailures,
      hashMatches: this.#hashMatches,
      hashMismatches: this.#hashMismatches,
      hashUnpaired: this.#hashUnpaired,
    };
  }

  #recordRollback(
    reason: 'upstream' | 'processing-error' | 'source-interruption',
  ) {
    this.#rollbacks++;
    this.#rollbackCounter.add(1, {reason});
  }

  #invariantFailure(message: string, details?: Record<string, unknown>) {
    this.#invariantFailures++;
    this.#invariantFailureCounter.add(1);
    this.#lc.error?.(message, details);
  }

  #compareHashes(
    watermark: string,
    sourceHash: string | undefined,
    sqliteHash: string,
  ): void {
    if (sourceHash === undefined) {
      this.#recordUnpaired(watermark, 'missing-source-hash');
      return;
    }
    if (sourceHash === sqliteHash) {
      this.#hashMatches++;
      this.#hashComparisonCounter.add(1, {outcome: 'match'});
      return;
    }
    this.#hashMismatches++;
    this.#hashComparisonCounter.add(1, {outcome: 'mismatch'});
    this.#lc.error?.('SQLite change-log transaction hash mismatch', {
      sqliteChangeLogHashComparison: {
        watermark,
        sourceHash: sourceHash.slice(0, 16),
        sqliteHash: sqliteHash.slice(0, 16),
      },
    });
  }

  #recordUnpaired(
    watermark: string,
    reason:
      | 'missing-source-hash'
      | 'missing-sqlite-hash'
      | 'sqlite-commit-failed',
  ): void {
    this.#hashUnpaired++;
    this.#hashUnpairedCounter.add(1, {reason});
    this.#lc.warn?.('SQLite change-log transaction hash was not compared', {
      sqliteChangeLogHashComparison: {watermark, reason},
    });
  }

  #resetSourceHash(): void {
    this.#sourceHasher = undefined;
    this.#sourcePos = 0;
    this.#sourceCommitHash = undefined;
  }
}

function watermarkValue(watermark: string): number | undefined {
  try {
    const value = Number(versionFromLexi(watermark));
    return Number.isFinite(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

function watermarkDistance(
  receivedHead: string,
  sqliteHead: string,
): number | undefined {
  if (receivedHead <= sqliteHead) {
    return 0;
  }
  try {
    const distance =
      versionFromLexi(receivedHead) - versionFromLexi(sqliteHead);
    const value = Number(distance);
    return Number.isFinite(value) ? value : undefined;
  } catch {
    return undefined;
  }
}
