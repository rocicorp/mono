import {stat} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
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
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  type ReconcileResult,
} from './change-log-db.ts';
import type {CommitResult} from './change-processor.ts';

/**
 * Reports the startup reconciliation of the change-log database against the
 * replica.
 *
 * A wipe is not an error — it is how every abnormal change-log state is
 * collapsed into a correct one — but each wipe costs the retention window, so
 * every reconnecting subscriber below the replica head gets `too-old`. The
 * `gap` reason in particular means log-first commit ordering did not hold, so a
 * nonzero rate in steady state is an alert, not a statistic.
 *
 * Called from the replicator process rather than the write worker, which does
 * not start OTel.
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
        'Phantom change-log rows removed at startup, i.e. logged transactions ' +
          'whose replica commit was lost to a crash.',
      ).add(result.rows);
      break;
    case 'reseeded':
      getOrCreateCounter(
        'replica',
        'sqlite_change_log.reconcile_wipes',
        'Change-log wipes at startup, by reason. Each one resets the retention ' +
          'window, so subscribers below the replica head get `too-old`.',
      ).add(1, {reason: result.reason});
      break;
    case 'none':
      break;
    default:
      unreachable(result);
  }
  lc.debug?.('SQLite change-log reconciliation', {
    sqliteChangeLogReconcile: result,
  });
}

/**
 * The replica-side half of the startup state. The change log no longer lives in
 * the replica, so these are the only change-log-relevant facts a replicator can
 * report without opening the change-log database.
 */
export type ReplicaChangeLogInfo = {
  readonly stateWatermark: string;
  readonly seedWatermark: string;
};

export type SQLiteChangeLogStartupInfo = ReplicaChangeLogInfo & {
  /**
   * The change-log database's own schema version, not the replica's. The two
   * are versioned independently: the log is a cache with no migrations, so a
   * mismatch costs it a reseed.
   */
  readonly schemaVersion: number;
  readonly headWatermark: string;
};

export type SQLiteChangeLogInfo = SQLiteChangeLogStartupInfo & {
  readonly rows: number;
  readonly estimatedBytes: number;
};

/**
 * Reads the replica's replication state. One single-row lookup, and no
 * change-log database required, so this is what a replicator with the writer
 * disabled reports.
 */
export function getReplicaChangeLogInfo(
  replica: Database,
): ReplicaChangeLogInfo {
  const state = replica
    .prepare(/*sql*/ `
      SELECT state."stateVersion", config."replicaVersion"
        FROM "_zero.replicationState" AS state,
             "_zero.replicationConfig" AS config
    `)
    .get<{stateVersion: string; replicaVersion: string} | undefined>();

  assert(state !== undefined, 'replication state must be initialized');
  return {
    stateWatermark: state.stateVersion,
    seedWatermark: state.replicaVersion,
  };
}

/**
 * Adds the change log's own schema version and head, read from the change-log
 * database. The head is an index-optimized `max()`, so unlike
 * {@link getSQLiteChangeLogInfo} this does not scan the log.
 *
 * Both handles are passed in rather than deriving one from the other: state
 * comes from the replica and everything else from the log, and holding them
 * apart is what lets this cross-check the two.
 *
 * Call this after the writer has reconciled the log against the replica, which
 * is the only point at which `headWatermark === stateWatermark` is guaranteed.
 */
export function getSQLiteChangeLogStartupInfo(
  replica: Database,
  changeLog: Database,
): SQLiteChangeLogStartupInfo {
  const replicaInfo = getReplicaChangeLogInfo(replica);
  const meta = changeLog
    .prepare(/*sql*/ `
      SELECT "schemaVersion", "replicaVersion" FROM "${CHANGE_LOG_META_TABLE}"
    `)
    .get<{schemaVersion: number; replicaVersion: string} | undefined>();
  assert(meta !== undefined, 'SQLite change log must have a meta row');
  // Reconciliation wipes the log on any disagreement, so a log that survives it
  // is anchored to this replica. Checking it here is the cheapest place to
  // notice a log that was served without being reconciled.
  assert(
    meta.replicaVersion === replicaInfo.seedWatermark,
    `SQLite change log is anchored to replica version ${meta.replicaVersion}, ` +
      `not ${replicaInfo.seedWatermark}`,
  );
  const head = changeLog
    .prepare(/*sql*/ `
      SELECT max("watermark") AS "headWatermark"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<{headWatermark: string | null}>();
  assert(
    head.headWatermark !== null,
    'SQLite change log must contain its seed transaction',
  );
  return {
    ...replicaInfo,
    schemaVersion: meta.schemaVersion,
    headWatermark: head.headWatermark,
  };
}

/**
 * Extends {@link getSQLiteChangeLogStartupInfo} with the retained row and
 * estimated byte totals. Computing these scans the entire change-log table, so
 * only call this when the totals are consumed, i.e. when the writer (and thus
 * the observer) is enabled.
 */
export function getSQLiteChangeLogInfo(
  replica: Database,
  changeLog: Database,
): SQLiteChangeLogInfo {
  const startup = getSQLiteChangeLogStartupInfo(replica, changeLog);
  const aggregate = changeLog
    .prepare(/*sql*/ `
      SELECT count(*) AS "rows",
             coalesce(sum(
               length(CAST("watermark" AS BLOB)) +
               8 +
               length(CAST("change" AS BLOB)) +
               coalesce(length(CAST("precommit" AS BLOB)), 0) +
               CASE WHEN "writeTimeMs" IS NULL THEN 0 ELSE 8 END
             ), 0) AS "estimatedBytes"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<{rows: number; estimatedBytes: number}>();

  return {
    ...startup,
    rows: aggregate.rows,
    estimatedBytes: aggregate.estimatedBytes,
  };
}

export function logSQLiteChangeLogStartup(
  lc: LogContext,
  fileMode: 'serving' | 'serving-copy' | 'backup',
  writerEnabled: boolean,
  info: ReplicaChangeLogInfo & {
    readonly schemaVersion?: number | undefined;
    readonly headWatermark?: string | undefined;
  },
): void {
  lc.info?.('SQLite change-log startup', {
    sqliteChangeLog: {
      fileMode,
      writerEnabled,
      // Both absent when the writer is disabled: there is no change-log
      // database to read a schema version or a head from, and the replica no
      // longer carries a copy.
      schemaVersion: info.schemaVersion,
      seedWatermark: info.seedWatermark,
      headWatermark: info.headWatermark,
      stateWatermark: info.stateWatermark,
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

export type SQLiteChangeLogObservabilityState = {
  readonly receivedHead: string;
  readonly sqliteHead: string;
  readonly headLag: number | undefined;
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly rollbacks: number;
  readonly invariantFailures: number;
  readonly hashMatches: number;
  readonly hashMismatches: number;
  readonly hashUnpaired: number;
};

const TRANSACTION_ROW_BUCKETS = [2, 3, 5, 10, 25, 50, 100, 250, 1000, 5000];

/**
 * Records shadow-writer metrics in the replicator process, where OTel is
 * initialized. State changes only after the corresponding worker operation
 * succeeds, except receivedHead, which intentionally advances before a commit
 * is sent to the worker so transient shadow lag is observable.
 */
export class SQLiteChangeLogObserver {
  readonly #lc: LogContext;
  readonly #messageProcessing = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.message_processing_duration',
    'Time to process a replication message while SQLite change logging is enabled.',
  );
  // There is no longer one commit to time. The log and the replica are
  // separate databases committed in a fixed order, so these are the two halves
  // of what `sqlite_change_log.commit_duration` used to fuse, and the second is
  // the phantom window. Both are recorded from the write worker's own
  // measurements, so neither includes the IPC round trip that
  // `message_processing_duration` covers; that metric, keyed `tag="commit"`,
  // is also where a failed commit is still timed.
  readonly #logCommitDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.log_commit_duration',
    'Time to append the commit row to the SQLite change log and commit its ' +
      'transaction.',
  );
  readonly #replicaCommitDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.replica_commit_duration',
    'Time from the SQLite change-log commit returning to the replica commit ' +
      'returning. This is the phantom window: a crash within it leaves the ' +
      'log ahead of the replica, to be truncated at startup.',
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

  #receivedHead: string;
  #sqliteHead: string;
  #rows: number;
  #estimatedBytes: number;
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
    this.#receivedHead = info.stateWatermark;
    this.#sqliteHead = info.headWatermark;
    this.#rows = info.rows;
    this.#estimatedBytes = info.estimatedBytes;

    getOrCreateGauge(
      'replica',
      'sqlite_change_log.rows',
      'Rows retained in the SQLite change log.',
    ).addCallback(result => result.observe(this.#rows));
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
      'Distance from the latest received PG commit to the SQLite change-log head.',
    ).addCallback(result => {
      const lag = watermarkDistance(this.#receivedHead, this.#sqliteHead);
      if (lag !== undefined) {
        result.observe(lag);
      }
    });

    if (info.headWatermark > info.stateWatermark) {
      this.#invariantFailure(
        'SQLite change-log head is ahead of replica state',
        {
          sqliteHead: info.headWatermark,
          stateWatermark: info.stateWatermark,
        },
      );
    }
  }

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

  messageProcessed(
    data: ChangeStreamData,
    result: CommitResult | null,
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

    const timing = result?.commitTiming;
    if (timing !== undefined) {
      this.#logCommitDuration.recordMs(timing.logCommitMs);
      this.#replicaCommitDuration.recordMs(timing.replicaCommitMs);
    }
    const watermark = data[2].watermark;
    if (this.#transactionWatermark !== watermark) {
      this.#invariantFailure(
        'SQLite change-log commit does not match the observed begin',
        {openWatermark: this.#transactionWatermark, commitWatermark: watermark},
      );
    }
    if (result?.watermark !== watermark) {
      this.#invariantFailure(
        'SQLite change-log commit result has an unexpected watermark',
        {commitWatermark: watermark, resultWatermark: result?.watermark},
      );
    }
    if (result?.changeLogStream === undefined) {
      this.#invariantFailure(
        'SQLite change-log commit result is missing writer statistics',
        {commitWatermark: watermark},
      );
      this.#recordUnpaired(watermark, 'missing-sqlite-hash');
    } else {
      this.#rows += result.changeLogStream.rows;
      this.#estimatedBytes += result.changeLogStream.estimatedBytes;
      this.#transactionRows.record(result.changeLogStream.rows);
      this.#compareHashes(
        watermark,
        this.#sourceCommitHash,
        result.changeLogStream.hash,
      );
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
      // No commit-half durations here: a failure gives no CommitResult, so
      // there is nothing to attribute to either database. The attempt is timed
      // by `message_processing_duration{tag="commit",outcome="error"}` above.
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

  abort(): void {
    if (this.#transactionWatermark !== undefined) {
      this.#recordRollback('source-interruption');
      this.#transactionWatermark = undefined;
    }
    this.#resetSourceHash();
  }

  state(): SQLiteChangeLogObservabilityState {
    return {
      receivedHead: this.#receivedHead,
      sqliteHead: this.#sqliteHead,
      headLag: watermarkDistance(this.#receivedHead, this.#sqliteHead),
      rows: this.#rows,
      estimatedBytes: this.#estimatedBytes,
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
