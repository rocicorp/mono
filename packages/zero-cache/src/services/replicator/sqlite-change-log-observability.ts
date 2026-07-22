import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {
  getOrCreateCounter,
  getOrCreateGauge,
  getOrCreateLatencyHistogram,
  getOrCreateValueHistogram,
} from '../../observability/metrics.ts';
import {versionFromLexi} from '../../types/lexi-version.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {CommitResult} from './change-processor.ts';
import {CHANGE_LOG_STREAM_TABLE} from './schema/change-log-stream.ts';

export type SQLiteChangeLogInfo = {
  readonly schemaVersion: number;
  readonly stateWatermark: string;
  readonly seedWatermark: string;
  readonly headWatermark: string;
  readonly rows: number;
  readonly estimatedBytes: number;
};

type ChangeLogAggregate = {
  readonly headWatermark: string | null;
  readonly rows: number;
  readonly estimatedBytes: number;
};

/** Reads the small set of values needed for startup logging and metrics. */
export function getSQLiteChangeLogInfo(db: Database): SQLiteChangeLogInfo {
  const version = db
    .prepare(/*sql*/ `
      SELECT "schemaVersion"
        FROM "_zero.versionHistory"
    `)
    .get<{schemaVersion: number} | undefined>();
  const state = db
    .prepare(/*sql*/ `
      SELECT state."stateVersion", config."replicaVersion"
        FROM "_zero.replicationState" AS state,
             "_zero.replicationConfig" AS config
    `)
    .get<{stateVersion: string; replicaVersion: string} | undefined>();
  const aggregate = db
    .prepare(/*sql*/ `
      SELECT max("watermark") AS "headWatermark",
             count(*) AS "rows",
             coalesce(sum(
               length(CAST("watermark" AS BLOB)) +
               8 +
               length(CAST("change" AS BLOB)) +
               coalesce(length(CAST("precommit" AS BLOB)), 0) +
               CASE WHEN "writeTimeMs" IS NULL THEN 0 ELSE 8 END
             ), 0) AS "estimatedBytes"
        FROM "${CHANGE_LOG_STREAM_TABLE}"
    `)
    .get<ChangeLogAggregate>();

  assert(version !== undefined, 'replica schema version must be initialized');
  assert(state !== undefined, 'replication state must be initialized');
  assert(
    aggregate.headWatermark !== null,
    'SQLite change log must contain its seed transaction',
  );
  return {
    schemaVersion: version.schemaVersion,
    stateWatermark: state.stateVersion,
    seedWatermark: state.replicaVersion,
    headWatermark: aggregate.headWatermark,
    rows: aggregate.rows,
    estimatedBytes: aggregate.estimatedBytes,
  };
}

export function logSQLiteChangeLogStartup(
  lc: LogContext,
  fileMode: 'serving' | 'serving-copy' | 'backup',
  writerEnabled: boolean,
  info: SQLiteChangeLogInfo,
): void {
  lc.info?.('SQLite change-log startup', {
    sqliteChangeLog: {
      fileMode,
      writerEnabled,
      schemaVersion: info.schemaVersion,
      seedWatermark: info.seedWatermark,
      headWatermark: info.headWatermark,
      stateWatermark: info.stateWatermark,
    },
  });
}

export type SQLiteChangeLogObservabilityState = {
  readonly receivedHead: string;
  readonly sqliteHead: string;
  readonly headLag: number | undefined;
  readonly rows: number;
  readonly estimatedBytes: number;
  readonly rollbacks: number;
  readonly invariantFailures: number;
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
  readonly #commitProcessing = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.commit_duration',
    'Time to atomically commit replica data and its SQLite change-log transaction.',
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

  #receivedHead: string;
  #sqliteHead: string;
  #rows: number;
  #estimatedBytes: number;
  #transactionWatermark: string | undefined;
  #rollbacks = 0;
  #invariantFailures = 0;

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

  messageReceived(data: ChangeStreamData): void {
    if (data[0] === 'commit') {
      this.#receivedHead = data[2].watermark;
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
      return;
    }

    if (data[0] !== 'commit') {
      return;
    }

    this.#commitProcessing.recordMs(durationMs, {outcome: 'success'});
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
    } else {
      this.#rows += result.changeLogStream.rows;
      this.#estimatedBytes += result.changeLogStream.estimatedBytes;
      this.#transactionRows.record(result.changeLogStream.rows);
    }
    if (watermark < this.#sqliteHead) {
      this.#invariantFailure('SQLite change-log head regressed', {
        previousHead: this.#sqliteHead,
        commitWatermark: watermark,
      });
    }
    this.#sqliteHead = watermark;
    this.#transactionWatermark = undefined;
  }

  messageFailed(
    data: ChangeStreamData,
    error: unknown,
    durationMs: number,
  ): void {
    const tag = data[1].tag;
    this.#messageProcessing.recordMs(durationMs, {tag, outcome: 'error'});
    if (data[0] === 'commit') {
      this.#commitProcessing.recordMs(durationMs, {outcome: 'error'});
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
  }

  abort(): void {
    if (this.#transactionWatermark !== undefined) {
      this.#recordRollback('source-interruption');
      this.#transactionWatermark = undefined;
    }
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
