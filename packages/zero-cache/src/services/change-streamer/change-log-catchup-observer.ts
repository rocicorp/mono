import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
  getOrCreateValueHistogram,
} from '../../observability/metrics.ts';

export type ChangeLogCatchupSource = 'pg' | 'sqlite';

export type ChangeLogCatchupOutcome =
  | 'success'
  | 'ahead'
  | 'too-old'
  | 'reset'
  | 'barrier-timeout'
  | 'reader-error';

export type ChangeLogCatchupResult = {
  readonly source: ChangeLogCatchupSource;
  readonly outcome: ChangeLogCatchupOutcome;
  readonly rows: number;
  readonly bytes: number;
  readonly durationMs: number;
  readonly barrierWaitMs?: number | undefined;
  readonly backlogRows: number;
  readonly backlogBytes: number;
};

let catchupMetrics: ReturnType<typeof createCatchupMetrics> | undefined;

export function recordChangeLogCatchup(result: ChangeLogCatchupResult): void {
  const {
    results,
    duration,
    barrierWait,
    rows,
    bytes,
    backlogRows,
    backlogBytes,
  } = (catchupMetrics ??= createCatchupMetrics());
  const attributes = {source: result.source, outcome: result.outcome};
  results.add(1, attributes);
  duration.recordMs(result.durationMs, attributes);
  rows.record(result.rows, attributes);
  bytes.record(result.bytes, attributes);
  backlogRows.record(result.backlogRows, attributes);
  backlogBytes.record(result.backlogBytes, attributes);
  if (result.barrierWaitMs !== undefined) {
    barrierWait.recordMs(result.barrierWaitMs, attributes);
  }
}

function createCatchupMetrics() {
  return {
    results: getOrCreateCounter(
      'replication',
      'sqlite_change_log.catchup_result',
      'Change-log catchup results by selected read source.',
    ),
    duration: getOrCreateLatencyHistogram(
      'replication',
      'sqlite_change_log.catchup_duration',
      'Time spent catching a subscriber up from its selected change log.',
    ),
    barrierWait: getOrCreateLatencyHistogram(
      'replication',
      'sqlite_change_log.barrier_wait',
      'Time SQLite catchup spent waiting for the required committed head.',
    ),
    rows: getOrCreateValueHistogram(
      'replication',
      'sqlite_change_log.catchup_rows',
      {
        description: 'Rows delivered during one change-log catchup.',
        unit: '{row}',
        bucketBoundaries: [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000],
      },
    ),
    bytes: getOrCreateValueHistogram(
      'replication',
      'sqlite_change_log.catchup_bytes',
      {
        description:
          'Canonical JSON bytes delivered during one change-log catchup.',
        unit: 'By',
        bucketBoundaries: [
          0,
          1024,
          16 * 1024,
          256 * 1024,
          4 * 1024 * 1024,
          64 * 1024 * 1024,
          1024 * 1024 * 1024,
        ],
      },
    ),
    backlogRows: getOrCreateValueHistogram(
      'replication',
      'sqlite_change_log.catchup_backlog_rows',
      {
        description: 'Peak live backlog rows accumulated during one catchup.',
        unit: '{row}',
        bucketBoundaries: [0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000],
      },
    ),
    backlogBytes: getOrCreateValueHistogram(
      'replication',
      'sqlite_change_log.catchup_backlog_bytes',
      {
        description: 'Peak live backlog bytes accumulated during one catchup.',
        unit: 'By',
        bucketBoundaries: [
          0,
          1024,
          16 * 1024,
          256 * 1024,
          4 * 1024 * 1024,
          64 * 1024 * 1024,
          1024 * 1024 * 1024,
        ],
      },
    ),
  };
}
