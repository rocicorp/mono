import type {ZeroConfig} from '../../config/zero-config.ts';
import {
  getOrCreateCounter,
  getOrCreateHistogram,
} from '../../observability/metrics.ts';

export type LitestreamRole = 'replication_manager' | 'view_syncer';
export type LitestreamVersion = 'legacy' | 'v5';

export type LitestreamMetricAttrs = {
  role: LitestreamRole;
  backup_scheme: string;
  litestream: LitestreamVersion;
};

type LitestreamMultipartMetricAttrs = {
  multipart_concurrency: number;
  multipart_size_mib: number;
};

const LITESTREAM_DURATION_HISTOGRAM_BOUNDARIES_S = [
  1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 2400, 3600, 7200,
];

export function litestreamRestoreMetricAttrs(
  config: ZeroConfig,
  role: LitestreamRole,
  backupURL = config.litestream.backupURL,
): LitestreamMetricAttrs & LitestreamMultipartMetricAttrs {
  return {
    role,
    backup_scheme: litestreamBackupScheme(backupURL),
    litestream: config.litestream.restoreUsingV5 ? 'v5' : 'legacy',
    ...litestreamMultipartMetricAttrs(config),
  };
}

export function litestreamBackupMetricAttrs(
  config: ZeroConfig,
): LitestreamMetricAttrs {
  return {
    role: 'replication_manager',
    backup_scheme: litestreamBackupScheme(config.litestream.backupURL),
    litestream:
      config.litestream.executableV5 !== undefined &&
      config.litestream.executable === config.litestream.executableV5
        ? 'v5'
        : 'legacy',
  };
}

export function litestreamBackupProcessMetricAttrs(
  config: ZeroConfig,
): LitestreamMetricAttrs & LitestreamMultipartMetricAttrs {
  return {
    ...litestreamBackupMetricAttrs(config),
    ...litestreamMultipartMetricAttrs(config),
  };
}

export function litestreamMonitorMetricAttrs(
  backupURL: string,
  litestream: LitestreamVersion,
  role: LitestreamRole,
): LitestreamMetricAttrs {
  return {
    role,
    backup_scheme: litestreamBackupScheme(backupURL),
    litestream,
  };
}

function litestreamBackupScheme(backupURL: string | undefined): string {
  if (!backupURL) {
    return 'unknown';
  }
  try {
    const protocol = new URL(backupURL).protocol;
    return protocol.endsWith(':') ? protocol.slice(0, -1) : protocol;
  } catch {
    return 'unknown';
  }
}

function litestreamMultipartMetricAttrs(
  config: ZeroConfig,
): LitestreamMultipartMetricAttrs {
  return {
    multipart_concurrency: config.litestream.multipartConcurrency,
    multipart_size_mib: Math.round(
      config.litestream.multipartSize / 1024 / 1024,
    ),
  };
}

export function litestreamRestoreRuns() {
  return getOrCreateCounter(
    'replica',
    'litestream_restore_runs',
    'Litestream restore runs, labeled by result.',
  );
}

export function litestreamRestoreAttempts() {
  return getOrCreateCounter(
    'replica',
    'litestream_restore_attempts',
    'Litestream restore subprocess attempts, labeled by result.',
  );
}

export function litestreamRestoredDbBytes() {
  return getOrCreateCounter('replica', 'litestream_restored_db', {
    description:
      'SQLite database bytes restored by successful litestream restores.',
    unit: 'bytes',
  });
}

export function litestreamBackupProcessRuns() {
  return getOrCreateCounter(
    'replica',
    'litestream_backup_process_runs',
    'Litestream backup process exits, labeled by result.',
  );
}

export function litestreamRestoreDuration() {
  return litestreamDurationHistogram(
    'litestream_restore_duration',
    'Wall-clock duration of a litestream restore run, labeled by result.',
  );
}

export function litestreamRestoreWaitDuration() {
  return litestreamDurationHistogram(
    'litestream_restore_wait_duration',
    'Time spent waiting for the replication-manager snapshot status before restoring.',
  );
}

export function litestreamRestoreProcessDuration() {
  return litestreamDurationHistogram(
    'litestream_restore_process_duration',
    'Wall-clock duration of the litestream restore subprocess.',
  );
}

export function litestreamRestoreValidationDuration() {
  return litestreamDurationHistogram(
    'litestream_restore_validation_duration',
    'Time spent validating a restored replica database.',
  );
}

export function litestreamBackupProcessDuration() {
  return litestreamDurationHistogram(
    'litestream_backup_process_duration',
    'Runtime duration of the litestream backup subprocess before it exits.',
  );
}

export function litestreamBackupListDuration() {
  return litestreamDurationHistogram(
    'litestream_backup_list_duration',
    'Duration of litestream backup destination listing commands.',
  );
}

export function litestreamBackupVerificationDuration() {
  return litestreamDurationHistogram(
    'litestream_backup_verification_duration',
    'Duration of verifying the actual backup state in the backup destination.',
  );
}

export function litestreamSnapshotReservationDuration() {
  return litestreamDurationHistogram(
    'litestream_snapshot_reservation_duration',
    'Duration of a snapshot reservation while a view-syncer restores and subscribes.',
  );
}

function litestreamDurationHistogram(name: string, description: string) {
  return getOrCreateHistogram('replica', name, {
    description,
    unit: 's',
    bucketBoundaries: LITESTREAM_DURATION_HISTOGRAM_BOUNDARIES_S,
  });
}
