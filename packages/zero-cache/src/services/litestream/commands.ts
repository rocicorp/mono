import type {ChildProcess} from 'node:child_process';
import {spawn} from 'node:child_process';
import {existsSync, statSync} from 'node:fs';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {must} from '../../../../shared/src/must.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {assertNormalized} from '../../config/normalize.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {assertDatabaseIntegrity} from '../../db/migration-lite.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getShardConfig} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {ChangeStreamerHttpClient} from '../change-streamer/change-streamer-http.ts';
import type {
  SnapshotMessage,
  SnapshotStatus,
} from '../change-streamer/snapshot.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {
  litestreamBackupListDuration,
  litestreamBackupMetricAttrs,
  litestreamBackupProcessMetricAttrs,
  litestreamBackupProcessDuration,
  litestreamBackupProcessRuns,
  litestreamRestoreAttempts,
  litestreamRestoredDbBytes,
  litestreamRestoreDuration,
  litestreamRestoreMetricAttrs,
  litestreamRestoreProcessDuration,
  litestreamRestoreRuns,
  litestreamRestoreValidationDuration,
  litestreamRestoreWaitDuration,
  type LitestreamRole,
} from './metrics.ts';

const RETRY_INTERVAL_MS = 3000;

type ReplicaConstraints = {
  replicaVersion: string;
  minWatermark: string;
};

type RestoreAttempt = {
  restored: boolean;
  backupURL: string | undefined;
};

export class BackupNotFoundException extends Error {
  static readonly name = 'BackupNotFoundException';

  constructor(backupURL: string | undefined) {
    super(`backup not found at ${backupURL}`);
  }
}

/**
 * @param replicaConstraints The constraints of the restored backup when
 *        restoring for the change-streamer (replication-manager). For the
 *        view-syncer, this should be unspecified so that the constraints are
 *        retrieved from the replication-manager via the snapshot protocol.
 */
export async function restoreReplica(
  lc: LogContext,
  config: ZeroConfig,
  replicaConstraints: ReplicaConstraints | null,
) {
  const start = performance.now();
  const role: LitestreamRole = replicaConstraints
    ? 'replication_manager'
    : 'view_syncer';
  let backupURL = config.litestream.backupURL;
  let result: 'success' | 'no_backup' | 'error' = 'error';
  // View-syncers (no replicaConstraints) wait indefinitely for the
  // replication-manager to publish a restorable backup. On a fresh stack the
  // first backup is not durable until the initial sync completes and litestream
  // uploads the initial snapshot, which can take many minutes for a large
  // replica. The platform's startup probe budget (which scales with replica
  // size) is the backstop, so restoreReplica must not impose its own shorter
  // cap and self-terminate while the backup is still being produced.
  //
  // `consecutiveErrors` distinguishes a transient restore *error* (e.g. the
  // `replicate` process compacting a snapshot mid-restore) — which is retried
  // once — from a "backup not found" result, which is expected while waiting.
  let consecutiveErrors = 0;
  try {
    for (;;) {
      try {
        const attempt = await tryRestore(lc, config, replicaConstraints, role);
        backupURL = attempt.backupURL;
        if (attempt.restored) {
          result = 'success';
          return;
        }
        consecutiveErrors = 0;
      } catch (e) {
        if (++consecutiveErrors <= 1) {
          // A restore will fail if the `replicate` process creates a new
          // snapshot (and compacts old files) at the same time. Snapshots are
          // infrequent (e.g. once every 12 hours), and the scenario is
          // recoverable with a retry.
          lc.warn?.(`restore attempt failed. retrying once`, e);
          continue;
        }
        // If it fails again on the retry, though, bail.
        throw e;
      }
      if (replicaConstraints) {
        // The replication-manager restores against explicit constraints, so a
        // missing backup is fatal (e.g. the litestream URL was purposefully
        // changed to force a resync). Only the view-syncer (no constraints)
        // waits for a backup to appear.
        throw new BackupNotFoundException(config.litestream.backupURL);
      }
      lc.info?.(
        `replica not found. retrying in ${RETRY_INTERVAL_MS / 1000} seconds`,
      );
      await sleep(RETRY_INTERVAL_MS);
    }
  } catch (e) {
    if (e instanceof BackupNotFoundException) {
      result = 'no_backup';
    }
    throw e;
  } finally {
    const attrs = litestreamRestoreMetricAttrs(config, role, backupURL);
    const labels = {...attrs, result};
    litestreamRestoreRuns().add(1, labels);
    litestreamRestoreDuration().recordMs(performance.now() - start, labels);
  }
}

function getLitestream(
  mode: 'restore' | 'replicate',
  config: ZeroConfig,
  logLevelOverride?: LogLevel,
  backupURLOverride?: string,
): {
  litestream: string;
  env: NodeJS.ProcessEnv;
} {
  const {
    executable,
    executableV5,
    restoreUsingV5,
    backupURL,
    logLevel,
    configPath,
    endpoint,
    region,
    port = config.port + 2,
    checkpointThresholdMB,
    minCheckpointPageCount = checkpointThresholdMB * 250, // SQLite page size is 4KB
    maxCheckpointPageCount = minCheckpointPageCount * 10,
    incrementalBackupIntervalMinutes,
    snapshotBackupIntervalHours,
    multipartConcurrency,
    multipartSize,
  } = config.litestream;

  // Set the snapshot interval to something smaller than x hours so that
  // the hourly check triggers on the hour, rather than the hour after.
  const snapshotBackupIntervalMinutes = snapshotBackupIntervalHours * 60 - 5;

  const litestream =
    // The v0.5.8+ litestream executable can restore from either the new LTX
    // format or the legacy WAL format, allowing forwards-compatibility /
    // rollback safety with zero-cache versions that backup to LTX.
    (mode === 'restore' && restoreUsingV5 ? executableV5 : executable) ??
    must(executable, `Missing --litestream-executable`);
  return {
    litestream,
    env: {
      ...process.env,
      ['ZERO_REPLICA_FILE']: config.replica.file,
      ['ZERO_LITESTREAM_BACKUP_URL']: must(backupURLOverride ?? backupURL),
      ['ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT']: String(
        minCheckpointPageCount,
      ),
      ['ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT']: String(
        maxCheckpointPageCount,
      ),
      ['ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES']: String(
        incrementalBackupIntervalMinutes,
      ),
      ['ZERO_LITESTREAM_LOG_LEVEL']: logLevelOverride ?? logLevel,
      ['ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_MINUTES']: String(
        snapshotBackupIntervalMinutes,
      ),
      ['ZERO_LITESTREAM_MULTIPART_CONCURRENCY']: String(multipartConcurrency),
      ['ZERO_LITESTREAM_MULTIPART_SIZE']: String(multipartSize),
      ['ZERO_LOG_FORMAT']: config.log.format,
      ['LITESTREAM_CONFIG']: configPath,
      ['LITESTREAM_PORT']: String(port),
      ...(endpoint ? {['ZERO_LITESTREAM_ENDPOINT']: endpoint} : {}),
      ...(region ? {['ZERO_LITESTREAM_REGION']: region} : {}),
    },
  };
}

async function tryRestore(
  lc: LogContext,
  config: ZeroConfig,
  replicaConstraints: ReplicaConstraints | null,
  role: LitestreamRole,
): Promise<RestoreAttempt> {
  let snapshotStatus: SnapshotStatus | undefined;
  if (!replicaConstraints) {
    // view-syncers fetch replica constraints from the replication-manager
    // via the snapshot protocol.
    const waitStart = performance.now();
    snapshotStatus = await reserveAndGetSnapshotStatus(lc, config);
    litestreamRestoreWaitDuration().recordMs(
      performance.now() - waitStart,
      litestreamRestoreMetricAttrs(config, role, snapshotStatus.backupURL),
    );
    lc.info?.(`restoring backup from ${snapshotStatus.backupURL}`);
    replicaConstraints = snapshotStatus;
  }

  const backupURL = snapshotStatus?.backupURL ?? config.litestream.backupURL;
  const attrs = litestreamRestoreMetricAttrs(config, role, backupURL);
  let result: 'success' | 'no_backup' | 'invalid_replica' | 'error' = 'error';
  try {
    const {litestream, env} = getLitestream(
      'restore',
      config,
      'debug', // Include all output from `litestream restore`, as it's minimal.
      snapshotStatus?.backupURL,
    );
    const {
      restoreParallelism: parallelism,
      multipartConcurrency,
      multipartSize,
    } = config.litestream;
    lc.info?.(`starting litestream restore`, {
      restoreParallelism: parallelism,
      multipartConcurrency,
      multipartSize,
    });
    const proc = spawn(
      litestream,
      [
        'restore',
        '-if-db-not-exists',
        '-if-replica-exists',
        '-parallelism',
        String(parallelism),
        config.replica.file,
      ],
      {env, stdio: 'inherit', windowsHide: true},
    );
    const {promise, resolve, reject} = resolver();
    proc.on('error', reject);
    proc.on('close', (code, signal) => {
      if (signal) {
        reject(`litestream killed with ${signal}`);
      } else if (code !== 0) {
        reject(`litestream exited with code ${code}`);
      } else {
        resolve();
      }
    });
    const processStart = performance.now();
    try {
      await promise;
      litestreamRestoreProcessDuration().recordMs(
        performance.now() - processStart,
        {...attrs, result: 'success'},
      );
    } catch (e) {
      litestreamRestoreProcessDuration().recordMs(
        performance.now() - processStart,
        {...attrs, result: 'error'},
      );
      throw e;
    }
    if (!existsSync(config.replica.file)) {
      result = 'no_backup';
      return {restored: false, backupURL};
    }
    const validationStart = performance.now();
    const valid = replicaIsValid(lc, config.replica.file, replicaConstraints);
    litestreamRestoreValidationDuration().recordMs(
      performance.now() - validationStart,
      {...attrs, result: valid ? 'success' : 'invalid_replica'},
    );
    if (!valid) {
      result = 'invalid_replica';
      lc.info?.(`Deleting local replica and retrying restore`);
      deleteLiteDB(config.replica.file);
      return {restored: false, backupURL};
    }
    result = 'success';
    litestreamRestoredDbBytes().add(statSync(config.replica.file).size, {
      ...attrs,
      result: 'success',
    });
    return {restored: true, backupURL};
  } finally {
    litestreamRestoreAttempts().add(1, {
      ...attrs,
      result,
    });
  }
}

function replicaIsValid(
  lc: LogContext,
  replica: string,
  constraints: ReplicaConstraints,
) {
  let db: Database | undefined;
  try {
    db = new Database(lc, replica);
    assertDatabaseIntegrity(lc, 'restored replica', db);
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(db),
    );
    if (replicaVersion !== constraints.replicaVersion) {
      lc.warn?.(
        `Local replica version ${replicaVersion} does not match expected replicaVersion ${constraints.replicaVersion}`,
        constraints,
      );
      return false;
    }
    if (watermark < constraints.minWatermark) {
      lc.warn?.(
        `Local replica watermark ${watermark} is earlier than minWatermark ${constraints.minWatermark}`,
      );
      return false;
    }
    lc.info?.(
      `Local replica at version ${replicaVersion} and watermark ${watermark} is compatible`,
      constraints,
    );
    return true;
  } catch (e) {
    lc.error?.('Error while validating restored replica', e);
    return false;
  } finally {
    db?.close();
  }
}

export function startReplicaBackupProcess(
  lc: LogContext,
  config: ZeroConfig,
): ChildProcess {
  const {litestream, env} = getLitestream('replicate', config);
  const attrs = litestreamBackupProcessMetricAttrs(config);
  lc.info?.(`starting litestream backup to ${config.litestream.backupURL}`);
  const start = performance.now();
  const proc = spawn(litestream, ['replicate'], {
    env,
    stdio: 'inherit',
    windowsHide: true,
  });
  let recorded = false;
  const record = (result: 'success' | 'error' | 'stopped') => {
    if (recorded) {
      return;
    }
    recorded = true;
    const labels = {...attrs, result};
    litestreamBackupProcessRuns().add(1, labels);
    litestreamBackupProcessDuration().recordMs(
      performance.now() - start,
      labels,
    );
  };
  proc.on('error', e => {
    lc.warn?.(`litestream backup process error`, e);
    record('error');
  });
  proc.on('close', (code, signal) => {
    if (signal) {
      lc.info?.(`litestream backup process stopped`, {signal});
      record('stopped');
    } else if (code === 0) {
      record('success');
    } else {
      lc.warn?.(`litestream backup process exited with code ${code}`);
      record('error');
    }
  });
  return proc;
}

// Listing the backup state requires a few S3 LIST requests, which should
// normally complete well within this timeout.
const LIST_BACKUP_TIMEOUT_MS = 30_000;

const wsRe = /\s+/;

/**
 * Returns the time of the most recent object (snapshot or WAL segment)
 * actually uploaded to the backup replica destination, as listed by the
 * bundled litestream CLI (`litestream snapshots` / `litestream wal`).
 *
 * This queries the replica destination (e.g. S3) directly, and thus serves
 * as a source of truth for backup durability. This is in contrast to the
 * `litestream_replica_progress` metric, which is exported when litestream
 * *believes* an upload has succeeded, and has been observed to advance even
 * when nothing is actually written to the destination.
 *
 * Rejects if the backup state cannot be determined (spawn error, non-zero
 * exit, timeout, or empty/unparseable listing).
 */
export async function getLastBackupTime(
  lc: LogContext,
  config: ZeroConfig,
): Promise<Date> {
  const [snapshots, wal] = await Promise.all([
    listBackupCreatedTimes(lc, config, 'snapshots'),
    listBackupCreatedTimes(lc, config, 'wal'),
  ]);
  const times = [...snapshots, ...wal];
  if (times.length === 0) {
    // Note: the litestream CLI exits with code 0 and logs listing errors
    // (e.g. S3 failures) to stderr, so an empty listing cannot be
    // distinguished from a failed one. Since a valid backup always contains
    // at least one snapshot, an empty listing is treated as a failure.
    throw new Error(
      `no snapshots or WAL segments listed at ${config.litestream.backupURL}`,
    );
  }
  return new Date(Math.max(...times.map(time => time.getTime())));
}

/**
 * Runs `litestream <snapshots|wal> <replica-file>` with the same config /
 * environment used by the `litestream replicate` process (so that the
 * backupURL, endpoint, region, and credentials are identical), and parses
 * the `created` column (RFC3339) of the tab-formatted output, e.g.:
 *
 * ```
 * replica  generation        index  size     created
 * s3       1862f44967b3863f  0      4546445  2026-06-10T01:11:32Z
 * ```
 */
async function listBackupCreatedTimes(
  lc: LogContext,
  config: ZeroConfig,
  command: 'snapshots' | 'wal',
): Promise<Date[]> {
  const start = performance.now();
  let result: 'success' | 'empty' | 'timeout' | 'error' = 'error';
  const {litestream, env} = getLitestream('replicate', config);
  const proc = spawn(litestream, [command, config.replica.file], {
    env,
    stdio: ['ignore', 'pipe', 'inherit'],
    windowsHide: true,
  });
  const {promise, resolve, reject} = resolver<string>();
  let stdout = '';
  proc.stdout.setEncoding('utf-8');
  proc.stdout.on('data', chunk => (stdout += chunk));
  proc.on('error', reject);
  proc.on('close', (code, signal) => {
    if (signal) {
      reject(new Error(`litestream ${command} killed with ${signal}`));
    } else if (code !== 0) {
      reject(new Error(`litestream ${command} exited with code ${code}`));
    } else {
      resolve(stdout);
    }
  });
  const timeout = setTimeout(() => {
    result = 'timeout';
    reject(new Error(`timed out listing backup state (litestream ${command})`));
    proc.kill('SIGKILL');
  }, LIST_BACKUP_TIMEOUT_MS);

  try {
    const output = await promise;
    const times = parseBackupCreatedTimes(lc, command, output);
    result = times.length ? 'success' : 'empty';
    return times;
  } finally {
    clearTimeout(timeout);
    litestreamBackupListDuration().recordMs(performance.now() - start, {
      ...litestreamBackupMetricAttrs(config),
      command,
      result,
    });
  }
}

/**
 * Parses the `created` column (the last, RFC3339-formatted column) from the
 * tab-formatted output of `litestream snapshots` / `litestream wal`. The
 * header row and any unparseable lines are skipped.
 *
 * Exported for testing.
 */
export function parseBackupCreatedTimes(
  lc: LogContext,
  command: 'snapshots' | 'wal',
  output: string,
): Date[] {
  const times: Date[] = [];
  for (const line of output.split('\n')) {
    const cols = line.trim().split(wsRe);
    const created = cols.at(-1);
    if (
      cols.length < 2 ||
      created === undefined ||
      created === 'created' /* header row */
    ) {
      continue;
    }
    const time = new Date(created);
    if (Number.isNaN(time.getTime())) {
      lc.warn?.(`unexpected line in litestream ${command} output: ${line}`);
      continue;
    }
    times.push(time);
  }
  return times;
}

function reserveAndGetSnapshotStatus(
  lc: LogContext,
  config: ZeroConfig,
): Promise<SnapshotStatus> {
  const {promise: status, resolve, reject} = resolver<SnapshotStatus>();

  void (async function () {
    const abort = new AbortController();
    process.on('SIGINT', () => abort.abort());
    process.on('SIGTERM', () => abort.abort());

    for (let i = 0; ; i++) {
      let err: unknown;
      try {
        let resolved = false;
        const stream = await reserveSnapshot(lc, config);
        for await (const msg of stream) {
          // Capture the value of the status message that the change-streamer
          // backup monitor returns, and hold the connection open to
          // "reserve" the snapshot and prevent change log cleanup.
          resolve(msg[1]);
          resolved = true;
        }
        // The change-streamer itself closes the connection when the
        // subscription is started (or the reservation retried).
        if (resolved) {
          break;
        }
      } catch (e) {
        err = e;
      }
      // Retry in the view-syncer since it cannot proceed until it connects
      // to a (compatible) replication-manager. In particular, a
      // replication-manager that does not support the view-syncer's
      // change-streamer protocol will close the stream with an error; this
      // retry logic essentially delays the startup of a view-syncer until
      // a compatible replication-manager has been rolled out, allowing
      // replication-manager and view-syncer services to be updated in
      // parallel.
      lc.warn?.(
        `Unable to reserve snapshot (attempt ${i + 1}). Retrying in 5 seconds.`,
        String(err),
      );
      try {
        await sleep(5000, abort.signal);
      } catch (e) {
        return reject(e);
      }
    }
  })();

  return status;
}

function reserveSnapshot(
  lc: LogContext,
  config: ZeroConfig,
): Promise<Source<SnapshotMessage>> {
  assertNormalized(config);
  const {taskID, change, changeStreamer} = config;
  const shardID = getShardConfig(config);

  const changeStreamerClient = new ChangeStreamerHttpClient(
    lc,
    shardID,
    change.db,
    changeStreamer.uri,
  );

  return changeStreamerClient.reserveSnapshot(taskID);
}
