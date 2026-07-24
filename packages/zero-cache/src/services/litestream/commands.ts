import type {ChildProcess} from 'node:child_process';
import {spawn} from 'node:child_process';
import {existsSync, statSync} from 'node:fs';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {type LitestreamConfig} from '../../config/normalize.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
} from '../../db/sqlite-corruption.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {
  litestreamBackupListDuration,
  litestreamBackupMetricAttrs,
  litestreamBackupProcessMetricAttrs,
  litestreamBackupProcessDuration,
  litestreamBackupProcessRuns,
  litestreamRestoreAttempts,
  litestreamRestoredDbBytes,
  litestreamRestoreMetricAttrs,
  litestreamRestoreProcessDuration,
  litestreamRestoreValidationDuration,
  type LitestreamRole,
} from './metrics.ts';

export type ReplicaConstraints = {
  replicaVersion: string;
  minWatermark: string;
};

export type RestoreResult =
  | 'success'
  | 'no_backup'
  | 'invalid_replica'
  | 'error';

type RestoreAttempt = {
  restored: boolean;
  backupURL: string | undefined;
  result: RestoreResult;
};

export class BackupNotFoundException extends Error {
  static readonly name = 'BackupNotFoundException';

  constructor(backupURL: string | undefined) {
    super(`backup not found at ${backupURL}`);
  }
}

function getLitestream(
  mode: 'restore' | 'replicate',
  config: LitestreamConfig,
  replicaFile: string,
  logLevelOverride?: LogLevel,
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
    port,
    checkpointThresholdMB,
    minCheckpointPageCount = checkpointThresholdMB * 250, // SQLite page size is 4KB
    maxCheckpointPageCount = minCheckpointPageCount * 10,
    incrementalBackupIntervalMinutes,
    snapshotBackupIntervalHours,
    multipartConcurrency,
    multipartSize,
  } = config;

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
      ['ZERO_REPLICA_FILE']: replicaFile,
      ['ZERO_LITESTREAM_BACKUP_URL']: must(backupURL),
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
      ['ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS']: String(
        snapshotBackupIntervalHours,
      ),
      ['ZERO_LITESTREAM_SNAPSHOT_RETENTION_INTERVAL_HOURS']: String(
        snapshotBackupIntervalHours + 6, // delete old snapshots after 6 hours
      ),
      ['ZERO_LITESTREAM_MULTIPART_CONCURRENCY']: String(multipartConcurrency),
      ['ZERO_LITESTREAM_MULTIPART_SIZE']: String(multipartSize),
      ['ZERO_LOG_FORMAT']: 'json',
      ['LITESTREAM_CONFIG']: configPath,
      ['LITESTREAM_PORT']: String(port),
      ...(endpoint ? {['ZERO_LITESTREAM_ENDPOINT']: endpoint} : {}),
      ...(region ? {['ZERO_LITESTREAM_REGION']: region} : {}),
    },
  };
}

export async function tryRestore(
  lc: LogContext,
  config: LitestreamConfig,
  replicaFile: string,
  replicaConstraints: ReplicaConstraints,
  role: LitestreamRole,
): Promise<RestoreAttempt> {
  const {backupURL} = config;
  const attrs = litestreamRestoreMetricAttrs(config, role, backupURL);
  let result: RestoreResult = 'error';
  try {
    const replicaExistedBeforeRestore = existsSync(replicaFile);
    const {litestream, env} = getLitestream(
      'restore',
      config,
      replicaFile,
      'debug', // Include all output from `litestream restore`, as it's minimal.
    );
    const {
      restoreParallelism: parallelism,
      multipartConcurrency,
      multipartSize,
    } = config;
    lc.info?.(`starting litestream restore`, {
      restoreParallelism: parallelism,
      multipartConcurrency,
      multipartSize,
    });
    // Pipe (rather than inherit) litestream's stdout/stderr so that its own
    // `"level":"ERROR"` output on a failed restore does not go straight to the
    // pod's stdout — where a log-scraper alert would page on it — before our
    // code has decided whether the failure is retriable. The captured output is
    // re-surfaced through `lc` below with a distinct message per attempt, so a
    // retriable failure is routed to a non-paging warning and only a post-retry
    // failure pages. See INC-961.
    const proc = spawn(
      litestream,
      [
        'restore',
        '-if-db-not-exists',
        '-if-replica-exists',
        '-parallelism',
        String(parallelism),
        replicaFile,
      ],
      {env, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true},
    );
    let stdout = '';
    let stderr = '';
    proc.stdout.setEncoding('utf-8');
    proc.stderr.setEncoding('utf-8');
    proc.stdout.on('data', chunk => (stdout += chunk));
    proc.stderr.on('data', chunk => (stderr += chunk));
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
      // A successful restore does not emit ERROR-level output; forward
      // litestream's own (minimal, debug-level) restore logging to the pod's
      // stdout/stderr, matching the previous `stdio: 'inherit'` behavior.
      if (stdout) {
        process.stdout.write(stdout);
      }
      if (stderr) {
        process.stderr.write(stderr);
      }
    } catch (e) {
      litestreamRestoreProcessDuration().recordMs(
        performance.now() - processStart,
        {...attrs, result: 'error'},
      );
      throw e;
    }
    if (!existsSync(replicaFile)) {
      result = 'no_backup';
      return {restored: false, backupURL, result};
    }
    const validationStart = performance.now();
    const valid = replicaIsValid(lc, replicaFile, replicaConstraints);
    litestreamRestoreValidationDuration().recordMs(
      performance.now() - validationStart,
      {...attrs, result: valid ? 'success' : 'invalid_replica'},
    );
    if (!valid) {
      result = 'invalid_replica';
      lc.info?.(`Deleting local replica and retrying restore`);
      deleteLiteDB(replicaFile);
      return {restored: false, backupURL, result};
    }
    result = 'success';
    if (!replicaExistedBeforeRestore) {
      litestreamRestoredDbBytes().add(statSync(replicaFile).size, {
        ...attrs,
        result: 'success',
      });
    }
    return {restored: true, backupURL, result};
  } finally {
    litestreamRestoreAttempts().add(1, {...attrs, result});
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
    if (isSQLiteCorruption(e)) {
      logSQLiteCorruptionDiagnostics(lc, 'restored replica', replica, e);
    }
    lc.error?.('Error while validating restored replica', e);
    return false;
  } finally {
    db?.close();
  }
}

export function startReplicaBackupProcess(
  lc: LogContext,
  config: LitestreamConfig,
  replicaFile: string,
): ChildProcess {
  const {litestream, env} = getLitestream('replicate', config, replicaFile);
  const attrs = litestreamBackupProcessMetricAttrs(config);
  lc.info?.(`starting litestream backup to ${config.backupURL}`);
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
  config: LitestreamConfig,
  replicaFile: string,
): Promise<Date> {
  const [snapshots, wal] = await Promise.all([
    listBackupCreatedTimes(lc, config, replicaFile, 'snapshots'),
    listBackupCreatedTimes(lc, config, replicaFile, 'wal'),
  ]);
  const times = [...snapshots, ...wal];
  if (times.length === 0) {
    // Note: the litestream CLI exits with code 0 and logs listing errors
    // (e.g. S3 failures) to stderr, so an empty listing cannot be
    // distinguished from a failed one. Since a valid backup always contains
    // at least one snapshot, an empty listing is treated as a failure.
    throw new Error(
      `no snapshots or WAL segments listed at ${config.backupURL}`,
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
  config: LitestreamConfig,
  replicaFile: string,
  command: 'snapshots' | 'wal',
): Promise<Date[]> {
  const start = performance.now();
  let result: 'success' | 'empty' | 'timeout' | 'error' = 'error';
  const {litestream, env} = getLitestream('replicate', config, replicaFile);
  const proc = spawn(litestream, [command, replicaFile], {
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
