import type {LogContext, LogLevel} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {ChildProcess, spawn} from 'node:child_process';
import {existsSync} from 'node:fs';
import {must} from '../../../../shared/src/must.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {assertNormalized} from '../../config/normalize.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {getShardConfig} from '../../types/shards.ts';
import {ChangeStreamerHttpClient} from '../change-streamer/change-streamer-http.ts';

// Retry for up to 3 minutes (60 times with 3 second delay).
// Beyond that, let the container runner restart the task.
const MAX_RETRIES = 60;
const RETRY_INTERVAL_MS = 3000;

/**
 * @returns The time at which the last restore started
 *          (i.e. not counting failed attempts).
 */
export async function restoreReplica(
  lc: LogContext,
  config: ZeroConfig,
): Promise<Date> {
  const {changeStreamer} = config;

  for (let i = 0; i < MAX_RETRIES; i++) {
    if (i > 0) {
      lc.info?.(
        `replica not found. retrying in ${RETRY_INTERVAL_MS / 1000} seconds`,
      );
      await sleep(RETRY_INTERVAL_MS);
    }
    const start = new Date();
    const restored = await tryRestore(lc, config);
    if (restored) {
      return start;
    }
    if (changeStreamer.mode === 'dedicated') {
      lc.info?.('no litestream backup found');
      return start;
    }
  }
  throw new Error(`max attempts exceeded restoring replica`);
}

function getLitestream(
  config: ZeroConfig,
  logLevelOverride?: LogLevel,
  backupURLOverride?: string,
): {
  litestream: string;
  env: NodeJS.ProcessEnv;
} {
  const {
    executable,
    backupURL,
    logLevel,
    configPath,
    port = config.port + 2,
    checkpointThresholdMB,
    incrementalBackupIntervalMinutes,
    snapshotBackupIntervalHours,
    multipartConcurrency,
    multipartSize,
  } = config.litestream;

  // Set the snapshot interval to something smaller than x hours so that
  // the hourly check triggers on the hour, rather than the hour after.
  const snapshotBackupIntervalMinutes = snapshotBackupIntervalHours * 60 - 5;
  const minCheckpointPageCount = checkpointThresholdMB * 250; // SQLite page size is 4k
  const maxCheckpointPageCount = minCheckpointPageCount * 10;

  return {
    litestream: must(executable, `Missing --litestream-executable`),
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
    },
  };
}

async function tryRestore(lc: LogContext, config: ZeroConfig) {
  const {changeStreamer} = config;

  // Fire off a snapshot reservation to the current replication-manager
  // (if there is one).
  const backupURL = reserveAndGetSnapshotLocation(lc, config);
  let backupURLOverride: string | undefined;
  if (changeStreamer.mode === 'discover') {
    // The return value is required by view-syncers ...
    backupURLOverride = await backupURL;
    lc.info?.(`restoring backup from ${backupURLOverride}`);
  } else {
    // but it is also useful to pause change-log cleanup when a new
    // replication-manager is starting up. In this case, the request is
    // best-effort. In particular, there may not be a previous
    // replication-manager running at all.
    void backupURL.catch(e => lc.debug?.(e));
  }

  const {litestream, env} = getLitestream(
    config,
    'debug', // Include all output from `litestream restore`, as it's minimal.
    backupURLOverride,
  );
  const {restoreParallelism: parallelism} = config.litestream;
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
  await promise;
  return existsSync(config.replica.file);
}

export function startReplicaBackupProcess(config: ZeroConfig): ChildProcess {
  const {litestream, env} = getLitestream(config);
  return spawn(litestream, ['replicate'], {
    env,
    stdio: 'inherit',
    windowsHide: true,
  });
}

async function reserveAndGetSnapshotLocation(
  lc: LogContext,
  config: ZeroConfig,
) {
  const {promise: backupURL, resolve, reject} = resolver<string>();
  try {
    assertNormalized(config);
    const {taskID, change, changeStreamer} = config;
    const shardID = getShardConfig(config);

    const changeStreamerClient = new ChangeStreamerHttpClient(
      lc,
      shardID,
      change.db,
      changeStreamer.uri,
    );

    const sub = await changeStreamerClient.reserveSnapshot(taskID);

    // Capture the value of the status message that the change-streamer
    // (i.e. BackupMonitor) returns, and hold the connection open to
    // "reserve" the snapshot and prevent change log cleanup.
    //
    // The change-streamer itself will close the connection when the
    // subscription is started (or the reservation retried).
    void (async function () {
      try {
        for await (const msg of sub) {
          resolve(msg[1].backupURL);
        }
      } catch (e) {
        reject(e);
      }
    })();
  } catch (e) {
    reject(e);
  }
  return backupURL;
}
