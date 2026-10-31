import {stat} from 'node:fs/promises';
import {pid} from 'node:process';
import type {ObservableCallback} from '@opentelemetry/api';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {
  LitestreamConfig,
  NormalizedZeroConfig,
} from '../config/normalize.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {initEventSink} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {
  ChangeStreamerHttpClient,
  type ReservationFollowup,
  type SnapshotReserver,
} from '../services/change-streamer/change-streamer-http.ts';
import {reserveAndGetSnapshotStatus} from '../services/change-streamer/snapshot.ts';
import {
  exitAfter,
  runUntilKilled,
  SupportingProcesses,
} from '../services/life-cycle.ts';
import {
  readLocalReplica,
  startReplicaBackupProcess,
  tryRestore,
  type RestoreAttempt,
  type RestoreResult,
} from '../services/litestream/commands.ts';
import {
  litestreamRestoreDuration,
  litestreamRestoreMetricAttrs,
  litestreamRestoreRuns,
} from '../services/litestream/metrics.ts';
import {RestoreProgressReporter} from '../services/litestream/restore-progress.ts';
import {ReplicationStatusPublisher} from '../services/replicator/replication-status.ts';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.ts';
import {sqliteFileBytes} from '../services/replicator/sqlite-change-log-observability.ts';
import {
  ThreadWriteWorkerClient,
  type ForceCheckpointConfig,
} from '../services/replicator/write-worker-client.ts';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type DrainRequestedMessage,
  type Worker,
} from '../types/processes.ts';
import {installProfileHandler} from '../types/profiler.ts';
import {getShardConfig} from '../types/shards.ts';
import {
  deleteStaleChangeLog,
  getPragmaConfig,
  replicaFileModeSchema,
  setUpMessageHandlers,
  setupReplica,
  type WalMode,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {SERVING_CHECKPOINTER_URL} from './worker-urls.ts';

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});
  const mode: ReplicatorMode = fileMode === 'backup' ? 'backup' : 'serving';
  const runningLocalChangeStreamer =
    config.changeStreamer.mode === 'dedicated' && !config.changeStreamer.uri;
  const workerName = `${mode}-replicator`;
  installProfileHandler(parent, workerName);
  startOtelAuto(createLogContext(config, workerName, 0, false), workerName, 0);
  lc = createLogContext(config, workerName);
  const unregisterInitialCorruptionDiagnosticTarget =
    registerSQLiteCorruptionDiagnosticTarget(
      {
        debugName: `${workerName} replica`,
        dbPath: config.replica.file,
      },
      config.sqliteCorruptionChecks,
    );
  initEventSink(lc, config);

  const shard = getShardConfig(config);
  const {
    taskID,
    change,
    changeStreamer: {
      port,
      uri: changeStreamerURI = runningLocalChangeStreamer
        ? `http://localhost:${port}/`
        : undefined,
    },
    replicationLag: {initialReadinessThresholdMs, maxReadinessDelayMs},
  } = config;
  const changeStreamer = new ChangeStreamerHttpClient(
    lc,
    shard,
    change.db,
    changeStreamerURI,
  );

  // Remote view-syncers restore from the replication-manager, which supplies
  // the backup URL. A local change-streamer owns the canonical replica and
  // must not infer backup configuration from bundled executable paths.
  //
  // The reservation's connection (`initialConnection`) is threaded through to
  // IncrementalSyncer's first subscribe(), so the replication-manager that
  // reserved the change log for this restore is the one that serves the
  // subscription (single-connection handoff).
  let initialConnection: ReservationFollowup | undefined;
  if (
    fileMode === 'serving' &&
    !runningLocalChangeStreamer &&
    (config.litestream.executable || config.litestream.executableV5)
  ) {
    initialConnection = await restoreReplica(lc, config, changeStreamer);
  }

  const {
    file: dbPath,
    walMode,
    pageSize,
  } = await setupReplica(lc, fileMode, config.replica);
  unregisterInitialCorruptionDiagnosticTarget();
  registerSQLiteCorruptionDiagnosticTarget(
    {
      debugName: `${workerName} replica`,
      dbPath,
    },
    config.sqliteCorruptionChecks,
  );

  // A file left behind by a run with the writer enabled would otherwise hold
  // local disk indefinitely. See `replicatorDeletesStaleChangeLog` for why
  // this is keyed on the config flag and nothing else.
  deleteStaleChangeLog(config.changeStreamer.sqliteChangeLogMode, dbPath);

  setupMetrics(lc, dbPath, walMode, pageSize);

  // Create the write worker for async SQLite writes. Enable write-path
  // checkpoint backpressure only for the backup replicator on litestream v5,
  // which owns the replica litestream backs up.
  const pragmas = getPragmaConfig();
  const checkpoint: ForceCheckpointConfig | null =
    mode === 'backup' &&
    config.litestream.backupURL &&
    config.litestream.backupUsingV5 &&
    config.litestream.forceCheckpointThresholdMB
      ? {
          checkpointThresholdPages:
            (config.litestream.forceCheckpointThresholdMB * 1024 ** 2) /
            pageSize,
          maxWalPages: !config.litestream.maxWalSizeMB
            ? undefined
            : (config.litestream.maxWalSizeMB * 1024 ** 2) / pageSize,
        }
      : null;

  const workerClient = new ThreadWriteWorkerClient();
  await workerClient.init(dbPath, mode, pragmas, config.log, checkpoint);

  // The backup-replicator manages the litestream backup process, which
  // checkpoints the backup replica. A serving replicator instead manages a
  // checkpointer for its replica. Either is started after the write worker
  // has prepared the db file in order to avoid contending on the sqlite lock.
  const processes = new SupportingProcesses(lc, parent);
  if (mode === 'backup') {
    processes.add(
      startReplicaBackupProcess(lc, config.litestream, dbPath),
      'litestream',
    );
  } else {
    processes.add(
      childWorker(SERVING_CHECKPOINTER_URL, env, dbPath),
      'serving-checkpointer',
    );
  }

  const replicator = new ReplicatorService(
    lc,
    taskID,
    `${workerName}-${pid}`,
    mode,
    changeStreamer,
    workerClient,
    runningLocalChangeStreamer
      ? // publish ReplicationStatusEvents from backup-replicator only
        ReplicationStatusPublisher.forReplicaFile(dbPath)
      : null,
    {
      readinessThresholdMs: initialReadinessThresholdMs,
      maxReadinessDelayMs,
    },
    initialConnection,
    // View-syncers (i.e. with a remote change-streamer) switch over to
    // another replication-manager when theirs drains.
    runningLocalChangeStreamer
      ? undefined
      : {
          onRestoreGranted: () =>
            parent.send<DrainRequestedMessage>(['drainRequested', {}]),
          replicaFile: dbPath,
        },
  );

  setUpMessageHandlers(lc, replicator, parent);

  void replicator.ready().then(() => {
    parent.send(['ready', {ready: true}]);
  });

  return runUntilKilled(lc, parent, replicator, processes);
}

export function setupMetrics(
  lc: LogContext,
  file: string,
  walMode: WalMode,
  pageSize: number,
) {
  let db: Database | undefined;
  const getDb = () => {
    if (!db) {
      try {
        db = new Database(lc, file, {readonly: true});
      } catch (e) {
        lc.warn?.(`unable to open ${file} for wal metrics`, e);
      }
    }
    return db;
  };

  getOrCreateGauge('replica', 'db_size', {
    description:
      `The size of the replica's main db file, ` +
      `which does not include the wal file(s)`,
    unit: 'bytes',
  }).addCallback(observeFileSize(lc, file));

  getOrCreateGauge('replica', 'wal_size', {
    description: `The size of the replica's wal file`,
    unit: 'bytes',
  }).addCallback(observeFileSize(lc, `${file}-wal`));

  if (walMode === 'wal2') {
    getOrCreateGauge('replica', 'wal2_size', {
      description: `The size of the replica's wal2 file`,
      unit: 'bytes',
    }).addCallback(observeFileSize(lc, `${file}-wal2`));
  }

  getOrCreateGauge('replica', 'wal_active_bytes', {
    description:
      `The actual size of active WAL frames currently in use ` +
      `(excluding pre-allocated or reclaimed space).`,
    unit: 'bytes',
  }).addCallback(observeWalActiveBytes(lc, getDb, pageSize));

  getOrCreateGauge('replica', 'wal_uncheckpointed_bytes', {
    description:
      `The size of un-checkpointed WAL frames that have not yet ` +
      `been backfilled into the main database.`,
    unit: 'bytes',
  }).addCallback(observeWalUncheckpointedBytes(lc, getDb, pageSize));

  // A whole-database total rather than a per-file one. Its counterpart,
  // `sqlite_change_log.file_bytes`, is emitted by the change-streamer, which is
  // the process that writes the log.
  getOrCreateGauge('replica', 'file_bytes', {
    description:
      `The replica's total on-disk footprint: its main db file plus its ` +
      `wal sidecars.`,
    unit: 'By',
  }).addCallback(observeSQLiteFileBytes(lc, file));
}

function observeFileSize(lc: LogContext, file: string): ObservableCallback {
  return async o => {
    try {
      const stats = await stat(file);
      o.observe(stats.size);
    } catch (e) {
      lc.warn?.(`unable to stat ${file} for size metrics`, e);
    }
  };
}

function observeWalActiveBytes(
  lc: LogContext,
  getDb: () => Database | undefined,
  pageSize: number,
): ObservableCallback {
  return o => {
    try {
      const db = getDb();
      if (!db) {
        return;
      }
      const [{log}] = db.pragma<{log: number}>('wal_checkpoint(NOOP)');
      if (typeof log === 'number') {
        o.observe(Math.max(0, log) * pageSize);
      }
    } catch (e) {
      lc.warn?.('unable to inspect wal_checkpoint for active bytes metric', e);
    }
  };
}

function observeWalUncheckpointedBytes(
  lc: LogContext,
  getDb: () => Database | undefined,
  pageSize: number,
): ObservableCallback {
  return o => {
    try {
      const db = getDb();
      if (!db) {
        return;
      }
      const [{log, checkpointed}] = db.pragma<{
        log: number;
        checkpointed: number;
      }>('wal_checkpoint(NOOP)');
      if (typeof log === 'number' && typeof checkpointed === 'number') {
        o.observe(Math.max(0, log - checkpointed) * pageSize);
      }
    } catch (e) {
      lc.warn?.(
        'unable to inspect wal_checkpoint for uncheckpointed bytes metric',
        e,
      );
    }
  };
}

function observeSQLiteFileBytes(
  lc: LogContext,
  file: string,
): ObservableCallback {
  return async o => o.observe(await sqliteFileBytes(lc, file));
}

const RETRY_INTERVAL_MS = 3000;

// The number of times a view-syncer retries a snapshot reservation from a
// replication-manager with an older replica version than its local replica
// (e.g. a not-yet-terminated replication-manager during a resync rollout),
// before accepting the older version (e.g. if the upstream was legitimately
// replaced by one with a lower replica version).
const MAX_OLDER_REPLICA_VERSION_RETRIES = 20;

// View-syncers wait indefinitely for the replication-manager to publish a
// restorable backup. On a fresh stack the first backup is not durable until
// the initial sync completes and litestream uploads the initial snapshot,
// which can take many minutes for a large replica. The platform's startup
// probe budget (which scales with replica size) is the backstop, so
// restoreReplica must not impose its own shorter cap and self-terminate
// while the backup is still being produced.
//
// Returns the `followup` of the reservation whose backup was actually
// restored, still open, so the caller can hand it to IncrementalSyncer's
// first subscribe() via `followup.subscribe()`: the same replication-manager
// that reserved the change log is the one that serves the subscription.
/** Exported for testing. */
export async function restoreReplica(
  lc: LogContext,
  config: NormalizedZeroConfig,
  changeStreamer: SnapshotReserver,
): Promise<ReservationFollowup> {
  const start = performance.now();
  let backupURL: string | undefined;
  let result: RestoreResult | undefined;
  const progress = new RestoreProgressReporter(lc, config.replica.file);
  let olderReplicaVersionRetries = 0;
  // Describing the local replica (if any) allows the replication-manager to
  // account for replicas that are being replaced (see RestoreAdmission). It
  // is read once, before the first attempt, as a failed attempt may have
  // already deleted it; it continues to describe the replica being replaced
  // (and the version not to downgrade from) on every retry.
  const localReplica = readLocalReplica(lc, config.replica.file);
  const localReplicaVersion = localReplica?.replicaVersion;
  try {
    for (;;) {
      const {reserved, followup} = await reserveAndGetSnapshotStatus(
        lc,
        config.taskID,
        changeStreamer,
        localReplica,
      );
      if (
        localReplicaVersion !== undefined &&
        reserved.replicaVersion < localReplicaVersion &&
        olderReplicaVersionRetries++ < MAX_OLDER_REPLICA_VERSION_RETRIES
      ) {
        // Don't replace a newer local replica with an older one, e.g. from
        // a replication-manager that has yet to be replaced by a resync.
        followup.cancel();
        lc.info?.(
          `replication-manager is at an older replica version ` +
            `(${reserved.replicaVersion}) than the local replica ` +
            `(${localReplicaVersion}). retrying in ${RETRY_INTERVAL_MS / 1000} seconds`,
        );
        await sleep(RETRY_INTERVAL_MS);
        continue;
      }
      // The backupURL comes from the replication-manager's snapshot response.
      ({backupURL} = reserved);
      const litestream: LitestreamConfig = {...config.litestream, backupURL};
      progress.start(reserved.replicaSize);
      let attempt: RestoreAttempt;
      try {
        attempt = await tryRestore(
          lc,
          litestream,
          config.replica.file,
          reserved,
          'view_syncer',
          followup.signal, // abort the restore if the reservation disconnects
        );
      } catch (e) {
        if (followup.signal.aborted) {
          lc.info?.(
            `reservation disconnected during restore. retrying`,
            String(e),
          );
          continue;
        }
        followup.cancel();
        throw e;
      } finally {
        progress.stop();
      }
      if (attempt.restored) {
        result = attempt.result;
        progress.done();
        return followup; // connection to be converted to a subscription
      }
      // This attempt's reservation is stale; a fresh one is made on retry.
      followup.cancel();
      lc.info?.(
        `replica not found. retrying in ${RETRY_INTERVAL_MS / 1000} seconds`,
      );
      await sleep(RETRY_INTERVAL_MS);
    }
  } finally {
    progress.stop();
    const attrs = litestreamRestoreMetricAttrs(
      config.litestream,
      'view_syncer',
      backupURL,
    );
    const labels = {...attrs, result: result ?? 'error'};
    litestreamRestoreRuns().add(1, labels);
    litestreamRestoreDuration().recordMs(performance.now() - start, labels);
  }
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
