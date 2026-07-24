import {stat} from 'node:fs/promises';
import {pid} from 'node:process';
import type {ObservableCallback} from '@opentelemetry/api';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import type {
  LitestreamConfig,
  NormalizedZeroConfig,
} from '../config/normalize.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {initEventSink} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.ts';
import {reserveAndGetSnapshotStatus} from '../services/change-streamer/snapshot.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  tryRestore,
  type RestoreResult,
} from '../services/litestream/commands.ts';
import {
  litestreamRestoreDuration,
  litestreamRestoreMetricAttrs,
  litestreamRestoreRuns,
} from '../services/litestream/metrics.ts';
import {ReplicationStatusPublisher} from '../services/replicator/replication-status.ts';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.ts';
import {ThreadWriteWorkerClient} from '../services/replicator/write-worker-client.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import {
  getPragmaConfig,
  replicaFileModeSchema,
  setUpMessageHandlers,
  setupReplica,
  type WalMode,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

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
  const workerName = `${mode}-replicator`;
  startOtelAuto(createLogContext(config, workerName, 0, false), workerName, 0);
  lc = createLogContext(config, workerName);
  const unregisterInitialCorruptionDiagnosticTarget =
    registerSQLiteCorruptionDiagnosticTarget({
      debugName: `${workerName} replica`,
      dbPath: config.replica.file,
    });
  initEventSink(lc, config);

  if (fileMode === 'serving' && config.litestream.backupURL) {
    await restoreReplica(lc, config);
  }

  const {file: dbPath, walMode} = await setupReplica(
    lc,
    fileMode,
    config.replica,
  );
  unregisterInitialCorruptionDiagnosticTarget();
  registerSQLiteCorruptionDiagnosticTarget({
    debugName: `${workerName} replica`,
    dbPath,
  });

  setupMetrics(lc, dbPath, walMode);

  // Create the write worker for async SQLite writes.
  const pragmas = getPragmaConfig(fileMode);
  const workerClient = new ThreadWriteWorkerClient();
  await workerClient.init(dbPath, mode, pragmas, config.log);

  const runningLocalChangeStreamer =
    config.changeStreamer.mode === 'dedicated' && !config.changeStreamer.uri;
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
  } = config;
  const changeStreamer = new ChangeStreamerHttpClient(
    lc,
    shard,
    change.db,
    changeStreamerURI,
  );

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
  );

  setUpMessageHandlers(lc, replicator, parent);

  const running = runUntilKilled(lc, parent, replicator);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }

  return running;
}

function setupMetrics(lc: LogContext, file: string, walMode: WalMode) {
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

const RETRY_INTERVAL_MS = 3000;

// View-syncers (no replicaConstraints) wait indefinitely for the
// replication-manager to publish a restorable backup. On a fresh stack the
// first backup is not durable until the initial sync completes and litestream
// uploads the initial snapshot, which can take many minutes for a large
// replica. The platform's startup probe budget (which scales with replica
// size) is the backstop, so restoreReplica must not impose its own shorter
// cap and self-terminate while the backup is still being produced.
async function restoreReplica(lc: LogContext, config: NormalizedZeroConfig) {
  const start = performance.now();
  let backupURL: string | undefined;
  let result: RestoreResult | undefined;
  try {
    for (;;) {
      const snapshotStatus = await reserveAndGetSnapshotStatus(lc, config);
      // The backupURL comes from the replication-manager's snapshot response.
      ({backupURL} = snapshotStatus);
      const litestream: LitestreamConfig = {...config.litestream, backupURL};
      const attempt = await tryRestore(
        lc,
        litestream,
        config.replica.file,
        snapshotStatus,
        'view_syncer',
      );
      if (attempt.restored) {
        result = attempt.result;
        return;
      }
      lc.info?.(
        `replica not found. retrying in ${RETRY_INTERVAL_MS / 1000} seconds`,
      );
      await sleep(RETRY_INTERVAL_MS);
    }
  } finally {
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
