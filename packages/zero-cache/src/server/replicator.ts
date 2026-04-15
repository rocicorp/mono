import {stat} from 'node:fs/promises';
import {pid} from 'node:process';
import type {ObservableCallback} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
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
  const lc = createLogContext(config, workerName);
  initEventSink(lc, config);

  const {file: dbPath, walMode} = await setupReplica(
    lc,
    fileMode,
    config.replica,
  );

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

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
