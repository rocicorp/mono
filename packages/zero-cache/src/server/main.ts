import {resolver} from '@rocicorp/resolver';
import {availableParallelism} from 'node:os';
import path from 'node:path';
import {must} from '../../../shared/src/must.js';
import {getZeroConfig} from '../config/zero-config.js';
import {getSubscriberContext} from '../services/change-streamer/change-streamer-http.js';
import {SyncDispatcher} from '../services/dispatcher/sync-dispatcher.js';
import {installWebSocketHandoff} from '../services/dispatcher/websocket-handoff.js';
import {
  restoreReplica,
  startReplicaBackupProcess,
} from '../services/litestream/commands.js';
import type {Service} from '../services/service.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/init.js';
import {pgClient} from '../types/pg.js';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {orTimeout} from '../types/timeout.js';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.js';
import {
  exitAfter,
  ProcessManager,
  runUntilKilled,
  type WorkerType,
} from './life-cycle.js';
import {createLogContext} from './logging.js';

export default async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getZeroConfig(env);
  const lc = createLogContext(config, {worker: 'dispatcher'});
  const taskID = must(config.taskID, `main must set --task-id`);

  const processes = new ProcessManager(lc, parent ?? process);

  const numSyncers =
    config.numSyncWorkers !== undefined
      ? config.numSyncWorkers
      : // Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
        Math.max(1, availableParallelism() - 1);

  if (config.upstream.maxConns < numSyncers) {
    throw new Error(
      `Insufficient upstream connections (${config.upstream.maxConns}) for ${numSyncers} syncers.` +
        `Increase ZERO_UPSTREAM_MAX_CONNS or decrease ZERO_NUM_SYNC_WORKERS (which defaults to available cores).`,
    );
  }
  if (config.cvr.maxConns < numSyncers) {
    throw new Error(
      `Insufficient cvr connections (${config.cvr.maxConns}) for ${numSyncers} syncers.` +
        `Increase ZERO_CVR_MAX_CONNS or decrease ZERO_NUM_SYNC_WORKERS (which defaults to available cores).`,
    );
  }

  const internalFlags: string[] =
    numSyncers === 0
      ? []
      : [
          '--upstream-max-conns-per-worker',
          String(Math.floor(config.upstream.maxConns / numSyncers)),
          '--cvr-max-conns-per-worker',
          String(Math.floor(config.cvr.maxConns / numSyncers)),
        ];

  function loadWorker(
    modulePath: string,
    type: WorkerType,
    id?: string | number,
    ...args: string[]
  ): Worker {
    const worker = childWorker(modulePath, env, ...args, ...internalFlags);
    const name = path.basename(modulePath) + (id ? ` (${id})` : '');
    return processes.addWorker(worker, type, name);
  }

  const {backupURL} = config.litestream;
  const litestream = backupURL?.length;
  const runChangeStreamer = !config.changeStreamerURI;

  if (litestream) {
    // For the replication-manager (i.e. authoritative replica), only attempt
    // a restore once, allowing the backup to be absent.
    // For view-syncers, attempt a restore for up to 10 times over 30 seconds.
    await restoreReplica(lc, config, runChangeStreamer ? 1 : 10, 3000);
  }

  const {promise: changeStreamerReady, resolve} = resolver();
  const changeStreamer = runChangeStreamer
    ? loadWorker('./server/change-streamer.ts', 'supporting').once(
        'message',
        resolve,
      )
    : resolve();

  if (numSyncers) {
    // Technically, setting up the CVR DB schema is the responsibility of the Syncer,
    // but it is done here in the main thread because it is wasteful to have all of
    // the Syncers attempt the migration in parallel.
    const cvrDB = pgClient(lc, config.cvr.db);
    await initViewSyncerSchema(lc, cvrDB);
    void cvrDB.end();
  }

  // Wait for the change-streamer to be ready to guarantee that a replica
  // file is present.
  await changeStreamerReady;

  if (runChangeStreamer && litestream) {
    // Start a backup replicator and corresponding litestream backup process.
    const mode: ReplicaFileMode = 'backup';
    loadWorker('./server/replicator.ts', 'supporting', mode, mode);

    processes.addSubprocess(
      startReplicaBackupProcess(config),
      'supporting',
      'litestream',
    );
  }

  const syncers: Worker[] = [];
  if (numSyncers) {
    const mode: ReplicaFileMode =
      runChangeStreamer && litestream ? 'serving-copy' : 'serving';
    const replicator = loadWorker(
      './server/replicator.ts',
      'supporting',
      mode,
      mode,
    ).once('message', () => subscribeTo(lc, replicator));
    const notifier = createNotifierFrom(lc, replicator);
    for (let i = 0; i < numSyncers; i++) {
      syncers.push(
        loadWorker('./server/syncer.ts', 'user-facing', i + 1, mode),
      );
    }
    syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
  }

  lc.info?.('waiting for workers to be ready ...');
  if ((await orTimeout(processes.allWorkersReady(), 60_000)) === 'timed-out') {
    lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
  } else {
    lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);
  }

  const mainServices: Service[] = [];
  const {port} = config;

  if (numSyncers) {
    mainServices.push(new SyncDispatcher(lc, taskID, parent, syncers, {port}));
  } else if (changeStreamer && parent) {
    // When running as the replication-manager, the dispatcher process
    // hands off websockets from the main (tenant) dispatcher to the
    // change-streamer process.
    installWebSocketHandoff(
      lc,
      req => ({payload: getSubscriberContext(req), receiver: changeStreamer}),
      parent,
    );
  }

  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(lc, parent ?? process, ...mainServices);
  } catch (err) {
    processes.logErrorAndExit(err, 'dispatcher');
  }

  await processes.done();
}

if (!singleProcessMode()) {
  void exitAfter(() => runWorker(parentWorker, process.env));
}
