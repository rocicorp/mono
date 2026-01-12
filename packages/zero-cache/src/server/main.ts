import {resolver} from '@rocicorp/resolver';
import path from 'node:path';
import {must} from '../../../shared/src/must.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {
  exitAfter,
  ProcessManager,
  runUntilKilled,
  type WorkerType,
} from '../services/life-cycle.ts';
import {
  restoreReplica,
  startReplicaBackupProcess,
} from '../services/litestream/commands.ts';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {WorkerDispatcher} from './worker-dispatcher.ts';
import {
  CHANGE_STREAMER_URL,
  MUTATOR_URL,
  REAPER_URL,
  REPLICATOR_URL,
  SYNCER_URL,
} from './worker-urls.ts';

const clientConnectionBifurcated = false;

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getNormalizedZeroConfig({env});

  startOtelAuto(createLogContext(config, {worker: 'dispatcher'}, false));
  const lc = createLogContext(config, {worker: 'dispatcher'}, true);
  initEventSink(lc, config);

  const processes = new ProcessManager(lc, parent);

  const {numSyncWorkers: numSyncers} = config;
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
    moduleUrl: URL,
    type: WorkerType,
    id?: string | number,
    ...args: string[]
  ): Worker {
    const worker = childWorker(moduleUrl, env, ...args, ...internalFlags);
    const name = path.basename(moduleUrl.pathname) + (id ? ` (${id})` : '');
    return processes.addWorker(worker, type, name);
  }

  const {
    taskID,
    changeStreamer: {mode: changeStreamerMode, uri: changeStreamerURI},
    litestream,
  } = config;
  const runChangeStreamer =
    changeStreamerMode === 'dedicated' && changeStreamerURI === undefined;

  let restoreStart = new Date();
  if (litestream.backupURL || (litestream.executable && !runChangeStreamer)) {
    try {
      restoreStart = await restoreReplica(lc, config);
    } catch (e) {
      if (runChangeStreamer) {
        // If the restore failed, e.g. due to a corrupt backup, the
        // replication-manager recovers by re-syncing.
        lc.error?.('error restoring backup. resyncing the replica.', e);
      } else {
        // View-syncers, on the other hand, have no option other than to retry
        // until a valid backup has been published. This is achieved by
        // shutting down and letting the container runner retry with its
        // configured policy.
        throw e;
      }
    }
  }

  const {promise: changeStreamerReady, resolve: changeStreamerStarted} =
    resolver();
  const changeStreamer = runChangeStreamer
    ? loadWorker(
        CHANGE_STREAMER_URL,
        'supporting',
        undefined,
        String(restoreStart.getTime()),
      ).once('message', changeStreamerStarted)
    : (changeStreamerStarted() ?? undefined);

  const {promise: reaperReady, resolve: reaperStarted} = resolver();
  if (numSyncers > 0) {
    loadWorker(REAPER_URL, 'supporting').once('message', reaperStarted);
  } else {
    reaperStarted();
  }

  // Wait for the change-streamer to be ready to guarantee that a replica
  // file is present.
  await changeStreamerReady;

  if (runChangeStreamer && litestream.backupURL) {
    // Start a backup replicator and corresponding litestream backup process.
    const {promise: backupReady, resolve} = resolver();
    const mode: ReplicaFileMode = 'backup';
    loadWorker(REPLICATOR_URL, 'supporting', mode, mode).once(
      // Wait for the Replicator's first message (i.e. "ready") before starting
      // litestream backup in order to avoid contending on the lock when the
      // replicator first prepares the db file.
      'message',
      () => {
        processes.addSubprocess(
          startReplicaBackupProcess(config),
          'supporting',
          'litestream',
        );
        resolve();
      },
    );
    await backupReady;
  }

  // Before starting the view-syncers, ensure that the reaper has started
  // up, indicating that any CVR db migrations have been performed.
  await reaperReady;

  const syncers: Worker[] = [];
  if (numSyncers) {
    const mode: ReplicaFileMode =
      runChangeStreamer && litestream.backupURL ? 'serving-copy' : 'serving';
    const {promise: replicaReady, resolve} = resolver();
    const replicator = loadWorker(
      REPLICATOR_URL,
      'supporting',
      mode,
      mode,
    ).once('message', () => {
      subscribeTo(lc, replicator);
      resolve();
    });
    await replicaReady;

    const notifier = createNotifierFrom(lc, replicator);
    for (let i = 0; i < numSyncers; i++) {
      syncers.push(loadWorker(SYNCER_URL, 'user-facing', i + 1, mode));
    }
    syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
  }
  let mutator: Worker | undefined;
  if (clientConnectionBifurcated) {
    mutator = loadWorker(MUTATOR_URL, 'supporting', 'mutator');
  }

  lc.info?.('waiting for workers to be ready ...');
  const logWaiting = setInterval(
    () => lc.info?.(`still waiting for ${processes.initializing().join(', ')}`),
    10_000,
  );
  await processes.allWorkersReady();
  clearInterval(logWaiting);
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);

  parent.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(
      lc,
      parent,
      new WorkerDispatcher(
        lc,
        taskID,
        parent,
        syncers,
        mutator,
        changeStreamer,
      ),
    );
  } catch (err) {
    processes.logErrorAndExit(err, 'dispatcher');
  }

  await processes.done();
}

if (!singleProcessMode()) {
  void exitAfter(() => runWorker(must(parentWorker), process.env));
}
