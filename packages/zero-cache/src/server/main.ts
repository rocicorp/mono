import {resolver} from '@rocicorp/resolver';
import {availableParallelism} from 'node:os';
import path from 'node:path';
import {must} from '../../../shared/src/must.ts';
import {assertNormalized} from '../config/normalize.ts';
import {getZeroConfig} from '../config/zero-config.ts';
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
import {initViewSyncerSchema} from '../services/view-syncer/schema/init.ts';
import {pgClient} from '../types/pg.ts';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardID} from '../types/shards.ts';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {WorkerDispatcher} from './worker-dispatcher.ts';

const clientConnectionBifurcated = false;

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getZeroConfig({env});
  assertNormalized(config);
  const lc = createLogContext(config, {worker: 'dispatcher'});

  const processes = new ProcessManager(lc, parent);

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

  const shard = getShardID(config);
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
        lc.error?.('error restoring backup. resyncing the replica.');
      } else {
        // View-syncers, on the other hand, have no option other than to retry
        // until a valid backup has been published. This is achieved by
        // shutting down and letting the container runner retry with its
        // configured policy.
        throw e;
      }
    }
  }

  const {promise: changeStreamerReady, resolve} = resolver();
  const changeStreamer = runChangeStreamer
    ? loadWorker(
        './server/change-streamer.ts',
        'supporting',
        undefined,
        String(restoreStart.getTime()),
      ).once('message', resolve)
    : (resolve() ?? undefined);

  if (numSyncers) {
    // Technically, setting up the CVR DB schema is the responsibility of the Syncer,
    // but it is done here in the main thread because it is wasteful to have all of
    // the Syncers attempt the migration in parallel.
    const {cvr} = config;
    const cvrDB = pgClient(lc, cvr.db);
    await initViewSyncerSchema(lc, cvrDB, shard);
    void cvrDB.end();
  }

  // Wait for the change-streamer to be ready to guarantee that a replica
  // file is present.
  await changeStreamerReady;

  if (runChangeStreamer && litestream.backupURL) {
    // Start a backup replicator and corresponding litestream backup process.
    const {promise: backupReady, resolve} = resolver();
    const mode: ReplicaFileMode = 'backup';
    loadWorker('./server/replicator.ts', 'supporting', mode, mode).once(
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

  const syncers: Worker[] = [];
  if (numSyncers) {
    const mode: ReplicaFileMode =
      runChangeStreamer && litestream.backupURL ? 'serving-copy' : 'serving';
    const {promise: replicaReady, resolve} = resolver();
    const replicator = loadWorker(
      './server/replicator.ts',
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
      syncers.push(
        loadWorker('./server/syncer.ts', 'user-facing', i + 1, mode),
      );
    }
    syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
  }
  let mutator: Worker | undefined;
  if (clientConnectionBifurcated) {
    mutator = loadWorker('./server/mutator.ts', 'supporting', 'mutator');
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
