import path from 'node:path';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {must} from '../../../shared/src/must.ts';
import {runsChangeStreamer} from '../config/normalize.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {initEventSink} from '../observability/events.ts';
import {
  exitAfter,
  ProcessManager,
  recordStartupDurationMs,
  runUntilKilled,
  type WorkerType,
} from '../services/life-cycle.ts';
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
  SHADOW_SYNCER_URL,
  SYNCER_URL,
} from './worker-urls.ts';

const clientConnectionBifurcated = false;

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getNormalizedZeroConfig({env});

  startOtelAuto(
    createLogContext(config, 'dispatcher', 0, false),
    'dispatcher',
    0,
  );
  lc = createLogContext(config, 'dispatcher');
  registerSQLiteCorruptionDiagnosticTarget(
    {
      debugName: 'dispatcher replica',
      dbPath: config.replica.file,
    },
    config.sqliteCorruptionChecks,
  );
  initEventSink(lc, config);

  const processes = new ProcessManager(lc, parent);

  const {numSyncWorkers: numSyncers} = config;
  if (config.enableCrudMutations && config.upstream.maxConns < numSyncers) {
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

  const {taskID, litestream} = config;
  const runChangeStreamer = runsChangeStreamer(config);
  const sqliteChangeLogEnabled =
    config.changeStreamer.sqliteChangeLogMode !== 'off';
  if (sqliteChangeLogEnabled && !runChangeStreamer) {
    // A multi-node deployment configures every task from one environment, so
    // the change log's options reach the view-syncers too. The log lives in
    // the change-streamer, and a task that connects to one simply never writes
    // a log. Warning rather than refusing to start is what makes the
    // fleet-wide environment -- often the only knob an operator has -- able to
    // express the rollout at all.
    lc.warn?.(
      `ignoring --change-streamer-sqlite-change-log-mode=` +
        `${config.changeStreamer.sqliteChangeLogMode}: this task connects to ` +
        `a change-streamer rather than running one, and the SQLite change log ` +
        `lives in the change-streamer`,
    );
  }

  let changeStreamer: Worker | undefined;

  if (runChangeStreamer) {
    const {promise: changeStreamerReady, resolve: changeStreamerStarted} =
      resolver();
    changeStreamer = loadWorker(CHANGE_STREAMER_URL, 'supporting').once(
      'message',
      changeStreamerStarted,
    );
    // Wait for the change-streamer to be ready to guarantee that a replica
    // file is present.
    await changeStreamerReady;
  }

  if (numSyncers > 0) {
    const {promise: reaperReady, resolve: reaperStarted} = resolver();
    loadWorker(REAPER_URL, 'supporting').once('message', reaperStarted);
    // Before starting the view-syncers, ensure that the reaper has started
    // up, indicating that any CVR db migrations have been performed.
    await reaperReady;
  }

  // Only run the shadow-sync canary on the replication-manager (or in
  // single-node mode, where it also owns upstream). Running on every
  // view-syncer would hammer the upstream with N redundant canaries.
  if (config.shadowSync.enabled && runChangeStreamer) {
    const {promise: shadowReady, resolve: shadowStarted} = resolver();
    loadWorker(SHADOW_SYNCER_URL, 'supporting').once('message', shadowStarted);
    await shadowReady;
  }

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
      syncers.push(loadWorker(SYNCER_URL, 'user-facing', i, mode, String(i)));
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
  const startupDurationMs = Date.now() - startMs;
  lc.info?.(`all workers ready (${startupDurationMs} ms)`);
  recordStartupDurationMs(startupDurationMs);

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
  } finally {
    await processes.shutdown();
  }
}

if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(must(parentWorker), process.env),
  );
}
