import {resolver} from '@rocicorp/resolver';
import 'dotenv/config';
import {availableParallelism} from 'node:os';
import path from 'node:path';
import {getZeroConfig} from '../config/zero-config.js';
import {Dispatcher, type Workers} from '../services/dispatcher/dispatcher.js';
import type {Service} from '../services/service.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/init.js';
import {pgClient} from '../types/pg.js';
import {childWorker, type Worker} from '../types/processes.js';
import {orTimeout} from '../types/timeout.js';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  type ReplicaFileMode,
  subscribeTo,
} from '../workers/replicator.js';
import {
  HeartbeatMonitor,
  runUntilKilled,
  Terminator,
  type WorkerType,
} from './life-cycle.js';
import {createLogContext} from './logging.js';
import {getTaskID} from './runtime.js';

const startMs = Date.now();
const config = getZeroConfig();
const lc = createLogContext(config.log, {worker: 'dispatcher'});

const terminator = new Terminator(lc);
const ready: Promise<void>[] = [];

const numSyncers =
  config.numSyncWorkers !== undefined
    ? config.numSyncWorkers
    : // Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
      Math.max(1, availableParallelism() - 1);

if (config.upstream.maxConns < numSyncers) {
  throw new Error(
    `insufficient upstream connections (${config.upstream.maxConns}) for ${numSyncers} syncers`,
  );
}
if (config.cvr.maxConns < numSyncers) {
  throw new Error(
    `insufficient cvr connections (${config.cvr.maxConns}) for ${numSyncers} syncers`,
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

let {taskID} = config;
if (!taskID) {
  taskID = await getTaskID(lc);
  internalFlags.push('--task-id', taskID);
}
lc.info?.(`starting task ${taskID}`);

function loadWorker(
  modulePath: string,
  type: WorkerType,
  id?: string | number,
  ...args: string[]
): Worker {
  const worker = childWorker(modulePath, undefined, ...args, ...internalFlags);
  const name = path.basename(modulePath) + (id ? ` (${id})` : '');
  const {promise, resolve} = resolver();
  ready.push(promise);

  return terminator.addWorker(worker, type).onceMessageType('ready', () => {
    lc.debug?.(`${name} ready (${Date.now() - startMs} ms)`);
    resolve();
  });
}

const {promise: changeStreamerReady, resolve} = resolver();
const changeStreamer = config.changeStreamerURI
  ? resolve()
  : loadWorker('./server/change-streamer.ts', 'supporting').once(
      'message',
      resolve,
    );

if (numSyncers) {
  // Technically, setting up the CVR DB schema is the responsibility of the Syncer,
  // but it is done here in the main thread because it is wasteful to have all of
  // the Syncers attempt the migration in parallel.
  const cvrDB = pgClient(lc, config.cvr.db);
  await initViewSyncerSchema(lc, cvrDB);
  void cvrDB.end();
}

// Start the replicator after the change-streamer is running to avoid
// connect error messages and exponential backoff.
await changeStreamerReady;

if (config.litestream) {
  const mode: ReplicaFileMode = 'backup';
  const replicator = loadWorker(
    './server/replicator.ts',
    'supporting',
    mode,
    mode,
  ).once('message', () => subscribeTo(lc, replicator));
  const notifier = createNotifierFrom(lc, replicator);
  if (changeStreamer) {
    handleSubscriptionsFrom(lc, changeStreamer, notifier);
  }
}

const syncers: Worker[] = [];
if (numSyncers) {
  const mode: ReplicaFileMode = config.litestream ? 'serving-copy' : 'serving';
  const replicator = loadWorker(
    './server/replicator.ts',
    'supporting',
    mode,
    mode,
  ).once('message', () => subscribeTo(lc, replicator));
  const notifier = createNotifierFrom(lc, replicator);
  for (let i = 0; i < numSyncers; i++) {
    syncers.push(loadWorker('./server/syncer.ts', 'user-facing', i + 1, mode));
  }
  syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
}

lc.info?.('waiting for workers to be ready ...');
if ((await orTimeout(Promise.all(ready), 30_000)) === 'timed-out') {
  lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
} else {
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);
}

const {port} = config;
const heartbeatMonitorPort = config.heartbeatMonitorPort ?? port + 2;

const mainServices: Service[] = [
  new HeartbeatMonitor(lc, {port: heartbeatMonitorPort}),
];

if (numSyncers) {
  const workers: Workers = {syncers};
  mainServices.push(new Dispatcher(lc, () => workers, {port}));
}

try {
  await runUntilKilled(lc, process, ...mainServices);
} catch (err) {
  terminator.logErrorAndExit(err);
}
