import {resolver} from '@rocicorp/resolver';
import {availableParallelism} from 'node:os';
import path from 'node:path';
import postgres from 'postgres';
import {Dispatcher, Workers} from '../services/dispatcher/dispatcher.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/pg-migrations.js';
import {postgresTypeConfig} from '../types/pg.js';
import {childWorker, Worker} from '../types/processes.js';
import {orTimeout} from '../types/timeout.js';
import {
  createNotifierFrom,
  handleSubscriptionsFrom,
  ReplicatorMode,
  subscribeTo,
} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const startMs = Date.now();
const config = configFromEnv();
const lc = createLogContext(config, {worker: 'dispatcher'});

function logErrorAndExit(err: unknown) {
  lc.error?.(err);
  process.exit(1);
}

const ready: Promise<void>[] = [];

function loadWorker(
  module: string,
  id?: string | number,
  ...args: string[]
): Worker {
  const worker = childWorker(module, ...args);
  const name = path.basename(module, '.ts') + (id ? ` (${id})` : '');
  const {promise, resolve} = resolver();
  ready.push(promise);

  return worker
    .onceMessageType('ready', () => {
      lc.debug?.(`${name} ready (${Date.now() - startMs} ms)`);
      resolve();
    })
    .on('close', logErrorAndExit);
}

const {promise: changeStreamerReady, resolve} = resolver();
const changeStreamer = config.CHANGE_STREAMER_URI
  ? resolve()
  : loadWorker('./src/server/change-streamer.ts').once('message', resolve);

const numSyncers = config.NUM_SYNC_WORKERS
  ? Number(config.NUM_SYNC_WORKERS)
  : // Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
    Math.max(1, availableParallelism() - 1);

const syncers = Array.from({length: numSyncers}, (_, i) =>
  loadWorker('./src/server/syncer.ts', i + 1),
);

if (numSyncers) {
  // Technically, setting up the CVR DB schema is the responsibility of the Syncer,
  // but it is done here in the main thread because it is wasteful to have all of
  // the Syncers attempt the migration in parallel.
  const cvrDB = postgres(config.CVR_DB_URI, {
    ...postgresTypeConfig(),
    onnotice: () => {},
  });
  await initViewSyncerSchema(lc, cvrDB);
  void cvrDB.end();
}

// Start the replicator after the change-streamer is running to avoid
// connect error messages and exponential backoff.
await changeStreamerReady;

if (config.LITESTREAM) {
  const mode: ReplicatorMode = 'backup';
  const replicator = loadWorker('./src/server/replicator.ts', mode, mode).once(
    'message',
    () => subscribeTo(replicator),
  );
  const notifier = createNotifierFrom(lc, replicator);
  if (changeStreamer) {
    handleSubscriptionsFrom(lc, changeStreamer, notifier);
  }
}

if (numSyncers) {
  const mode: ReplicatorMode = config.LITESTREAM ? 'serving-copy' : 'serving';
  const replicator = loadWorker('./src/server/replicator.ts', mode, mode).once(
    'message',
    () => subscribeTo(replicator),
  );
  const notifier = createNotifierFrom(lc, replicator);
  syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));
}

lc.info?.('waiting for workers to be ready ...');
if ((await orTimeout(Promise.all(ready), 30_000)) === 'timed-out') {
  lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
} else {
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);
}

if (numSyncers) {
  const workers: Workers = {syncers};

  const dispatcher = new Dispatcher(lc, () => workers);
  try {
    await dispatcher.run();
  } catch (err) {
    logErrorAndExit(err);
  }
}
