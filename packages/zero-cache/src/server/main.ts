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

// Reserve 1 core for the replicator. The change-streamer is not CPU heavy.
const numSyncers = Math.max(1, availableParallelism() - 1);
const numWorkers = numSyncers + 2; // syncers + replicator + change-streamer

let numReady = 0;
const {promise: allReady, resolve: signalAllReady} = resolver<true>();

function loadWorker(module: string, id?: number): Worker {
  const worker = childWorker(module);
  const name = path.basename(module, '.ts') + (id ? ' #' + id : '');

  return worker
    .onceMessageType('ready', () => {
      lc.debug?.(`${name} ready (${Date.now() - startMs} ms)`);
      if (++numReady === numWorkers) {
        signalAllReady(true);
      }
    })
    .on('close', logErrorAndExit);
}

const {promise: changeStreamerReady, resolve: ready} = resolver();
const changeStreamer = loadWorker('./src/server/change-streamer.ts').once(
  'message',
  ready,
);

const syncers = Array.from({length: numSyncers}, (_, i) =>
  loadWorker('./src/server/syncer.ts', i + 1),
);

// Technically, setting up the CVR DB schema is the responsibility of the Syncer,
// but it is done here in the main thread because it is wasteful to have all of
// the Syncers attempt the migration in parallel.
const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  onnotice: () => {},
});
await initViewSyncerSchema(lc, cvrDB);
void cvrDB.end();

// Start the replicator after the change-streamer is running to avoid
// connect error messages and exponential backoff.
await changeStreamerReady;
const replicator = loadWorker('./src/server/replicator.ts').once(
  'message',
  () => subscribeTo(replicator),
);
const notifier = createNotifierFrom(lc, replicator);
syncers.forEach(syncer => handleSubscriptionsFrom(lc, syncer, notifier));

lc.info?.('waiting for workers to be ready ...');
if ((await orTimeout(allReady, 30_000)) === 'timed-out') {
  lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
} else {
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);
}

const workers: Workers = {changeStreamer, replicator, syncers};

const dispatcher = new Dispatcher(lc, () => workers);
try {
  await dispatcher.run();
} catch (err) {
  logErrorAndExit(err);
}
