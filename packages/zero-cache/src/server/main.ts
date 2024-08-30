import {resolver} from '@rocicorp/resolver';
import {availableParallelism} from 'node:os';
import postgres from 'postgres';
import {sleep} from 'shared/src/sleep.js';
import {Dispatcher, Workers} from '../services/dispatcher/dispatcher.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/pg-migrations.js';
import {postgresTypeConfig} from '../types/pg.js';
import {childWorker, getMessage, Worker} from '../types/processes.js';
import {createNotifier} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const startMs = Date.now();
const config = configFromEnv();
const lc = createLogContext(config, {worker: 'dispatcher'});

function logErrorAndExit(err: unknown) {
  lc.error?.(err);
  process.exit(1);
}

let numReady = 0;
const {promise: allReady, resolve: signalAllReady} = resolver<true>();

function handleReady(worker: Worker, name: string, id?: number): Worker {
  const handler = (data: unknown) => {
    if (getMessage('ready', data)) {
      // Similar to once('message') but only after receiving the 'ready' signal.
      worker.off('message', handler);
      lc.debug?.(
        `${name}${id ? ' #' + id : ''} ready (${Date.now() - startMs} ms)`,
      );
      if (++numReady === numSyncers + 1) {
        signalAllReady(true);
      }
    }
  };
  return worker.on('message', handler);
}

const replicator = childWorker('./src/server/replicator.ts');
handleReady(replicator, 'replicator').on('close', logErrorAndExit);

const numSyncers = Math.max(1, availableParallelism() - 1); // Reserve 1 for the Replicator

// Create a Notifier from a subscription to the Replicator,
// and relay notifications to all subscriptions from syncers.
const notifier = createNotifier(replicator);

const syncers = Array.from({length: numSyncers}, (_, i) => {
  const syncer = childWorker('./src/server/syncer.ts');
  handleReady(syncer, 'syncer', i + 1)
    .on('message', async data => {
      if (getMessage('subscribe', data)) {
        const subscription = notifier.addSubscription();
        for await (const msg of subscription) {
          syncer.send(['notify', msg]);
        }
      }
    })
    .on('close', logErrorAndExit);
  return syncer;
});

// Technically, setting up the CVR DB schema is the responsibility of the Syncer,
// but it is done here in the main thread because it is wasteful to have all of
// the Syncers attempt the migration in parallel.
const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  onnotice: () => {},
});
await initViewSyncerSchema(lc, 'view-syncer', 'cvr', cvrDB);
void cvrDB.end();

lc.info?.('waiting for workers to be ready ...');
if (await Promise.race([allReady, sleep(30_000)])) {
  lc.info?.(`all workers ready (${Date.now() - startMs} ms)`);
} else {
  lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
}

const workers: Workers = {replicator, syncers};

const dispatcher = new Dispatcher(lc, () => workers);
try {
  await dispatcher.run();
} catch (err) {
  logErrorAndExit(err);
}
