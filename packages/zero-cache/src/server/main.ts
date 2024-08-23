import {cpus} from 'node:os';
import {SHARE_ENV, Worker} from 'node:worker_threads';
import postgres from 'postgres';
import {Dispatcher, Workers} from '../services/dispatcher/dispatcher.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/pg-migrations.js';
import {postgresTypeConfig} from '../types/pg.js';
import {ReplicatorWorkerData} from '../workers/replicator.js';
import {SyncerWorkerData} from '../workers/syncer.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();
const lc = createLogContext(config, {thread: 'main'});

function logErrorAndExit(err: unknown) {
  lc.error?.(err);
  process.exit(1);
}

const numSyncers = Math.max(1, cpus().length - 1 /* one for replicator */);
const syncerReplicatorChannels = Array.from(
  {length: numSyncers},
  () => new MessageChannel(),
);

const subscriberPorts = syncerReplicatorChannels.map(c => c.port1);

const replicator = new Worker('./src/server/replicator.ts', {
  env: SHARE_ENV,
  workerData: {subscriberPorts} satisfies ReplicatorWorkerData,
  transferList: [...subscriberPorts],
}).on('error', logErrorAndExit);

const syncers = syncerReplicatorChannels.map(c =>
  new Worker('./src/server/syncer.ts', {
    env: SHARE_ENV,
    workerData: {replicatorPort: c.port2} satisfies SyncerWorkerData,
    transferList: [c.port2],
  }).on('error', logErrorAndExit),
);

const workers: Workers = {replicator, syncers};

// Technically, setting up the CVR DB schema is the responsibility of the Syncer,
// but it is done here in the main thread because:
// * it is wasteful to have all of the Syncers attempt the migration in parallel
// * we want to delay accepting requests (and eventually advertising health)
//   until initialization logic is complete.
const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  onnotice: () => {},
});
await initViewSyncerSchema(lc, 'view-syncer', 'cvr', cvrDB);
void cvrDB.end();

const dispatcher = new Dispatcher(lc, () => workers);
try {
  await dispatcher.run();
} catch (err) {
  logErrorAndExit(err);
}
