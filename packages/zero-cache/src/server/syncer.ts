import {parentPort, workerData} from 'node:worker_threads';
import postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {MutagenService} from '../services/mutagen/mutagen.js';
import {ReplicaVersionReady} from '../services/replicator/replicator.js';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.js';
import {postgresTypeConfig} from '../types/pg.js';
import {Subscription} from '../types/subscription.js';
import {Syncer} from '../workers/syncer.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();
assert(parentPort);

// Consider parameterizing these (in main) based on total number of workers.
const MAX_CVR_CONNECTIONS = 10;
const MAX_MUTAGEN_CONNECTIONS = 5;

const lc = createLogContext(config, {thread: 'syncer'});

const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  max: MAX_CVR_CONNECTIONS,
});

const upstreamDB = postgres(config.UPSTREAM_URI, {
  ...postgresTypeConfig(),
  max: MAX_MUTAGEN_CONNECTIONS,
});

const viewSyncerFactory = (
  id: string,
  sub: Subscription<ReplicaVersionReady>,
) => new ViewSyncerService(lc, id, cvrDB, config.REPLICA_DB_FILE, sub);

const mutagenFactory = (id: string) => new MutagenService(lc, id, upstreamDB);

new Syncer(lc, viewSyncerFactory, mutagenFactory, parentPort, workerData).run();

lc.info?.('started Syncer');
