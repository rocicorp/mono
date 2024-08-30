import Fastify from 'fastify';
import {tmpdir} from 'os';
import path from 'path';
import postgres from 'postgres';
import {randInt} from 'shared/src/rand.js';
import {threadId} from 'worker_threads';
import {MutagenService} from '../services/mutagen/mutagen.js';
import {
  ReplicatorService,
  ReplicaVersionReady,
} from '../services/replicator/replicator.js';
import {DatabaseStorage} from '../services/view-syncer/database-storage.js';
import {PipelineDriver} from '../services/view-syncer/pipeline-driver.js';
import {initViewSyncerSchema} from '../services/view-syncer/schema/pg-migrations.js';
import {Snapshotter} from '../services/view-syncer/snapshotter.js';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.js';
import {postgresTypeConfig} from '../types/pg.js';
import {getMessage, inProcChannel} from '../types/processes.js';
import {Subscription} from '../types/subscription.js';
import {createNotifierFrom, runAsWorker} from '../workers/replicator.js';
import {Syncer} from '../workers/syncer.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();
const lc = createLogContext(config, {worker: 'single-thread'});

const [replicatorParent, replicatorChannel] = inProcChannel();
const [syncerParent, syncerChannel] = inProcChannel();

// Adapted from replicator.ts
const replicator = new ReplicatorService(
  lc.withContext('component', 'replicator'),
  config.REPLICA_ID, // TODO: Parameterize replicaID
  config.UPSTREAM_URI,
  config.REPLICA_DB_FILE,
);

void runAsWorker(replicator, replicatorParent);

// Adapted from syncer.ts
const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  max: 5,
});

const upstreamDB = postgres(config.UPSTREAM_URI, {
  ...postgresTypeConfig(),
  max: 5,
});

await initViewSyncerSchema(lc, 'view-syncer', 'cvr', cvrDB);

const tmpDir = config.STORAGE_DB_TMP_DIR ?? tmpdir();
const operatorStorage = DatabaseStorage.create(
  lc.withContext('component', 'syncer'),
  path.join(tmpDir, `sync-worker-${threadId}-${randInt(1000000, 9999999)}`),
);

const viewSyncerFactory = (
  id: string,
  sub: Subscription<ReplicaVersionReady>,
) =>
  new ViewSyncerService(
    lc.withContext('component', 'syncer'),
    id,
    cvrDB,
    new PipelineDriver(
      lc,
      new Snapshotter(lc, config.REPLICA_DB_FILE),
      operatorStorage.createClientGroupStorage(id),
    ),
    sub,
  );

const mutagenFactory = (id: string) => new MutagenService(lc, id, upstreamDB);

// Create a Notifier from a subscription to the Replicator,
// and relay notifications to all subscriptions from syncers.
const notifier = createNotifierFrom(replicatorChannel);
syncerChannel.on('message', async data => {
  if (getMessage('subscribe', data)) {
    const subscription = notifier.subscribe();
    for await (const msg of subscription) {
      syncerChannel.send(['notify', msg]);
    }
  }
});

const syncer = new Syncer(
  lc.withContext('component', 'syncer'),
  viewSyncerFactory,
  mutagenFactory,
  syncerParent,
);
syncer.run();

// Adapted from main.ts
const fastify = Fastify();
fastify.server.on('upgrade', (msg, socket, head) =>
  syncer.handleUpgrade(msg, socket, head),
);
const address = await fastify.listen({port: 3000});
lc.info?.(`Server listening at ${address}`);
