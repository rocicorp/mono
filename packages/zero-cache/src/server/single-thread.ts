import Fastify from 'fastify';
import {MessageChannel} from 'node:worker_threads';
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
import {Snapshotter} from '../services/view-syncer/snapshotter.js';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.js';
import {postgresTypeConfig} from '../types/pg.js';
import {Subscription} from '../types/subscription.js';
import {ReplicatorWorkerData, runAsWorker} from '../workers/replicator.js';
import {Syncer, SyncerWorkerData} from '../workers/syncer.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();
const lc = createLogContext(config, {thread: 'main'});

const parentToReplicator = new MessageChannel();
const replicatorToSyncer = new MessageChannel();
const parentToSyncer = new MessageChannel();

// Adapted from replicator.ts
const replicator = new ReplicatorService(
  lc.withContext('component', 'replicator'),
  config.REPLICA_ID, // TODO: Parameterize replicaID
  config.UPSTREAM_URI,
  config.REPLICA_DB_FILE,
);

void runAsWorker(replicator, parentToReplicator.port2, {
  subscriberPorts: [replicatorToSyncer.port1],
} satisfies ReplicatorWorkerData);

// Adapted from syncer.ts
const cvrDB = postgres(config.CVR_DB_URI, {
  ...postgresTypeConfig(),
  max: 5,
});

const upstreamDB = postgres(config.UPSTREAM_URI, {
  ...postgresTypeConfig(),
  max: 5,
});

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

const syncer = new Syncer(
  lc.withContext('component', 'syncer'),
  viewSyncerFactory,
  mutagenFactory,
  parentToSyncer.port2,
  {
    replicatorPort: replicatorToSyncer.port2,
  } satisfies SyncerWorkerData,
);
syncer.run();

// Adapted from main.ts
const fastify = Fastify();
fastify.server.on('upgrade', (msg, socket, head) =>
  syncer.handleUpgrade(msg, socket, head),
);
const address = await fastify.listen({port: 3000});
lc.info?.(`Server listening at ${address}`);
