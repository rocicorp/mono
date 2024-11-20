import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import {randInt} from '../../../shared/src/rand.js';
import {getAuthorizationConfig} from '../auth/load-authorization.js';
import {getZeroConfig} from '../config/zero-config.js';
import {MutagenService} from '../services/mutagen/mutagen.js';
import type {ReplicaState} from '../services/replicator/replicator.js';
import {DatabaseStorage} from '../services/view-syncer/database-storage.js';
import {DrainCoordinator} from '../services/view-syncer/drain-coordinator.js';
import {PipelineDriver} from '../services/view-syncer/pipeline-driver.js';
import {Snapshotter} from '../services/view-syncer/snapshotter.js';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.js';
import {pgClient} from '../types/pg.js';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {Subscription} from '../types/subscription.js';
import {replicaFileName, type ReplicaFileMode} from '../workers/replicator.js';
import {Syncer} from '../workers/syncer.js';
import {exitAfter, runUntilKilled} from './life-cycle.js';
import {createLogContext} from './logging.js';

export default async function runWorker(
  parent: Worker,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = args[0] as ReplicaFileMode;

  const config = getZeroConfig(args.slice(1));
  const authorizationConfig = await getAuthorizationConfig(config);
  assert(config.cvr.maxConnsPerWorker);
  assert(config.upstream.maxConnsPerWorker);

  const lc = createLogContext(config.log, {worker: 'syncer'});
  const replicaFile = replicaFileName(config.replicaFile, fileMode);
  lc.debug?.(`running view-syncer on ${replicaFile}`);

  const cvrDB = pgClient(lc, config.cvr.db, {
    max: config.cvr.maxConnsPerWorker,
    connection: {['application_name']: `zero-sync-worker-${pid}-cvr`},
  });

  const upstreamDB = pgClient(lc, config.upstream.db, {
    max: config.upstream.maxConnsPerWorker,
    connection: {['application_name']: `zero-sync-worker-${pid}-upstream`},
  });

  const dbWarmup = Promise.allSettled([
    ...Array.from({length: config.cvr.maxConnsPerWorker}, () =>
      cvrDB`SELECT 1`.simple().execute(),
    ),
    ...Array.from({length: config.upstream.maxConnsPerWorker}, () =>
      upstreamDB`SELECT 1`.simple().execute(),
    ),
  ]);

  const tmpDir = config.storageDBTmpDir ?? tmpdir();
  const operatorStorage = DatabaseStorage.create(
    lc,
    path.join(tmpDir, `sync-worker-${pid}-${randInt(1000000, 9999999)}`),
  );

  const viewSyncerFactory = (
    id: string,
    sub: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
  ) => {
    const logger = lc
      .withContext('component', 'view-syncer')
      .withContext('clientGroupID', id);
    return new ViewSyncerService(
      logger,
      id,
      config.shard.id,
      cvrDB,
      new PipelineDriver(
        logger,
        new Snapshotter(logger, replicaFile),
        operatorStorage.createClientGroupStorage(id),
      ),
      sub,
      drainCoordinator,
    );
  };

  const mutagenFactory = (id: string) =>
    new MutagenService(
      lc.withContext('component', 'mutagen').withContext('clientGroupID', id),
      config.shard.id,
      id,
      upstreamDB,
      config,
      authorizationConfig,
    );

  const syncer = new Syncer(
    lc,
    config,
    viewSyncerFactory,
    mutagenFactory,
    parent,
  );

  void dbWarmup.then(() => parent.send(['ready', {ready: true}]));

  return runUntilKilled(lc, parent, syncer);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() => runWorker(must(parentWorker), ...process.argv.slice(2)));
}
