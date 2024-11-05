import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {must} from '../../../shared/src/must.js';
import {randInt} from '../../../shared/src/rand.js';
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
import {Syncer} from '../workers/syncer.js';
import {exitAfter, runUntilKilled} from './life-cycle.js';
import {createLogContext} from './logging.js';

export default async function runWorker(parent: Worker): Promise<void> {
  const config = await getZeroConfig();

  // Consider parameterizing these (in main) based on total number of workers.
  const MAX_CVR_CONNECTIONS = 5;
  const MAX_MUTAGEN_CONNECTIONS = 5;

  const lc = createLogContext(config.log, {worker: 'syncer'});

  const cvrDB = pgClient(lc, config.cvrDB, {
    max: MAX_CVR_CONNECTIONS,
  });

  const upstreamDB = pgClient(lc, config.upstreamDB, {
    max: MAX_MUTAGEN_CONNECTIONS,
  });

  const dbWarmup = Promise.allSettled([
    ...Array.from({length: MAX_CVR_CONNECTIONS}, () =>
      cvrDB`SELECT 1`.simple().execute(),
    ),
    ...Array.from({length: MAX_MUTAGEN_CONNECTIONS}, () =>
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
  ) =>
    new ViewSyncerService(
      lc,
      id,
      config.shard.id,
      cvrDB,
      new PipelineDriver(
        lc,
        new Snapshotter(lc, config.replicaFile),
        operatorStorage.createClientGroupStorage(id),
      ),
      sub,
      drainCoordinator,
    );

  const mutagenFactory = (id: string) =>
    new MutagenService(lc, config.shard.id, id, upstreamDB, config);

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
  void exitAfter(() => runWorker(must(parentWorker)));
}
