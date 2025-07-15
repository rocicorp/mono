import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {randInt} from '../../../shared/src/rand.ts';
import * as v from '../../../shared/src/valita.ts';
import {assertNormalized} from '../config/normalize.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {warmupConnections} from '../db/warmup.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import {DatabaseStorage} from '../services/view-syncer/database-storage.ts';
import {DrainCoordinator} from '../services/view-syncer/drain-coordinator.ts';
import {PipelineDriver} from '../services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {ViewSyncerService} from '../services/view-syncer/view-syncer.ts';
import {pgClient} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardID} from '../types/shards.ts';
import {Subscription} from '../types/subscription.ts';
import {replicaFileModeSchema, replicaFileName} from '../workers/replicator.ts';
import {Syncer} from '../workers/syncer.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {startAnonymousTelemetry} from './anonymous-otel-start.ts';

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getZeroConfig({env, argv: args.slice(1)});
  assertNormalized(config);
  startOtelAuto();

  const lc = createLogContext(config, {worker: 'syncer'});

  // Start telemetry in all workers
  startAnonymousTelemetry(lc, config);

  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const {cvr, upstream} = config;
  assert(cvr.maxConnsPerWorker);
  assert(upstream.maxConnsPerWorker);

  const replicaFile = replicaFileName(config.replica.file, fileMode);
  lc.debug?.(`running view-syncer on ${replicaFile}`);

  const cvrDB = pgClient(lc, cvr.db, {
    max: cvr.maxConnsPerWorker,
    connection: {['application_name']: `zero-sync-worker-${pid}-cvr`},
  });

  const upstreamDB = pgClient(lc, upstream.db, {
    max: upstream.maxConnsPerWorker,
    connection: {['application_name']: `zero-sync-worker-${pid}-upstream`},
  });

  const dbWarmup = Promise.allSettled([
    warmupConnections(lc, cvrDB, 'cvr'),
    warmupConnections(lc, upstreamDB, 'upstream'),
  ]);

  const tmpDir = config.storageDBTmpDir ?? tmpdir();
  const operatorStorage = DatabaseStorage.create(
    lc,
    path.join(tmpDir, `sync-worker-${pid}-${randInt(1000000, 9999999)}`),
  );

  const shard = getShardID(config);

  const viewSyncerFactory = (
    id: string,
    sub: Subscription<ReplicaState>,
    drainCoordinator: DrainCoordinator,
  ) => {
    const logger = lc
      .withContext('component', 'view-syncer')
      .withContext('clientGroupID', id)
      .withContext('instance', randomID());
    lc.debug?.(`creating view syncer`);
    return new ViewSyncerService(
      config.query,
      logger,
      shard,
      config.taskID,
      id,
      cvrDB,
      new PipelineDriver(
        logger,
        config.log,
        new Snapshotter(logger, replicaFile, shard),
        shard,
        operatorStorage.createClientGroupStorage(id),
        id,
      ),
      sub,
      drainCoordinator,
      config.log.slowHydrateThreshold,
      undefined,
      config.targetClientRowCount,
    );
  };

  const mutagenFactory = (id: string) =>
    new MutagenService(
      lc.withContext('component', 'mutagen').withContext('clientGroupID', id),
      shard,
      id,
      upstreamDB,
      config,
    );

  const pusherFactory =
    config.push.url === undefined && config.mutate.url === undefined
      ? undefined
      : (id: string) =>
          new PusherService(
            config,
            {
              ...config.push,
              ...config.mutate,
              url: must(
                config.push.url ?? config.mutate.url,
                'No push or mutate URL configured',
              ),
            },
            lc.withContext('clientGroupID', id),
            id,
          );

  const syncer = new Syncer(
    lc,
    config,
    viewSyncerFactory,
    mutagenFactory,
    pusherFactory,
    parent,
  );

  void dbWarmup.then(() => parent.send(['ready', {ready: true}]));

  return runUntilKilled(lc, parent, syncer);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
