import {randomUUID} from 'node:crypto';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {randInt} from '../../../shared/src/rand.ts';
import * as v from '../../../shared/src/valita.ts';
import {DatabaseStorage} from '../../../zqlite/src/database-storage.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {CustomQueryTransformer} from '../custom-queries/transform-query.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink} from '../observability/events.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import type {ReplicaState} from '../services/replicator/replicator.ts';
import type {DrainCoordinator} from '../services/view-syncer/drain-coordinator.ts';
import {PipelineDriver} from '../services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../services/view-syncer/snapshotter.ts';
import {
  isPriorityOpRunning,
  ViewSyncerService,
} from '../services/view-syncer/view-syncer.ts';
import {pgClient} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardID} from '../types/shards.ts';
import type {Subscription} from '../types/subscription.ts';
import {replicaFileModeSchema, replicaFileName} from '../workers/replicator.ts';
import {Syncer} from '../workers/syncer.ts';
import {startAnonymousTelemetry} from './anonymous-otel-start.ts';
import {InspectorDelegate} from './inspector-delegate.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

function getCustomQueryConfig(
  config: Pick<NormalizedZeroConfig, 'query' | 'getQueries'>,
) {
  const queryConfig = config.query?.url ? config.query : config.getQueries;

  if (!queryConfig?.url) {
    return undefined;
  }

  return {
    url: queryConfig.url,
    forwardCookies: queryConfig.forwardCookies ?? false,
  };
}

export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});

  startOtelAuto(createLogContext(config, {worker: 'syncer'}, false));
  const lc = createLogContext(config, {worker: 'syncer'}, true);
  initEventSink(lc, config);

  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const {cvr, upstream} = config;
  assert(cvr.maxConnsPerWorker, 'cvr.maxConnsPerWorker must be set');
  assert(upstream.maxConnsPerWorker, 'upstream.maxConnsPerWorker must be set');

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
    path.join(tmpDir, `sync-worker-${randomUUID()}`),
  );
  const writeAuthzStorage = DatabaseStorage.create(
    lc,
    path.join(tmpDir, `mutagen-${randomUUID()}`),
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
    lc.debug?.(
      `creating view syncer. Query Planner Enabled: ${config.enableQueryPlanner}`,
    );

    // Create the custom query transformer if configured
    const customQueryConfig = getCustomQueryConfig(config);
    const customQueryTransformer =
      customQueryConfig &&
      new CustomQueryTransformer(logger, customQueryConfig, shard);

    const inspectorDelegate = new InspectorDelegate(customQueryTransformer);

    const priorityOpRunningYieldThresholdMs = Math.max(
      config.yieldThresholdMs / 4,
      2,
    );
    const normalYieldThresholdMs = Math.max(config.yieldThresholdMs, 2);
    return new ViewSyncerService(
      config,
      logger,
      shard,
      config.taskID,
      id,
      cvrDB,
      config.upstream.type === 'pg' ? upstreamDB : undefined,
      new PipelineDriver(
        logger,
        config.log,
        new Snapshotter(
          logger,
          replicaFile,
          shard,
          config.replica.pageCacheSizeKib,
        ),
        shard,
        operatorStorage.createClientGroupStorage(id),
        id,
        inspectorDelegate,
        () =>
          isPriorityOpRunning()
            ? priorityOpRunningYieldThresholdMs
            : normalYieldThresholdMs,
        config.enableQueryPlanner,
      ),
      sub,
      drainCoordinator,
      config.log.slowHydrateThreshold,
      inspectorDelegate,
      customQueryTransformer,
    );
  };

  const mutagenFactory = (id: string) =>
    new MutagenService(
      lc.withContext('component', 'mutagen').withContext('clientGroupID', id),
      shard,
      id,
      upstreamDB,
      config,
      writeAuthzStorage,
    );

  const pusherFactory =
    config.push.url === undefined && config.mutate.url === undefined
      ? undefined
      : (id: string) =>
          new PusherService(
            upstreamDB,
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

  startAnonymousTelemetry(lc, config);

  void dbWarmup.then(() => parent.send(['ready', {ready: true}]));

  return runUntilKilled(lc, parent, syncer);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
