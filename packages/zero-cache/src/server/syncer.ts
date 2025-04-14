import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {Resource} from '@opentelemetry/resources';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {pid} from 'node:process';
import {NoopSpanExporter} from '../../../otel/src/noop-span-exporter.ts';
import {version} from '../../../otel/src/version.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {randInt} from '../../../shared/src/rand.ts';
import * as v from '../../../shared/src/valita.ts';
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

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getZeroConfig(env, args.slice(1));
  const lc = createLogContext(config, {worker: 'syncer'});

  const {traceCollector} = config.log;
  if (!traceCollector) {
    lc.warn?.('trace collector not set');
  } else {
    lc.debug?.(`trace collector: ${traceCollector}`);
  }

  const sdk = new NodeSDK({
    resource: new Resource({
      [ATTR_SERVICE_NAME]: 'syncer',
      [ATTR_SERVICE_VERSION]: version,
    }),
    traceExporter:
      config.log.traceCollector === undefined
        ? new NoopSpanExporter()
        : new OTLPTraceExporter({
            url: config.log.traceCollector,
          }),
  });
  sdk.start();

  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const {cvr, upstream} = config;
  assert(cvr.maxConnsPerWorker);
  assert(upstream.maxConnsPerWorker);

  const replicaFile = replicaFileName(config.replica.file, fileMode);
  lc.debug?.(`running view-syncer on ${replicaFile}`);

  const cvrDB = pgClient(lc, cvr.db ?? upstream.db, {
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
    return new ViewSyncerService(
      logger,
      shard,
      must(config.taskID, 'main must set --task-id'),
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
    config.push.url === undefined
      ? undefined
      : (id: string) =>
          new PusherService(
            config,
            lc.withContext('clientGroupID', id),
            id,
            must(config.push.url),
            config.push.apiKey,
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
