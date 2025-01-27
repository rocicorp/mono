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
import {getSchema} from '../auth/load-schema.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {MutagenService} from '../services/mutagen/mutagen.ts';
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
import {Subscription} from '../types/subscription.ts';
import {replicaFileModeSchema, replicaFileName} from '../workers/replicator.ts';
import {Syncer} from '../workers/syncer.ts';
import {createLogContext} from './logging.ts';

function randomID() {
  return randInt(1, Number.MAX_SAFE_INTEGER).toString(36);
}

export default async function runWorker(
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

  const {schema, permissions} = await getSchema(config);
  assert(config.cvr.maxConnsPerWorker);
  assert(config.upstream.maxConnsPerWorker);

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
      .withContext('clientGroupID', id)
      .withContext('instance', randomID());
    return new ViewSyncerService(
      logger,
      must(config.taskID, 'main must set --task-id'),
      id,
      config.shard.id,
      cvrDB,
      new PipelineDriver(
        logger,
        new Snapshotter(logger, replicaFile),
        operatorStorage.createClientGroupStorage(id),
        id,
      ),
      sub,
      drainCoordinator,
      permissions,
    );
  };

  const mutagenFactory = (id: string) =>
    new MutagenService(
      lc.withContext('component', 'mutagen').withContext('clientGroupID', id),
      config.shard.id,
      id,
      upstreamDB,
      config,
      schema,
      permissions,
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
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
