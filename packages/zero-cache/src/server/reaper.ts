import {must} from '../../../shared/src/must.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {CVRPurger} from '../services/view-syncer/cvr-purger.ts';
import {ActiveUsersGauge} from '../services/view-syncer/active-users-gauge.ts';
import {initViewSyncerSchema} from '../services/view-syncer/schema/init.ts';
import {pgClient} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardID} from '../types/shards.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {startAnonymousTelemetry} from './anonymous-otel-start.ts';

const MS_PER_HOUR = 1000 * 60 * 60;

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
  const config = getNormalizedZeroConfig({env, argv});

  startOtelAuto(createLogContext(config, {worker: 'reaper'}, false));
  const lc = createLogContext(config, {worker: 'reaper'}, true);
  initEventSink(lc, config);
  startAnonymousTelemetry(lc, config);

  const {cvr} = config;
  const shard = getShardID(config);
  const cvrDB = pgClient(lc, cvr.db, {
    connection: {['application_name']: `zero-sync-cvr-purger`},
  });
  await initViewSyncerSchema(lc, cvrDB, shard);
  parent.send(['ready', {ready: true}]);

  return runUntilKilled(
    lc,
    parent,
    new CVRPurger(lc, cvrDB, shard, {
      inactivityThresholdMs:
        cvr.garbageCollectionInactivityThresholdHours * MS_PER_HOUR,
      initialBatchSize: cvr.garbageCollectionInitialBatchSize,
      initialIntervalMs: cvr.garbageCollectionInitialIntervalSeconds * 1000,
    }),
    // Periodically computes and exports active users gauge to anonymous telemetry
    new ActiveUsersGauge(lc, cvrDB, shard, {
      // Default 10minutes refresh; can be made configurable later if needed
      updateIntervalMs: 10 * 60 * 1000,
    }),
  );
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
