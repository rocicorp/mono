import {must} from '../../../shared/src/must.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {CVRPurger} from '../services/view-syncer/cvr-purger.ts';
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
    new CVRPurger(
      lc,
      cvrDB,
      shard,
      cvr.garbageCollectionInactivityThresholdHours * MS_PER_HOUR,
    ),
  );
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
