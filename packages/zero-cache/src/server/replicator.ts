import postgres from 'postgres';
import {must} from 'shared/src/must.js';
import {Database} from 'zqlite/src/db.js';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.js';
import {initializeChangeSource} from '../services/change-streamer/pg/change-source.js';
import {ReplicatorService} from '../services/replicator/replicator.js';
import {postgresTypeConfig} from '../types/pg.js';
import {parentWorker, singleProcessMode, Worker} from '../types/processes.js';
import {runAsWorker} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const MAX_CHANGE_DB_CONNECTIONS = 5;

export default async function runWorker(parent: Worker) {
  const config = configFromEnv();
  const lc = createLogContext(config, {worker: 'replicator'});

  // Kick off DB connection warmup in the background.
  const changeDB = postgres(config.CHANGE_DB_URI, {
    ...postgresTypeConfig(),
    max: MAX_CHANGE_DB_CONNECTIONS,
  });
  void Promise.allSettled(
    Array.from({length: MAX_CHANGE_DB_CONNECTIONS}, () =>
      changeDB`SELECT 1`.simple().execute(),
    ),
  );

  // Note: This performs initial sync of the replica if necessary.
  const replicationStream = await initializeChangeSource(
    lc,
    config.UPSTREAM_URI,
    config.REPLICA_ID,
    config.REPLICA_DB_FILE,
  );

  const replica = new Database(lc, config.REPLICA_DB_FILE);
  replica.pragma('journal_mode = WAL');

  const changeStreamer = await initializeStreamer(
    lc,
    changeDB,
    replicationStream,
    replica,
  );

  void changeStreamer.run();

  const replicator = new ReplicatorService(
    lc,
    config.TASK_ID ?? 'z1', // To eventually accommodate multiple zero-caches.
    changeStreamer,
    replica,
  );

  void runAsWorker(replicator, parent);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }
}

// fork()
if (!singleProcessMode()) {
  void runWorker(must(parentWorker));
}
