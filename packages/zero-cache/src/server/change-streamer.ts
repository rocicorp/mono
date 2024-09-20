import postgres from 'postgres';
import {must} from 'shared/src/must.js';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.js';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.js';
import {initializeChangeSource} from '../services/change-streamer/pg/change-source.js';
import {runOrExit} from '../services/runner.js';
import {postgresTypeConfig} from '../types/pg.js';
import {parentWorker, singleProcessMode, Worker} from '../types/processes.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const MAX_CHANGE_DB_CONNECTIONS = 5;

export default async function runWorker(parent: Worker) {
  const config = configFromEnv();
  const lc = createLogContext(config, {worker: 'change-streamer'});

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
  const {changeSource, replicationConfig} = await initializeChangeSource(
    lc,
    config.UPSTREAM_URI,
    config.REPLICA_ID,
    config.REPLICA_DB_FILE,
  );

  const changeStreamer = await initializeStreamer(
    lc,
    changeDB,
    changeSource,
    replicationConfig,
  );

  const changeStreamerWebServer = new ChangeStreamerHttpServer(
    lc,
    changeStreamer,
  );

  void runOrExit(lc, changeStreamer, changeStreamerWebServer);

  parent.send(['ready', {ready: true}]);
}

// fork()
if (!singleProcessMode()) {
  void runWorker(must(parentWorker));
}
