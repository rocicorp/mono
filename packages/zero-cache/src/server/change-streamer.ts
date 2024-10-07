import {must} from 'shared/src/must.js';
import {getZeroConfig} from '../config/zero-config.js';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.js';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.js';
import {initializeChangeSource} from '../services/change-streamer/pg/change-source.js';
import {pgClient} from '../types/pg.js';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {exitAfter, runUntilKilled} from './life-cycle.js';
import {createLogContext} from './logging.js';

const MAX_CHANGE_DB_CONNECTIONS = 5;

export default async function runWorker(parent: Worker): Promise<void> {
  const config = await getZeroConfig();
  const lc = createLogContext(config.log, {worker: 'change-streamer'});

  // Kick off DB connection warmup in the background.
  const changeDB = pgClient(lc, config.changeDBConnStr, {
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
    config.upstreamDBConnStr,
    config.shard,
    config.replicaDBFile,
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

  parent.send(['ready', {ready: true}]);

  return runUntilKilled(lc, parent, changeStreamer, changeStreamerWebServer);
}

// fork()
if (!singleProcessMode()) {
  exitAfter(runWorker(must(parentWorker)));
}
