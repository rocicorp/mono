import {assert} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import {getZeroConfig} from '../config/zero-config.js';
import {deleteLiteDB} from '../db/delete-lite-db.js';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.js';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.js';
import type {ChangeStreamerService} from '../services/change-streamer/change-streamer.js';
import {initializeChangeSource} from '../services/change-streamer/pg/change-source.js';
import {AutoResetSignal} from '../services/change-streamer/schema/tables.js';
import {pgClient} from '../types/pg.js';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {exitAfter, runUntilKilled} from './life-cycle.js';
import {createLogContext} from './logging.js';

export default async function runWorker(parent: Worker): Promise<void> {
  const config = getZeroConfig();
  const port = config.changeStreamerPort ?? config.port + 1;
  const lc = createLogContext(config.log, {worker: 'change-streamer'});

  // Kick off DB connection warmup in the background.
  const changeDB = pgClient(lc, config.change.db, {
    max: config.change.maxConns,
    connection: {['application_name']: 'zero-change-streamer'},
  });
  void Promise.allSettled(
    Array.from({length: config.change.maxConns}, () =>
      changeDB`SELECT 1`.simple().execute(),
    ),
  );

  let {autoReset} = config;
  if (autoReset && config.litestream) {
    lc.warn?.(
      '--auto-reset is incompatible with --litestream. Disabling --auto-reset.',
    );
    autoReset = false;
  }

  let changeStreamer: ChangeStreamerService | undefined;

  for (const first of [true, false]) {
    // Note: This performs initial sync of the replica if necessary.
    const {changeSource, replicationConfig} = await initializeChangeSource(
      lc,
      config.upstream.db,
      config.shard,
      config.replicaFile,
    );

    try {
      changeStreamer = await initializeStreamer(
        lc,
        changeDB,
        changeSource,
        replicationConfig,
        autoReset ?? false,
      );
      break;
    } catch (e) {
      if (first && e instanceof AutoResetSignal) {
        lc.warn?.(`auto-reset: resetting replica ${config.replicaFile}`);
        deleteLiteDB(config.replicaFile);
        continue; // execute again with a fresh initial-sync
      }
      throw e;
    }
  }
  // impossible: upstream must have advanced in order for replication to be stuck.
  assert(changeStreamer, `resetting replica did not advance replicaVersion`);

  const changeStreamerWebServer = new ChangeStreamerHttpServer(
    lc,
    changeStreamer,
    {port},
  );

  parent.send(['ready', {ready: true}]);

  return runUntilKilled(lc, parent, changeStreamer, changeStreamerWebServer);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() => runWorker(must(parentWorker)));
}
