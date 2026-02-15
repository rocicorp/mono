import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {DatabaseInitError} from '../../../zqlite/src/db.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink, publishCriticalEvent} from '../observability/events.ts';
import {initializeCustomChangeSource} from '../services/change-source/custom/change-source.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source.ts';
import {BackupMonitor} from '../services/change-streamer/backup-monitor.ts';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.ts';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.ts';
import type {ChangeStreamerService} from '../services/change-streamer/change-streamer.ts';
import {ReplicaMonitor} from '../services/change-streamer/replica-monitor.ts';
import {AutoResetSignal} from '../services/change-streamer/schema/tables.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {replicationStatusError} from '../services/replicator/replication-status.ts';
import {pgClient} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `parent startMs not specified`);
  const parentStartMs = parseInt(args[0]);

  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});
  const {
    taskID,
    changeStreamer: {
      port,
      address,
      protocol,
      startupDelayMs,
      backPressureLimitHeapProportion,
      queueSizeBackPressureThreshold,
    },
    upstream,
    change,
    replica,
    initialSync,
    litestream,
  } = config;

  startOtelAuto(createLogContext(config, {worker: 'change-streamer'}, false));
  const lc = createLogContext(config, {worker: 'change-streamer'}, true);
  initEventSink(lc, config);

  // Kick off DB connection warmup in the background.
  const changeDB = pgClient(
    lc,
    change.db,
    {
      max: change.maxConns,
      connection: {['application_name']: 'zero-change-streamer'},
    },
    {sendStringAsJson: true},
  );
  void warmupConnections(lc, changeDB, 'change');

  const {autoReset} = config;
  const shard = getShardConfig(config);

  let changeStreamer: ChangeStreamerService | undefined;

  for (const first of [true, false]) {
    try {
      // Note: This performs initial sync of the replica if necessary.
      const {changeSource, subscriptionState} =
        upstream.type === 'pg'
          ? await initializePostgresChangeSource(
              lc,
              upstream.db,
              shard,
              replica.file,
              initialSync,
            )
          : await initializeCustomChangeSource(
              lc,
              upstream.db,
              shard,
              replica.file,
            );

      changeStreamer = await initializeStreamer(
        lc,
        shard,
        taskID,
        address,
        protocol,
        changeDB,
        changeSource,
        subscriptionState,
        autoReset ?? false,
        backPressureLimitHeapProportion,
        queueSizeBackPressureThreshold,
        setTimeout,
      );
      break;
    } catch (e) {
      if (first && e instanceof AutoResetSignal) {
        lc.warn?.(`resetting replica ${replica.file}`, e);
        // TODO: Make deleteLiteDB work with litestream. It will probably have to be
        //       a semantic wipe instead of a file delete.
        deleteLiteDB(replica.file);
        continue; // execute again with a fresh initial-sync
      }
      await publishCriticalEvent(
        lc,
        replicationStatusError(lc, 'Initializing', e),
      );
      if (e instanceof DatabaseInitError) {
        throw new Error(
          `Cannot open ZERO_REPLICA_FILE at "${replica.file}". Please check that the path is valid.`,
          {cause: e},
        );
      }
      throw e;
    }
  }
  // impossible: upstream must have advanced in order for replication to be stuck.
  assert(changeStreamer, `resetting replica did not advance replicaVersion`);

  const {backupURL, port: metricsPort} = litestream;
  const monitor = backupURL
    ? new BackupMonitor(
        lc,
        backupURL,
        `http://localhost:${metricsPort}/metrics`,
        changeStreamer,
        // The time between when the zero-cache was started to when the
        // change-streamer is ready to start serves as the initial delay for
        // watermark cleanup (as it either includes a similar replica
        // restoration/preparation step, or an initial-sync, which
        // generally takes longer).
        //
        // Consider: Also account for permanent volumes?
        Date.now() - parentStartMs,
      )
    : new ReplicaMonitor(lc, replica.file, changeStreamer);

  const changeStreamerWebServer = new ChangeStreamerHttpServer(
    lc,
    config,
    {port, startupDelayMs},
    parent,
    changeStreamer,
    monitor instanceof BackupMonitor ? monitor : null,
  );

  parent.send(['ready', {ready: true}]);

  // Note: The changeStreamer itself is not started here; it is started by the
  //       changeStreamerWebServer.
  return runUntilKilled(lc, parent, changeStreamerWebServer, monitor);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
