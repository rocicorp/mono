import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {DatabaseInitError} from '../../../zqlite/src/db.ts';
import {getServerContext} from '../config/server-context.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink, publishCriticalEvent} from '../observability/events.ts';
import {upgradeReplica} from '../services/change-source/common/replica-schema.ts';
import {initializeCustomChangeSource} from '../services/change-source/custom/change-source.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source.ts';
import {BackupMonitor} from '../services/change-streamer/backup-monitor.ts';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.ts';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.ts';
import type {ChangeStreamerService} from '../services/change-streamer/change-streamer.ts';
import {ReplicaMonitor} from '../services/change-streamer/replica-monitor.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {AutoResetSignal} from '../services/change-streamer/schema/tables.ts';
import {PurgeLocker} from '../services/change-streamer/storer.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  BackupNotFoundException,
  restoreReplica,
} from '../services/litestream/commands.ts';
import {
  replicationStatusError,
  ReplicationStatusPublisher,
} from '../services/replicator/replication-status.ts';
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
  ...argv: string[]
): Promise<void> {
  const workerStartTime = Date.now();
  const config = getNormalizedZeroConfig({env, argv});
  const {
    taskID,
    changeStreamer: {
      port,
      address,
      protocol,
      startupDelayMs,
      backPressureLimitHeapProportion,
      flowControlConsensusPaddingSeconds,
    },
    upstream,
    change,
    replica,
    initialSync,
    litestream,
  } = config;

  startOtelAuto(
    createLogContext(config, 'change-streamer', 0, false),
    'change-streamer',
    0,
  );
  const lc = createLogContext(config, 'change-streamer');
  initEventSink(lc, config);

  // Kick off DB connection warmup in the background.
  const changeDB = pgClient(
    lc,
    change.db,
    'change-streamer',
    {
      max: change.maxConns,
    },
    {sendStringAsJson: true},
  );
  void warmupConnections(lc, changeDB, 'change').catch(() => {});

  const {autoReset, replicationLag} = config;
  const shard = getShardConfig(config);

  // Ensure the change DB schema is initialized/up-to-date, then acquire
  // a lock to prevent change-lock purges. This ensures that (this)
  // change-streamer will be able to resume from the backup.
  await initChangeStreamerSchema(lc, changeDB, shard);
  let purgeLock = await new PurgeLocker(lc, shard, changeDB).acquire();

  // Restore from litestream if the change-log has entries.
  if (purgeLock) {
    try {
      await restoreReplica(lc, config, purgeLock);
    } catch (e) {
      // If the restore failed, e.g. due to a corrupt or missing backup, the
      // replication-manager recovers by re-syncing.
      const log = e instanceof BackupNotFoundException ? 'warn' : 'error';
      lc[log]?.(
        `error restoring backup. resyncing the replica: ${String(e)}`,
        e,
      );

      // The purgeLock must be released if the backup could not be restored,
      // or it will otherwise prevent the change-db update after the resync
      // completes.
      await purgeLock.release();
      purgeLock = null;
    }
  }

  let changeStreamer: ChangeStreamerService | undefined;

  const context = getServerContext(config);

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
              {
                ...initialSync,
                replicationSlotFailover: upstream.pgReplicationSlotFailover,
              },
              context,
              replicationLag.reportIntervalMs,
            )
          : await initializeCustomChangeSource(
              lc,
              upstream.db,
              shard,
              replica.file,
              context,
            );

      const replicationStatusPublisher =
        ReplicationStatusPublisher.forReplicaFile(replica.file);

      changeStreamer = await initializeStreamer(
        lc,
        shard,
        taskID,
        address,
        protocol,
        changeDB,
        changeSource,
        replicationStatusPublisher,
        subscriptionState,
        purgeLock,
        autoReset ?? false,
        {
          backPressureLimitHeapProportion,
          flowControlConsensusPaddingSeconds,
          statementTimeoutMs: change.statementTimeoutMs,
        },
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

  // Perform any upgrades to the replica in case it was restored from an
  // earlier version. Note that this upgrade is done by the replicator worker
  // as well (in both the replication-manager and the view-syncer), but the
  // change-streamer independently reads the replica, and it is fine run the
  // upgrade logic redundantly since it is idempotent.
  await upgradeReplica(lc, 'change-streamer-init', replica.file);

  const {backupURL, port: metricsPort} = litestream;
  const monitor = backupURL
    ? new BackupMonitor(
        lc,
        replica.file,
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
        Date.now() - workerStartTime,
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
