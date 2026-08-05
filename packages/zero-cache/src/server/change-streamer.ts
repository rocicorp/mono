import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {DatabaseInitError} from '../../../zqlite/src/db.ts';
import {getServerContext} from '../config/server-context.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink, publishCriticalEvent} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {initializeCustomChangeSource} from '../services/change-source/custom/change-source.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source.ts';
import {createBackupCleanupMonitor} from '../services/change-streamer/backup-cleanup-monitor-factory.ts';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.ts';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.ts';
import type {ChangeStreamerService} from '../services/change-streamer/change-streamer.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {AutoResetSignal} from '../services/change-streamer/schema/tables.ts';
import {PurgeLocker} from '../services/change-streamer/storer.ts';
import {
  exitAfter,
  ProcessManager,
  runUntilKilled,
} from '../services/life-cycle.ts';
import {startReplicaBackupProcess} from '../services/litestream/commands.ts';
import {
  changeLogFileName,
  deleteChangeLogDB,
} from '../services/replicator/change-log-db.ts';
import {
  replicationStatusError,
  ReplicationStatusPublisher,
} from '../services/replicator/replication-status.ts';
import {sqliteFileBytes} from '../services/replicator/sqlite-change-log-observability.ts';
import {connectPgClient} from '../types/pg.ts';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import type {ReplicaFileMode} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';
import {REPLICATOR_URL} from './worker-urls.ts';

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
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
      flowControlEventDrivenRelease,
      sqliteChangeLogMode,
      sqliteChangeLogRetentionMs,
      sqliteChangeLogReadBatchRows,
      sqliteChangeLogPurgeBatchRows,
      sqliteChangeLogBarrierTimeoutMs,
    },
    autoReset,
    replicationLag,
    litestream,
    upstream,
    change,
    replica,
    initialSync,
    keepaliveTimeoutMs,
  } = config;

  startOtelAuto(
    createLogContext(config, 'change-streamer', 0, false),
    'change-streamer',
    0,
  );
  lc = createLogContext(config, 'change-streamer');
  registerSQLiteCorruptionDiagnosticTarget({
    debugName: 'change-streamer replica',
    dbPath: replica.file,
  });
  initEventSink(lc, config);

  // Kick off DB connection warmup in the background.
  const changeDB = await connectPgClient(
    lc,
    change.db,
    'change-streamer',
    {max: change.maxConns},
    {sendStringAsJson: true},
  );
  void warmupConnections(lc, changeDB, 'change').catch(() => {});

  const shard = getShardConfig(config);

  // Ensure the change DB schema is initialized/up-to-date.
  await initChangeStreamerSchema(lc, changeDB, shard);

  // When restoring from litestream, acquire a lock to prevent change-log
  // purges. This ensures that (this) change-streamer will be able to resume
  // from the backup.
  let purgeLock =
    litestream.backupURL && litestream.executable
      ? await new PurgeLocker(lc, shard, changeDB).acquire()
      : null;
  const restoreOptions = {litestream, constraints: purgeLock ?? undefined};

  let changeStreamer: ChangeStreamerService | undefined;
  let backupURL: string | undefined;

  const context = getServerContext(config);
  const sqliteChangeLogEnabled = sqliteChangeLogMode !== 'off';
  if (sqliteChangeLogEnabled) {
    const changeLogFile = changeLogFileName(replica.file);
    registerSQLiteCorruptionDiagnosticTarget({
      debugName: 'change-streamer change-log',
      dbPath: changeLogFile,
    });
    // The log's bytes are accounted for in the process that writes them. This
    // is local disk only — the log is excluded from the litestream backup — and
    // it is a whole-database total, because a table that left the replica left
    // through its wal: the replica's footprint is what shrank and this is where
    // it went.
    getOrCreateGauge('replica', 'sqlite_change_log.file_bytes', {
      description:
        `The SQLite change log's total on-disk footprint: its main db file ` +
        `plus its wal sidecars.`,
      unit: 'By',
    }).addCallback(async o =>
      o.observe(await sqliteFileBytes(lc, changeLogFile)),
    );
  }

  let waitForFirstBackupBeforeServing = false;
  for (const first of [true, false]) {
    try {
      // Note: This performs initial sync of the replica if necessary.
      const {
        changeSource,
        subscriptionState,
        destinationBackupURL,
        replicaID,
        waitForBackupBeforeServing,
      } =
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
              restoreOptions,
              purgeLock,
            )
          : await initializeCustomChangeSource(
              lc,
              upstream.db,
              shard,
              replica.file,
              context,
              restoreOptions,
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
        destinationBackupURL
          ? {backupURL: destinationBackupURL, litestreamVersion: 'legacy'}
          : null,
        purgeLock,
        autoReset ?? false,
        {
          backPressureLimitHeapProportion,
          flowControlConsensusPaddingSeconds,
          flowControlEventDrivenRelease,
          statementTimeoutMs: change.statementTimeoutMs,
          changeLogBatchSize: change.logBatchSize,
          sqliteCatchup: {
            changeLogFile: changeLogFileName(replica.file),
            readBatchRows: sqliteChangeLogReadBatchRows,
            barrierTimeoutMs: sqliteChangeLogBarrierTimeoutMs,
          },
          // The presence of these options is the writer's gate. This process
          // performs the restore and the initial sync, so the replica's
          // identity is in hand at the moment the log is opened.
          sqliteChangeLogWriter: sqliteChangeLogEnabled
            ? {
                replicaFile: replica.file,
                identity: {
                  // RMv2 supplies the epoch; until then the generation and the
                  // replica ID are the whole of the identity.
                  epoch: null,
                  generation: subscriptionState.replicaVersion,
                  replicaID,
                },
              }
            : undefined,
          // The purge scheduler shares the writer's gate -- mode, not the
          // read path -- because `write` mode, with no read selector at all,
          // is the configuration it ships in.
          sqliteChangeLogPurge: sqliteChangeLogEnabled
            ? {
                retentionMs: sqliteChangeLogRetentionMs,
                batchRows: sqliteChangeLogPurgeBatchRows,
              }
            : undefined,
        },
        setTimeout,
      );
      backupURL = destinationBackupURL;
      waitForFirstBackupBeforeServing = waitForBackupBeforeServing;
      break;
    } catch (e) {
      if (first && e instanceof AutoResetSignal) {
        lc.warn?.(`resetting replica ${replica.file}`, e);
        // TODO: Make deleteLiteDB work with litestream. It will probably have to be
        //       a semantic wipe instead of a file delete.
        deleteLiteDB(replica.file);
        // The change log carries the identity of the replica it was written
        // beside, and the retry performs a fresh initial sync with a new
        // replicaVersion. Reconciliation would catch that on its own (as
        // 'identity-mismatch') but only after the writer opened a file that is
        // known here to be garbage.
        deleteChangeLogDB(replica.file);
        // Release the purge lock before retrying. This is safe because the
        // purge lock exists to preserve change-log entries so the new
        // change-streamer can resume from the backup replica's watermark.
        // An AutoResetSignal means we cant resume from the backup replica
        // (e.g. its replication slot is gone), so the change-log entries the lock
        // was protecting are no longer needed. The retry performs a fresh
        // initial sync with a new replication slot, independent of the old
        // change-log. Releasing is also necessary to avoid a
        // self-deadlock when CHANGE_DB == UPSTREAM_DB:
        // CREATE_REPLICATION_SLOT waits for all older transactions to
        // finish, including this lock's open transaction.
        await purgeLock?.release();
        purgeLock = null;
        continue; // execute again with a fresh initial-sync
      }
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

  const processes = new ProcessManager(lc, parent);
  if (backupURL) {
    lc.info?.('setting up backup to', backupURL);
    litestream.backupURL = backupURL;
    const {promise: backupStarted, resolve} = resolver();

    // Start a backup replicator and corresponding litestream backup process.
    processes
      .addWorker(
        childWorker(REPLICATOR_URL, env, 'backup' satisfies ReplicaFileMode),
        'supporting',
        'backup-replicator',
      )
      // Wait for the replicator's first message (i.e. "ready") before starting
      // litestream backup in order to avoid contending on the sqlite lock
      // when the replicator first prepares the db file.
      .once('message', () => {
        processes.addSubprocess(
          startReplicaBackupProcess(lc, litestream, replica.file),
          'supporting',
          'litestream',
        );
        resolve();
      });
    await backupStarted;
  }

  const backupMonitor = createBackupCleanupMonitor({
    lc,
    processes,
    config,
    replicaFile: replica.file,
    changeStreamer,
    env,
  });

  const changeStreamerWebServer = new ChangeStreamerHttpServer(
    lc,
    {port, keepaliveTimeoutMs, startupDelayMs},
    parent,
    changeStreamer,
  );

  if (waitForFirstBackupBeforeServing) {
    lc.info?.(`awaiting initial backup ...`);
    void backupMonitor
      .firstBackupReceived()
      .then(() => parent.send(['ready', {ready: true}]));
  } else {
    parent.send(['ready', {ready: true}]);
  }

  // Note: The changeStreamer itself is not started here; it is started by the
  //       changeStreamerWebServer after a delay to ensure that routing
  //       routing elements have registered the server before the
  //       change-streamer takes over the replication slot.
  // TODO: Remove this delay and start the changeStreamer normally here once
  //       transitioned to RMv2, since changeStreamer startup will no longer
  //       disrupt an existing task.
  try {
    await runUntilKilled(lc, parent, changeStreamerWebServer, backupMonitor);
  } catch (err) {
    processes.logErrorAndExit(err, 'change-streamer');
  } finally {
    await processes.shutdown();
  }
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () =>
      runWorker(
        must(parentWorker),
        process.env,
        ...process.argv.slice(2),
      ).catch(async e => {
        await publishCriticalEvent(
          lc,
          replicationStatusError(lc, 'Initializing', e),
        );
        throw e;
      }),
  );
}
