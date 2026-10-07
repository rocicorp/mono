import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {DatabaseInitError} from '../../../zqlite/src/db.ts';
import {getServerContext} from '../config/server-context.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {registerSQLiteCorruptionDiagnosticTarget} from '../db/sqlite-corruption.ts';
import {warmupConnections} from '../db/warmup.ts';
import {initEventSink, publishCriticalEvent} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {InitCleanup} from '../services/change-source/common/init-cleanup.ts';
import type {PgChangeLogPurgeLocker} from '../services/change-source/common/replica-restore.ts';
import {initializeCustomChangeSource} from '../services/change-source/custom/change-source.ts';
import {initializePostgresChangeSource} from '../services/change-source/pg/change-source-init.ts';
import {createBackupCleanupMonitor} from '../services/change-streamer/backup-cleanup-monitor-factory.ts';
import {ChangeStreamerHttpServer} from '../services/change-streamer/change-streamer-http.ts';
import {initializeStreamer} from '../services/change-streamer/change-streamer-service.ts';
import type {ChangeStreamerService} from '../services/change-streamer/change-streamer.ts';
import {initChangeStreamerSchema} from '../services/change-streamer/schema/init.ts';
import {AutoResetSignal} from '../services/change-streamer/schema/tables.ts';
import {
  PurgeLocker,
  type PurgeLock,
} from '../services/change-streamer/storer.ts';
import {
  exitAfter,
  ProcessManager,
  runUntilKilled,
} from '../services/life-cycle.ts';
import {deleteLitestreamMetaDir} from '../services/litestream/commands.ts';
import type {WalKeeper} from '../services/litestream/wal-keeper.ts';
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
  broadcastWorker,
  childWorker,
  parentWorker,
  singleProcessMode,
  type ProfileResponseMessage,
  type Worker,
} from '../types/processes.ts';
import {installProfileHandler} from '../types/profiler.ts';
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
  installProfileHandler(parent, 'change-streamer');
  const config = getNormalizedZeroConfig({env, argv});
  const {
    taskID,
    changeStreamer: {
      port,
      address,
      protocol,
      startupDelayMs,
      backPressureLimitHeapProportion,
      flowControlConsensusTimeoutProportion,
      flowControlSlowSubscriberGracePeriodSeconds,
      pgChangeLogEnabled,
      sqliteChangeLogMode,
      sqliteChangeLogReadPercent,
      sqliteChangeLogColdReadPercent,
      sqliteChangeLogComparePercent,
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
    sqliteCorruptionChecks,
  } = config;

  startOtelAuto(
    createLogContext(config, 'change-streamer', 0, false),
    'change-streamer',
    0,
  );
  lc = createLogContext(config, 'change-streamer');
  registerSQLiteCorruptionDiagnosticTarget(
    {
      debugName: 'change-streamer replica',
      dbPath: replica.file,
    },
    sqliteCorruptionChecks,
  );
  initEventSink(lc, config);

  // Startup-time client used for for change-streamer initialization
  // and handoff / takeover. Steady-state clients are managed by the
  // change-streamer (Storer) implementation.
  const changeDB = await connectPgClient(
    lc,
    change.db,
    'change-streamer-init',
    {max: 5},
    {sendStringAsJson: true},
  );
  void warmupConnections(lc, changeDB, 'change').catch(() => {});

  const shard = getShardConfig(config);

  // Ensure the change DB schema is initialized/up-to-date.
  await initChangeStreamerSchema(lc, changeDB, shard);

  let purgeLock = null as PurgeLock | null;
  let changeStreamer: ChangeStreamerService | undefined;
  let backupURL: string | undefined;

  const context = getServerContext(config);
  const sqliteChangeLogEnabled = sqliteChangeLogMode !== 'off';
  // The modes are cumulative, so `serve` implies `compare`.
  const sqliteChangeLogComparing =
    sqliteChangeLogMode === 'compare' || sqliteChangeLogMode === 'serve';
  if (sqliteChangeLogEnabled) {
    const changeLogFile = changeLogFileName(replica.file);
    registerSQLiteCorruptionDiagnosticTarget(
      {
        debugName: 'change-streamer change-log',
        dbPath: changeLogFile,
      },
      sqliteCorruptionChecks,
    );
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
  let newBackupLineage = false;
  let walKeeper: WalKeeper | undefined;
  for (const first of [true, false]) {
    // Resources acquired by an initialization attempt (e.g. a claimed
    // replication slot, the purge lock) to be released if the attempt fails.
    const cleanup = new InitCleanup(lc);
    // When restoring from litestream, a lock is acquired to prevent change-log
    // purges. This ensures that (this) change-streamer will be able to resume
    // from the backup. The lock is acquired by the change source initialization
    // just before restoring (i.e. after a replication slot is created, if
    // applicable), and released once this change-streamer takes over the
    // change-log.
    const acquirePurgeLock: PgChangeLogPurgeLocker | undefined =
      pgChangeLogEnabled && litestream.backupURL && litestream.executable
        ? async (slotWatermark?: string) => {
            const lock = await new PurgeLocker(lc, shard, changeDB).acquire(
              slotWatermark,
            );
            purgeLock = lock === 'behind-slot' ? null : lock;
            if (purgeLock) {
              const acquired = purgeLock;
              cleanup.onFailure('purge lock', () => {
                purgeLock = null;
                return acquired.release();
              });
            }
            return lock;
          }
        : undefined;

    const restoreOptions = {litestream, acquirePurgeLock, cleanup};
    try {
      // Note: This performs initial sync of the replica if necessary.
      const {
        pgReplicationEpoch: epoch,
        pgReplicationSlotPerReplica: slotPerReplica,
        pgResumeOrphanedSlotGracePeriodMs: inactiveReplicaGracePeriodMs,
      } = upstream;
      const {
        changeSource,
        subscriptionState,
        destinationBackupURL,
        replicaID,
        waitForBackupBeforeServing,
        newBackupLineage: initNewBackupLineage,
        walKeeper: initWalKeeper,
        pgChangeLogBehindSlot,
      } = upstream.type === 'pg'
        ? await initializePostgresChangeSource(
            lc,
            upstream.db,
            shard,
            replica.file,
            {
              ...initialSync,
              replicationSlotFailover: upstream.pgReplicationSlotFailover,
              installPartialIndexTriggers: upstream.pgPartialIndexTriggers,
            },
            context,
            replicationLag.reportIntervalMs,
            restoreOptions,
            {
              epoch,
              slotPerReplica,
              inactiveReplicaGracePeriodMs,
              backupV5: litestream.backupUsingV5,
            },
            upstream.pgStreamInboundTimeoutMs,
          )
        : await initializeCustomChangeSource(
            lc,
            upstream.db,
            shard,
            replica.file,
            context,
            restoreOptions,
          );
      walKeeper = initWalKeeper;

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
          ? {
              backupURL: destinationBackupURL,
              litestreamVersion: litestream.backupUsingV5 ? 'v5' : 'legacy',
              replicaFile: replica.file,
            }
          : null,
        purgeLock,
        autoReset ?? false,
        {
          pgChangeLogEnabled,
          pgChangeLogBehindSlot: pgChangeLogBehindSlot ?? false,
          backPressureLimitHeapProportion,
          flowControlConsensusTimeoutProportion,
          flowControlSlowSubscriberGracePeriodMs:
            flowControlSlowSubscriberGracePeriodSeconds > 0
              ? flowControlSlowSubscriberGracePeriodSeconds * 1000
              : undefined,
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
          // Compare mode runs both advisory checks. Postgres remains authoritative.
          sqliteChangeLogCompare:
            pgChangeLogEnabled && sqliteChangeLogComparing
              ? {
                  replicaFile: replica.file,
                  comparePercent: sqliteChangeLogComparePercent,
                  retentionMs: sqliteChangeLogRetentionMs,
                  readBatchRows: sqliteChangeLogReadBatchRows,
                }
              : undefined,
          // Slice 11 lands dark by default: serve mode constructs the stable
          // router, while readPercent=0 keeps every catchup on PG and emits
          // eligibility metrics before any canary traffic is enabled.
          sqliteChangeLogServe:
            sqliteChangeLogMode === 'serve'
              ? {
                  readPercent: sqliteChangeLogReadPercent,
                  coldReadPercent: sqliteChangeLogColdReadPercent,
                  retentionMs: sqliteChangeLogRetentionMs,
                }
              : undefined,
        },
        setTimeout,
      );
      backupURL = destinationBackupURL;
      waitForFirstBackupBeforeServing = waitForBackupBeforeServing;
      newBackupLineage = initNewBackupLineage;
      break;
    } catch (e) {
      // Release the resources acquired by the failed attempt, e.g.:
      // * The claimed replication slot, which would otherwise remain active
      //   indefinitely, preventing a retry from resuming (or forking) it.
      // * The purge lock. This is safe because the purge lock exists to
      //   preserve change-log entries so the new change-streamer can resume
      //   from the backup replica's watermark. An AutoResetSignal means we
      //   can't resume from the backup replica (e.g. its replication slot is
      //   gone), so the change-log entries the lock was protecting are no
      //   longer needed. The retry performs a fresh initial sync with a new
      //   replication slot, independent of the old change-log. Releasing is
      //   also necessary to avoid a self-deadlock when CHANGE_DB ==
      //   UPSTREAM_DB: CREATE_REPLICATION_SLOT waits for all older
      //   transactions to finish, including this lock's open transaction.
      // * The WAL keeper of a restore that prepared a backup lineage to
      //   continue, which would otherwise hold the (deleted) replica open.
      await cleanup.release();
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
  const profileSubWorkers: Worker[] = [];
  const {promise: replicatorReady, resolve} = resolver();
  if (!backupURL) {
    resolve(); // No backup replicator to wait for, so resolve immediately.
  } else {
    lc.info?.('setting up backup to', backupURL);
    config.litestream.backupURLOverride = backupURL;
    if (newBackupLineage) {
      deleteLitestreamMetaDir(replica.file);
    }
    // Start a backup replicator, which starts up the corresponding
    // litestream backup process.
    const backupReplicator = processes
      .addWorker(
        childWorker(
          REPLICATOR_URL,
          {...env, ZERO_LITESTREAM_BACKUP_URL_OVERRIDE: backupURL},
          'backup' satisfies ReplicaFileMode,
        ),
        'supporting',
        'backup-replicator',
      )
      .onceMessageType('ready', () => resolve());
    profileSubWorkers.push(backupReplicator);
    // Relay profileResponse messages from backup-replicator up to parent
    backupReplicator.onMessageType<ProfileResponseMessage>(
      'profileResponse',
      res => parent.send(['profileResponse', res]),
    );
  }

  const backupMonitor = createBackupCleanupMonitor({
    lc,
    config,
    replicaFile: replica.file,
    changeStreamer,
  });

  let backupReady = promiseVoid;
  if (waitForFirstBackupBeforeServing) {
    const start = performance.now();
    lc.info?.(`awaiting initial backup ...`);

    backupReady = backupMonitor.firstBackupReceived().then(() => {
      const elapsed = performance.now() - start;
      lc.info?.(`initial backup confirmed after ${elapsed.toFixed(2)}ms`);
    });
  }
  // By the initial backup, litestream holds the restored WAL itself.
  void backupReady.then(
    () => walKeeper?.release('initial backup confirmed'),
    () => walKeeper?.release('initial backup failed'),
  );

  // In RMv2, readiness additionally waits for the backup-replicator to catch
  // up to the replication stream. This is not done in RMv1 (i.e. when the
  // PG change-log is enabled), in which the change-streamer itself is only
  // started after the readinessGate (plus a startup delay), and thus the
  // backup-replicator cannot catch up before then.
  const readinessGate = pgChangeLogEnabled
    ? backupReady
    : Promise.all([backupReady, replicatorReady]);

  // Create the broadcast facade once: each broadcastWorker() adds permanent
  // 'message' forwarders to every sub worker, so creating one per /profz
  // request would leak a forwarder per request.
  const profileWorker =
    profileSubWorkers.length > 0
      ? broadcastWorker(profileSubWorkers)
      : undefined;
  const getProfileWorker = profileWorker
    ? () => Promise.resolve(profileWorker)
    : undefined;

  const changeStreamerWebServer = new ChangeStreamerHttpServer(
    lc,
    {
      port,
      keepaliveTimeoutMs,
      // The startup delay is only relevant when taking over the PG change-log
      // (RMv1 and RMv1.5), and is disabled for RMv2.
      startupDelayMs: pgChangeLogEnabled ? startupDelayMs : 0,
      readinessGate,
      config,
      getProfileWorker,
    },
    parent,
    changeStreamer,
  );

  void readinessGate.then(() => parent.send(['ready', {ready: true}]));

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
    walKeeper?.release('shutting down');
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
