import {getDefaultHighWaterMark} from 'node:stream';
import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {publishCriticalEvent} from '../../observability/events.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import {min} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../change-source/change-source.ts';
import {
  type ChangeStreamControl,
  type ChangeStreamData,
  type Rollback,
} from '../change-source/protocol/current/downstream.ts';
import type {LitestreamVersion} from '../litestream/metrics.ts';
import {
  publishReplicationError,
  replicationStatusError,
  type ReplicationStatusPublisher,
} from '../replicator/replication-status.ts';
import type {SubscriptionState} from '../replicator/schema/replication-state.ts';
import {
  DEFAULT_MAX_RETRY_DELAY_MS,
  RunningState,
  UnrecoverableError,
} from '../running-state.ts';
import {
  type ChangeStreamerService,
  type Status,
  type SubscriberContext,
  type WatermarkedChange,
} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import {
  AutoResetSignal,
  ensureReplicationConfig,
  markResetRequired,
} from './schema/tables.ts';
import {SnapshotReservations} from './snapshot-reservations.ts';
import type {SnapshotMessage} from './snapshot.ts';
import {
  SQLiteChangeLogCatchup,
  type SQLiteChangeLogCleanupGuard,
} from './sqlite-change-log-catchup.ts';
import {
  SQLiteChangeLogPurgeScheduler,
  type PurgeContinuation,
  type SQLiteChangeLogPurgeSchedulerOptions,
} from './sqlite-change-log-purge-scheduler.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';
import {
  SQLiteChangeLogWriter,
  type SQLiteChangeLogWriterOptions,
} from './sqlite-change-log-writer.ts';
import {
  Storer,
  type PurgeLock,
  type TuningOptions as StorerOptions,
} from './storer.ts';
import {Subscriber} from './subscriber.ts';
import {UpstreamAcker} from './upstream-acker.ts';

export type BackupConfig = {
  backupURL: string;
  litestreamVersion: LitestreamVersion;
};

export type SQLiteCatchupOptions = {
  /** The change-log database, i.e. `changeLogFileName(replicaFile)`. */
  changeLogFile: string;
  readBatchRows: number;
  barrierTimeoutMs: number;
  /**
   * Backstop for the change-log writer's ACK, which is what normally releases
   * the barrier. Defaults to the catchup coordinator's interval.
   */
  barrierPollIntervalMs?: number | undefined;
  /**
   * Slice 11 supplies the production canary selector.
   *
   * It does not need to exclude backup subscribers or the change-log writer:
   * eligibility checks reject both before invoking this selector. Serving a
   * writer from SQLite would make it wait on its own ACK.
   */
  shouldUse?: ((ctx: SubscriberContext) => boolean) | undefined;
  /**
   * Overrides the purge scheduler's guard, for tests. When absent, the
   * service supplies the scheduler's real writer-serialized guard, or the
   * catchup's no-op default when purging is not configured.
   */
  cleanupGuard?: SQLiteChangeLogCleanupGuard | undefined;
  /**
   * How long the change log may be unavailable before declining to serve from
   * it is reported as a warning rather than a debug line. Defaults to
   * {@link DEFAULT_CHANGE_LOG_UNAVAILABLE_WARN_THRESHOLD_MS}.
   */
  notReadyWarnThresholdMs?: number | undefined;
};

export type TuningOptions = StorerOptions & {
  flowControlConsensusPaddingSeconds: number;
  flowControlEventDrivenRelease?: boolean | undefined;
  sqliteCatchup?: SQLiteCatchupOptions | undefined;
  /**
   * Supplied when `sqliteChangeLogMode != off`, i.e. this is the gate on the
   * change-log writer. Absent, nothing writes the log.
   */
  sqliteChangeLogWriter?:
    | Omit<SQLiteChangeLogWriterOptions, 'onCommit' | 'onDisabled'>
    | undefined;
  /**
   * Also supplied when `sqliteChangeLogMode != off`, and deliberately not
   * gated on the read path: in `write` mode no reader ever opens, and this is
   * the configuration the purge scheduler actually ships in. The scheduler
   * runs on the writer's own connection, so with the writer absent (mode
   * `off`) it is never constructed, and a leftover file on disk is left to
   * the replicator's cleanup.
   */
  sqliteChangeLogPurge?: SQLiteChangeLogPurgeSchedulerOptions | undefined;
};

/**
 * Performs initialization and schema migrations to initialize a ChangeStreamerImpl.
 */
export async function initializeStreamer(
  lc: LogContext,
  shard: ShardID,
  taskID: string,
  discoveryAddress: string,
  discoveryProtocol: string,
  changeDB: PostgresDB,
  changeSource: ChangeSource,
  replicationStatusPublisher: ReplicationStatusPublisher,
  subscriptionState: SubscriptionState,
  backupConfig: BackupConfig | null,
  purgeLock: PurgeLock | null,
  autoReset: boolean,
  opts: TuningOptions,
  setTimeoutFn = setTimeout,
): Promise<ChangeStreamerService> {
  await ensureReplicationConfig(
    lc,
    changeDB,
    subscriptionState,
    shard,
    autoReset,
    purgeLock ?? undefined,
    setTimeoutFn,
  );

  const {replicaVersion} = subscriptionState;
  return new ChangeStreamerImpl(
    lc,
    shard,
    taskID,
    discoveryAddress,
    discoveryProtocol,
    changeDB,
    replicaVersion,
    changeSource,
    replicationStatusPublisher,
    backupConfig,
    purgeLock,
    autoReset,
    opts,
    setTimeoutFn,
  );
}

const REPLICATION_STATUS_ERROR_DELAY_THRESHOLD_MS = 5000;

// How long the change log may be unavailable before declining to serve from it
// stops looking like normal startup ordering. Subscriptions can arrive before
// the stream loop's first reconcile has created and seeded the log, so early
// declines are expected. Still declining a minute later means the log is not
// being written at all -- `sqliteChangeLogMode=off`, a writer that has failed
// soft, or a path mismatch -- which is worth surfacing.
const DEFAULT_CHANGE_LOG_UNAVAILABLE_WARN_THRESHOLD_MS = 60_000;

/**
 * Upstream-agnostic dispatch of messages in a {@link ChangeStreamMessage} to a
 * {@link Forwarder} and {@link Storer} to execute the forward-store-ack
 * procedure described in {@link ChangeStreamer}.
 *
 * ### Subscriber Catchup
 *
 * Connecting clients first need to be "caught up" to the current watermark
 * (from stored change log entries) before new entries are forwarded to
 * them. This is non-trivial because the replication stream may be in the
 * middle of a pending streamed Transaction for which some entries have
 * already been forwarded but are not yet committed to the store.
 *
 *
 * ```
 * ------------------------------- - - - - - - - - - - - - - - - - - - -
 * | Historic changes in storage |  Pending (streamed) tx  |   Next tx
 * ------------------------------- - - - - - - - - - - - - - - - - - - -
 *                                           Replication stream
 *                                           >  >  >  >  >  >  >  >  >
 *           ^  ---> required catchup --->   ^
 * Subscriber watermark               Subscription begins
 * ```
 *
 * Preemptively buffering the changes of every pending transaction
 * would be wasteful and consume too much memory for large transactions.
 *
 * Instead, the streamer synchronously dispatches changes and subscriptions
 * to the {@link Forwarder} and the {@link Storer} such that the two
 * components are aligned as to where in the stream the subscription started.
 * The two components then coordinate catchup and handoff via the
 * {@link Subscriber} object with the following algorithm:
 *
 * * If the streamer is in the middle of a pending Transaction, the
 *   Subscriber is "queued" on both the Forwarder and the Storer. In this
 *   state, new changes are *not* forwarded to the Subscriber, and catchup
 *   is not yet executed.
 * * Once the commit message for the pending Transaction is processed
 *   by the Storer, it begins catchup on the Subscriber (with a READONLY
 *   snapshot so that it does not block subsequent storage operations).
 *   This catchup is thus guaranteed to load the change log entries of
 *   that last Transaction.
 * * When the Forwarder processes that same commit message, it moves the
 *   Subscriber from the "queued" to the "active" set of clients such that
 *   the Subscriber begins receiving new changes, starting from the next
 *   Transaction.
 * * The Subscriber does not forward those changes, however, if its catchup
 *   is not complete. Until then, it buffers the changes in memory.
 * * Once catchup is complete, the buffered changes are immediately sent
 *   and the Subscriber henceforth forwards changes as they are received.
 *
 * In the (common) case where the streamer is not in the middle of a pending
 * transaction when a subscription begins, the Storer begins catchup
 * immediately and the Forwarder directly adds the Subscriber to its active
 * set. However, the Subscriber still buffers any forwarded messages until
 * its catchup is complete.
 *
 * ### Watermarks and ordering
 *
 * The ChangeStreamerService depends on its {@link ChangeSource} to send
 * changes in contiguous [`begin`, `data` ..., `data`, `commit`] sequences
 * in commit order. This follows Postgres's Logical Replication Protocol
 * Message Flow:
 *
 * https://www.postgresql.org/docs/16/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW
 *
 * > The logical replication protocol sends individual transactions one by one.
 * > This means that all messages between a pair of Begin and Commit messages belong to the same transaction.
 *
 * In order to correctly replay (new) and filter (old) messages to subscribers
 * at different points in the replication stream, these changes must be assigned
 * watermarks such that they preserve the order in which they were received
 * from the ChangeSource.
 *
 * A previous implementation incorrectly derived these watermarks from the Postgres
 * Log Sequence Numbers (LSN) of each message. However, LSNs from concurrent,
 * non-conflicting transactions can overlap, which can result in a `begin` message
 * with an earlier LSN arriving after a `commit` message. For example, the
 * changes for these transactions:
 *
 * ```
 * LSN:   1     2     3  4    5   6   7     8   9      10
 * tx1: begin  data     data     data     commit
 * tx2:             begin    data    data      data  commit
 * ```
 *
 * will arrive as:
 *
 * ```
 * begin1, data2, data4, data6, commit8, begin3, data5, data7, data9, commit10
 * ```
 *
 * Thus, LSN of non-commit messages are not suitable for tracking the sorting
 * order of the replication stream.
 *
 * Instead, the ChangeStreamer uses the following algorithm for deterministic
 * catchup and filtering of changes:
 *
 * * A `commit` message is assigned to a watermark corresponding to its LSN.
 *   These are guaranteed to be in commit order by definition.
 *
 * * `begin` and `data` messages are assigned to the watermark of the
 *   preceding `commit` (the previous transaction, or the replication
 *   slot's starting LSN) plus 1. This guarantees that they will be sorted
 *   after the previously commit transaction even if their LSNs came before it.
 *   This is referred to as the `preCommitWatermark`.
 *
 * * In the ChangeLog DB, messages have a secondary sort column `pos`, which is
 *   the position of the message within its transaction, with the `begin` message
 *   starting at `0`. This guarantees that `begin` and `data` messages will be
 *   fetched in the original ChangeSource order during catchup.
 *
 * `begin` and `data` messages share the same watermark, but this is sufficient for
 * Subscriber filtering because subscribers only know about the `commit` watermarks
 * exposed in the `Downstream` `Commit` message. The Subscriber object thus compares
 * the internal watermarks of the incoming messages against the commit watermark of
 * the caller, updating the watermark at every `Commit` message that is forwarded.
 *
 * ### Cleanup
 *
 * As mentioned in the {@link ChangeStreamer} documentation: "the ChangeStreamer
 * uses a combination of [the "initial", i.e. backup-derived watermark and] ACK
 * responses from connected subscribers to determine the watermark up
 * to which it is safe to purge old change log entries."
 *
 * More concretely:
 *
 * * The `initial`, backup-derived watermark is the earliest to which cleanup
 *   should ever happen.
 *
 * * However, it is possible for the replica backup to be *ahead* of a connected
 *   subscriber; and if a network error causes that subscriber to retry from its
 *   last watermark, the change streamer must support it.
 *
 * Thus, before cleaning up to an `initial` backup-derived watermark, the change
 * streamer first confirms that all connected subscribers have also passed
 * that watermark.
 */
class ChangeStreamerImpl implements ChangeStreamerService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #changeDB: PostgresDB;
  readonly #replicaVersion: string;
  readonly #source: ChangeSource;
  readonly #storer: Storer;
  readonly #forwarder: Forwarder;
  readonly #reservations: SnapshotReservations | undefined;
  readonly #replicationStatusPublisher: ReplicationStatusPublisher;
  readonly #sqliteCatchupOptions: SQLiteCatchupOptions | undefined;
  readonly #changeLogWriter: SQLiteChangeLogWriter | undefined;
  readonly #purgeScheduler: SQLiteChangeLogPurgeScheduler | undefined;
  readonly #acker: UpstreamAcker;

  readonly #autoReset: boolean;
  readonly #state: RunningState;

  // Starting the (Postgres) ChangeStream results in killing the previous
  // Postgres subscriber, potentially creating a gap in which the old
  // change-streamer has shut down and the new change-streamer has not yet
  // been recognized as "healthy" (and thus does not get any requests).
  //
  // To minimize this gap, delay starting the ChangeStream until the first
  // request from a `serving` replicator, indicating that higher level
  // load-balancing / routing logic has begun routing requests to this task.
  readonly #serving = resolver();

  readonly #txCounter = getOrCreateCounter(
    'replication',
    'transactions',
    'Count of replicated transactions',
  );
  readonly #changeCounter = getOrCreateCounter(
    'replication',
    'changes',
    'Count of replicated changes (DML or DDL statements)',
  );
  // The number the SQLite change-log commit lands in. It is labeled by whether
  // the log is being written so that the cost of putting a commit on the
  // forward path is attributable rather than inferred.
  readonly #transactionForwardDuration = getOrCreateLatencyHistogram(
    'replication',
    'transaction_forward_duration',
    "Time from receiving a transaction's `begin` to forwarding its `commit`, " +
      'i.e. the change-streamer half of forward-to-subscriber latency.',
  );

  #latestStatus: Status;
  #latestLagReportCommitTimeMs = 0;
  #backupWatermark: string | undefined;
  #pgPurgedWatermark: string = '';
  /**
   * The floor last logged as held back by a laggard. The level-triggered
   * retry re-evaluates every {@link CLEANUP_DELAY_MS}; logging only when the
   * blocking floor moves keeps a stuck subscriber from emitting the same
   * line indefinitely.
   */
  #loggedBehindWatermark: string | undefined;
  #sqlitePurgeContinuation: PurgeContinuation | undefined;
  #purgeLock: PurgeLock | null;
  // PG and SQLite intentionally own separate level-triggered loops. Neither
  // waits for, advances, or retries the other.
  #pgPurgeScheduled = false;
  #pgPurgeRunning = false;
  #sqlitePurgeScheduled = false;
  #sqlitePurgeRunning = false;
  #stream: ChangeStream | undefined;
  #sqliteCatchup: SQLiteChangeLogCatchup | undefined;
  #changeLogUnavailableSince: number | undefined;
  #lastForwardedCommitWatermark: string | undefined;
  #transactionForwardStartedAt: number | undefined;
  #currentTransactionCompletion:
    | Resolver<ForwardedTransactionCompletion>
    | undefined;

  constructor(
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    discoveryAddress: string,
    discoveryProtocol: string,
    changeDB: PostgresDB,
    replicaVersion: string,
    source: ChangeSource,
    replicationStatusPublisher: ReplicationStatusPublisher,
    backupConfig: BackupConfig | null,
    initialPurgeLock: PurgeLock | null,
    autoReset: boolean,
    opts: TuningOptions,
    setTimeoutFn = setTimeout,
  ) {
    this.id = `change-streamer`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#shard = shard;
    this.#changeDB = changeDB;
    this.#replicaVersion = replicaVersion;
    this.#source = source;
    this.#storer = new Storer(
      lc,
      shard,
      taskID,
      discoveryAddress,
      discoveryProtocol,
      changeDB,
      replicaVersion,
      consumed => this.#acker.trackPgChangeLog(consumed[2].watermark),
      err => this.stop(err),
      opts,
    );
    this.#forwarder = new Forwarder(lc, {
      flowControlConsensusPaddingSeconds:
        opts.flowControlConsensusPaddingSeconds,
      eventDrivenRelease: opts.flowControlEventDrivenRelease,
    });
    this.#reservations = backupConfig
      ? new SnapshotReservations(lc, backupConfig, taskID =>
          this.#purgeScheduler?.resume(taskID),
        )
      : undefined;
    this.#replicationStatusPublisher = replicationStatusPublisher;
    this.#changeLogWriter = opts.sqliteChangeLogWriter
      ? new SQLiteChangeLogWriter(lc, {
          ...opts.sqliteChangeLogWriter,
          onCommit: watermark => {
            this.#sqliteCatchup?.onChangeLogCommit(watermark);
            // The commit closed the log's transaction, i.e. opened a purge
            // window (§3.3).
            this.#purgeScheduler?.onWriterIdle();
          },
          // Fail-soft deletes the file, and the reader is cached here, so it
          // has to go with it: otherwise it serves an unlinked inode while
          // every new open sees nothing.
          onDisabled: () => this.#closeSQLiteCatchup(),
        })
      : undefined;
    // The purge scheduler runs on the writer's own connection, which is also
    // its gate: no writer (mode `off`) means no scheduler, and a writer that
    // has not created the file yet -- or failed soft and deleted it -- makes
    // cycles skip rather than fail, so a late-appearing file is picked up
    // without a restart.
    this.#purgeScheduler =
      opts.sqliteChangeLogPurge && this.#changeLogWriter
        ? new SQLiteChangeLogPurgeScheduler(
            lc,
            () => this.#changeLogWriter?.connection,
            () => this.#forwarder.getAcks(),
            opts.sqliteChangeLogPurge,
          )
        : undefined;
    this.#acker = new UpstreamAcker({
      trackPgChangeLog: true, // TODO: set false when retiring PG
      trackBackup: backupConfig?.litestreamVersion === 'v5',
    });
    this.#sqliteCatchupOptions = opts.sqliteCatchup
      ? {
          ...opts.sqliteCatchup,
          // The real cleanup guard, in place of the catchup's no-op default:
          // registration and purge batches now serialize on one mutex.
          cleanupGuard:
            opts.sqliteCatchup.cleanupGuard ??
            this.#purgeScheduler?.cleanupGuard,
        }
      : undefined;
    this.#purgeLock = initialPurgeLock;
    this.#autoReset = autoReset;
    this.#state = new RunningState(this.id, undefined, setTimeoutFn);
    this.#latestStatus = {tag: 'status'};
  }

  async run() {
    this.#lc.info?.('starting change stream');

    this.#forwarder.startProgressMonitor();

    const lagReportInit = await this.#source.startLagReporter();
    if (lagReportInit) {
      this.#latestStatus.lagReport = {
        nextSendTimeMs: lagReportInit.nextSendTimeMs,
      };
      // Record the commit time of the initiated lag report (i.e. "head")
      // for the purpose of skipping over any lag reports that are re-streamed
      // by the change-source in the case of a change-streamer starting from
      // an older watermark.
      this.#latestLagReportCommitTimeMs = lagReportInit.firstCommitTimeMs;
    }

    // Once this change-streamer acquires "ownership" of the change DB,
    // it is safe to start the storer.
    await this.#storer.assumeOwnership(this.#purgeLock);
    this.#purgeLock = null;

    // The threshold in (estimated number of) bytes to send() on subscriber
    // websockets before `await`-ing the I/O buffers to be ready for more.
    const flushBytesThreshold = getDefaultHighWaterMark(false);

    while (this.#state.shouldRun()) {
      let err: unknown;
      let watermark: string | null = null;
      let unflushedBytes = 0;
      try {
        const {lastWatermark, backfillRequests, cookies} =
          await this.#storer.getStartStreamInitializationParameters();
        // SQLite catchup must not be eligible until this has been initialized
        // from the durable PG head. Commits observed only since process startup
        // are insufficient after a change-streamer restart.
        this.#lastForwardedCommitWatermark = lastWatermark;
        // Per stream connection, not once per process: the resume watermark is
        // re-read on every reconnect, so the log can be holding transactions
        // above it -- a restarted backfill's abandoned ones, in bulk -- and the
        // writer's plain INSERT would collide with the first re-delivery.
        // Ordered before startStream so no change can arrive mid-reconcile.
        //
        // The cookies come from the same read as the watermark, and go to the
        // log with it: a reconcile that moves the head discards the set the log
        // had folded, and only the set that belongs to this watermark can
        // replace it (invariant 15).
        this.#changeLogWriter?.reconcile({
          resumeWatermark: lastWatermark,
          cookies,
        });
        const stream = await this.#source.startStream(
          lastWatermark,
          backfillRequests,
        );
        this.#storer.run().catch(e => stream.changes.cancel(e));

        this.#stream = stream;
        if (
          this.#state.resetBackoff() >
          REPLICATION_STATUS_ERROR_DELAY_THRESHOLD_MS
        ) {
          // After recovering from a backoff for which a replication status
          // error was published, publish an OK status
          this.#replicationStatusPublisher.publish(
            this.#lc,
            'Replicating',
            `Replicating from ${lastWatermark}`,
          );
        }
        watermark = null;

        this.#acker.reset(stream.acks);
        for await (const change of stream.changes) {
          this.#acker.trackDownstream(change);

          const [type, msg] = change;
          switch (type) {
            case 'status':
              if (
                msg.lagReport &&
                msg.lagReport.lastTimings.commitTimeMs >=
                  this.#latestLagReportCommitTimeMs
              ) {
                // Lag reports are not stored in the cdc change log, but rather
                // only forwarded on "live" connections. When a new subscriber
                // is catching up, it is initialized with the #latestStatus
                // from which it can measure lag while catching up.
                this.#latestStatus.lagReport = msg.lagReport;
                this.#latestLagReportCommitTimeMs =
                  msg.lagReport.lastTimings.commitTimeMs;
                this.#forwarder.sendStatus(this.#latestStatus);
              }
              continue;
            case 'control':
              await this.#handleControlMessage(msg);
              continue; // control messages are not stored/forwarded
            case 'begin':
              watermark = change[2].commitWatermark;
              break;
            case 'commit':
              if (watermark !== change[2].watermark) {
                throw new UnrecoverableError(
                  `commit watermark ${change[2].watermark} does not match 'begin' watermark ${watermark}`,
                );
              }
              this.#txCounter.add(1);
              break;
            default:
              if (type === 'data') {
                this.#changeCounter.add(1);
              }
              if (watermark === null) {
                throw new UnrecoverableError(
                  `${type} change (${msg.tag}) received before 'begin' message`,
                );
              }
              break;
          }

          const json = this.#storer.store(watermark, change);
          // The SQLite change log commits at transaction boundaries, and its
          // commit for this transaction lands here -- before the forward of the
          // `commit` message, and before #recordForwardedTransactionBoundary
          // advances what #captureRequiredHead reads. No `await` separates it
          // from #storer.store() above, which is the assertable form of
          // invariant 1: the storer only enqueues, and its Postgres commit
          // cannot complete without yielding, so a synchronous SQLite commit in
          // the same loop iteration always precedes anything that can advance
          // the watermark this stream would resume from. Never throws; a write
          // failure disables the writer rather than stopping replication.
          this.#changeLogWriter?.write(change, json);
          const entry: WatermarkedChange = [watermark, change[1].tag, json];
          unflushedBytes += json.length;
          if (unflushedBytes < flushBytesThreshold) {
            // pipeline changes until flushBytesThreshold
            this.#forwarder.forward(entry);
            this.#recordForwardedTransactionBoundary(type, entry[0]);
          } else {
            // Wait for messages to clear socket buffers to ensure that they
            // make their way to subscribers. Without this `await`, the
            // messages end up being buffered in this process, which:
            // (1) results in memory pressure and increased GC activity
            // (2) prevents subscribers from processing the messages as they
            //     arrive, instead getting them in a large batch after being
            //     idle while they were queued (causing further delays).
            const forwarded = this.#forwarder.forwardWithFlowControl(entry);
            // forwardWithFlowControl synchronously sends the entry and updates
            // the Forwarder's transaction state before returning its flow-
            // control promise. Record the boundary before awaiting that promise
            // so registrations during the wait observe the forwarded state.
            this.#recordForwardedTransactionBoundary(type, entry[0]);
            await forwarded;
            unflushedBytes = 0;
          }

          if (type === 'commit' || type === 'rollback') {
            watermark = null;
          }

          // Allow the storer to exert back pressure.
          const readyForMore = this.#storer.readyForMore();
          if (readyForMore) {
            await readyForMore;
          }
        }
      } catch (e) {
        err = e;
      } finally {
        this.#stream?.changes.cancel();
        this.#stream = undefined;
      }

      // When the change stream is interrupted, abort any pending transaction.
      if (watermark) {
        this.#lc.warn?.(`aborting interrupted transaction ${watermark}`);
        this.#storer.abort();
        // Rolling back the log leaves no rows for the interrupted transaction,
        // so the next connection's reconciliation sees a head at or below its
        // resume watermark rather than a partial transaction.
        this.#changeLogWriter?.abort();
        // A rollback ends the log's open transaction without a commit
        // notification; wake any purge batch waiting for that window.
        this.#purgeScheduler?.onWriterIdle();
        this.#forwarder.forward([watermark, 'rollback', ROLLBACK_JSON]);
        this.#recordForwardedTransactionBoundary('rollback', watermark);
      }

      // Backoff and drain any pending entries in the storer before reconnecting.
      await Promise.all([
        this.#storer.stop(),
        this.#state.backoff(this.#lc, err),
        this.#state.retryDelay > REPLICATION_STATUS_ERROR_DELAY_THRESHOLD_MS
          ? publishCriticalEvent(
              this.#lc,
              replicationStatusError(this.#lc, 'Replicating', err),
            )
          : promiseVoid,
      ]);
    }

    this.#forwarder.stopProgressMonitor();
    this.#lc.info?.('ChangeStreamer stopped');
  }

  async #handleControlMessage(msg: ChangeStreamControl[1]) {
    this.#lc.info?.('received control message', msg);
    const {tag} = msg;

    switch (tag) {
      case 'reset-required':
        await markResetRequired(this.#changeDB, this.#shard);
        await publishReplicationError(
          this.#lc,
          'Replicating',
          msg.message ?? 'Resync required',
          msg.errorDetails,
        );
        if (this.#autoReset) {
          this.#lc.warn?.('shutting down for auto-reset');
          await this.stop(new AutoResetSignal());
        }
        break;
      default:
        unreachable(tag);
    }
  }

  async subscribe(ctx: SubscriberContext): Promise<Source<string>> {
    const {protocolVersion, id, mode, replicaVersion, watermark} = ctx;
    if (mode === 'serving') {
      this.#serving.resolve();
    }
    let cleanupSubscriber = () => {};
    const downstream = Subscription.create<string>({
      cleanup: () => cleanupSubscriber(),
    });
    // No subscriber's ACK advances the SQLite change log's head any more: the
    // writer runs in this process, so the barrier is notified from the commit
    // itself (see #changeLogWriter's onCommit).
    const subscriber = new Subscriber(
      protocolVersion,
      id,
      watermark,
      downstream,
      () => this.#latestStatus,
      {},
    );
    const lc = this.#lc.withContext('subscriber', subscriber.id);
    cleanupSubscriber = () => {
      lc.info?.(`removing subscriber ${subscriber.id}`);
      this.#forwarder.remove(subscriber);
    };
    if (replicaVersion !== this.#replicaVersion) {
      lc.warn?.(`rejecting subscriber at replica version ${replicaVersion}`);
      subscriber.close(
        ErrorType.WrongReplicaVersion,
        `current replica version is ${
          this.#replicaVersion
        } (requested ${replicaVersion})`,
      );
    } else {
      lc.info?.(`adding subscriber ${subscriber.id}`);

      const sqliteCatchup = this.#selectSQLiteCatchup(ctx);
      if (sqliteCatchup) {
        cleanupSubscriber = () => sqliteCatchup.remove(subscriber);
        await sqliteCatchup.catchup(subscriber, () =>
          this.#captureRequiredHead(),
        );
      } else {
        // Keep the existing PG registration/catchup lockstep unchanged when
        // SQLite was not selected before Forwarder.add().
        this.#forwarder.add(subscriber);
        this.#storer.catchup(subscriber, mode);
      }
    }
    // Any snapshot reservation held by this task can be closed now that
    // it is subscribed to the change stream.
    this.#reservations?.close(ctx.taskID);
    return downstream;
  }

  async startSnapshotReservation(
    taskID: string,
  ): Promise<Source<SnapshotMessage>> {
    if (!this.#reservations) {
      throw new Error('backups are not configured');
    }
    const downstream = this.#reservations.open(taskID);

    try {
      // Wait for an in-flight SQLite purge batch before reading and
      // advertising the reservation's bounds.
      await (this.#purgeScheduler?.pause(taskID) ?? promiseVoid);
      // If a backup has been confirmed, immediately confirm the reservation.
      await this.#confirmReservations();
      return downstream;
    } catch (e) {
      // Cancel the reservation this call opened, not whatever currently
      // holds the task's slot: an overlapping retry for the same taskID may
      // have already replaced it, and a by-taskID close would tear down the
      // replacement and release its purge pause while it is advertising
      // snapshot bounds. cancel() routes through the subscription's cleanup,
      // which closes the reservation only if this instance still owns it.
      downstream.cancel();
      throw e;
    }
  }

  trackBackupWatermark(watermark: string) {
    this.#backupWatermark = watermark;
    this.#acker.trackBackup(watermark);
    // The durable backup floor is an independent input to each change-log
    // implementation. SQLite receives it even when the PG log has already
    // reached this watermark.
    this.#requestSQLitePurge('deferred');
    this.#maybeSchedulePGPurge();
    this.#maybeScheduleSQLitePurge();

    // Confirm any waiting reservations now that a backup has been confirmed.
    // Note that this is asynchronous and best effort; if it fails, the watermark
    // is still "tracked" and the confirmation will be retried on the next backup.
    void this.#confirmReservations().catch(e =>
      this.#lc.warn?.(`error confirming snapshot reservation`, e),
    );
  }

  async #confirmReservations() {
    if (this.#backupWatermark && this.#reservations?.confirmationsRequired()) {
      const {replicaVersion, minWatermark} = await this.#getChangeLogState();
      if (minWatermark <= this.#backupWatermark) {
        this.#reservations?.confirm(replicaVersion, this.#backupWatermark);
      } else {
        // Something is wrong here ... the change-log should not have been
        // purged past the backup watermark. If we confirm the reservation,
        // we may not be able to catch up from the backup that the requester
        // restores.
        this.#lc.error?.(
          `change-log minWatermark ${minWatermark} is later than backupWatermark ${this.#backupWatermark}. ` +
            `Delaying confirmation of snapshot reservation until next backup.`,
        );
      }
    }
  }

  #maybeSchedulePGPurge(): void {
    const backupWatermark = this.#backupWatermark;
    if (
      this.#pgPurgeScheduled ||
      this.#pgPurgeRunning ||
      backupWatermark === undefined ||
      this.#pgPurgedWatermark >= backupWatermark
    ) {
      return;
    }
    this.#pgPurgeScheduled = true;
    this.#state.setTimeout(() => {
      this.#pgPurgeScheduled = false;
      this.#pgPurgeRunning = true;
      return this.#purgePGChangeLog().finally(() => {
        this.#pgPurgeRunning = false;
        this.#maybeSchedulePGPurge();
      });
    }, CLEANUP_DELAY_MS);
  }

  #maybeScheduleSQLitePurge(): void {
    if (
      this.#sqlitePurgeScheduled ||
      this.#sqlitePurgeRunning ||
      this.#backupWatermark === undefined ||
      this.#sqlitePurgeContinuation === undefined
    ) {
      return;
    }
    const delay =
      this.#sqlitePurgeContinuation === 'immediate' ? 0 : CLEANUP_DELAY_MS;
    this.#sqlitePurgeScheduled = true;
    this.#state.setTimeout(() => {
      this.#sqlitePurgeScheduled = false;
      this.#sqlitePurgeRunning = true;
      return this.#purgeSQLiteChangeLog().finally(() => {
        this.#sqlitePurgeRunning = false;
        this.#maybeScheduleSQLitePurge();
      });
    }, delay);
  }

  async #getChangeLogState(): Promise<{
    replicaVersion: string;
    minWatermark: string;
  }> {
    const minWatermark = await this.#storer.getMinWatermarkForCatchup();
    if (!minWatermark) {
      this.#lc.warn?.(
        `Unexpected empty changeLog. Resync if "Local replica watermark" errors arise`,
      );
    }
    return {
      replicaVersion: this.#replicaVersion,
      minWatermark: minWatermark ?? this.#replicaVersion,
    };
  }

  #getCleanupFloor(): {
    backupWatermark: string;
    purgeWatermark: string;
    current: string[];
  } {
    const backupWatermark = this.#backupWatermark;
    assert(
      backupWatermark !== undefined,
      'cleanup cannot run without a backup watermark',
    );
    const current = [
      ...this.#forwarder.getAcks(),
      ...(this.#reservations?.getReservedWatermarks() ?? []),
    ];
    // The cleanup delay above is the grace period for disconnected
    // subscribers to reconnect and expose their ACKs. Once it expires, an
    // empty set places no additional constraint on the confirmed backup
    // watermark and must not pin either change log indefinitely.
    return {
      backupWatermark,
      purgeWatermark: min(backupWatermark, ...current),
      current,
    };
  }

  async #purgePGChangeLog(): Promise<void> {
    try {
      const {backupWatermark, purgeWatermark, current} =
        this.#getCleanupFloor();
      if (purgeWatermark < backupWatermark) {
        if (this.#loggedBehindWatermark !== purgeWatermark) {
          this.#loggedBehindWatermark = purgeWatermark;
          this.#lc.info?.(
            `At least one client is behind backup ${backupWatermark}`,
            {watermarks: current},
          );
        }
      } else {
        this.#loggedBehindWatermark = undefined;
      }
      if (purgeWatermark <= this.#pgPurgedWatermark) {
        return;
      }
      this.#lc.info?.(`Purging PG changes before ${purgeWatermark} ...`);
      const start = performance.now();
      const deleted = await this.#storer.purgeRecordsBefore(purgeWatermark);
      const elapsed = (performance.now() - start).toFixed(2);
      this.#lc.info?.(
        `Purged ${deleted} PG changes before ${purgeWatermark} (${elapsed} ms)`,
      );
      this.#pgPurgedWatermark = purgeWatermark;
    } catch (e) {
      this.#lc.warn?.(`error purging the PG change log`, e);
    }
  }

  async #purgeSQLiteChangeLog(): Promise<void> {
    const scheduler = this.#purgeScheduler;
    if (!scheduler) {
      return;
    }
    try {
      const {backupWatermark, purgeWatermark} = this.#getCleanupFloor();
      // Consume the request before starting. A backup notification that
      // arrives during this pass records another request, which is merged with
      // the continuation returned by this pass rather than being overwritten.
      this.#sqlitePurgeContinuation = undefined;
      if (purgeWatermark < backupWatermark) {
        // Live constraint changes are not edge-triggered. Keep evaluating the
        // floor until it reaches the durable backup watermark, independently
        // of whether the PG implementation still exists.
        this.#requestSQLitePurge('deferred');
      }
      const result = await scheduler.purge(purgeWatermark);
      this.#requestSQLitePurge(result.continuation);
    } catch (e) {
      // purge() is fail-soft, but retain the coordinator's retry if an
      // unexpected caller-boundary failure escapes it.
      this.#requestSQLitePurge('deferred');
      this.#lc.warn?.(`error purging the SQLite change log`, e);
    }
  }

  #requestSQLitePurge(continuation: PurgeContinuation | undefined): void {
    if (!this.#purgeScheduler || continuation === undefined) {
      return;
    }
    if (
      continuation === 'immediate' ||
      this.#sqlitePurgeContinuation === undefined
    ) {
      this.#sqlitePurgeContinuation = continuation;
    }
  }

  async stop(err?: unknown) {
    this.#state.stop(this.#lc, err);
    this.#stream?.changes.cancel();
    this.#purgeScheduler?.stop();
    this.#sqliteCatchup?.close();
    this.#changeLogWriter?.close();
    await this.#storer.stop();
    await this.#source.stop();
  }

  #recordForwardedTransactionBoundary(
    type: ChangeStreamData[0],
    watermark: string,
  ) {
    switch (type) {
      case 'begin':
        assert(
          this.#currentTransactionCompletion === undefined,
          'forwarded begin while a transaction is already in progress',
        );
        this.#currentTransactionCompletion =
          resolver<ForwardedTransactionCompletion>();
        this.#transactionForwardStartedAt = performance.now();
        break;
      case 'commit': {
        const completion = this.#currentTransactionCompletion;
        assert(completion, 'forwarded commit without a pending transaction');
        this.#recordTransactionForwardDuration();
        this.#lastForwardedCommitWatermark = watermark;
        this.#currentTransactionCompletion = undefined;
        completion.resolve({kind: 'committed', watermark});
        break;
      }
      case 'rollback': {
        const completion = this.#currentTransactionCompletion;
        assert(completion, 'forwarded rollback without a pending transaction');
        const committed = this.#lastForwardedCommitWatermark;
        assert(committed, 'last forwarded commit watermark is not initialized');
        this.#transactionForwardStartedAt = undefined;
        this.#currentTransactionCompletion = undefined;
        completion.resolve({kind: 'rolled-back', watermark: committed});
        break;
      }
    }
  }

  #recordTransactionForwardDuration() {
    const startedAt = this.#transactionForwardStartedAt;
    this.#transactionForwardStartedAt = undefined;
    if (startedAt !== undefined) {
      this.#transactionForwardDuration.recordMs(performance.now() - startedAt, {
        sqlite_change_log: this.#changeLogWriter?.enabled ? 'on' : 'off',
      });
    }
  }

  /**
   * Closes and forgets the cached catchup coordinator, so that a later
   * subscription re-opens the log from scratch rather than reading a handle
   * whose file is gone.
   */
  #closeSQLiteCatchup() {
    this.#sqliteCatchup?.close();
    this.#sqliteCatchup = undefined;
  }

  #captureRequiredHead(): string | Promise<string> {
    const committed = this.#lastForwardedCommitWatermark;
    assert(committed, 'last forwarded commit watermark is not initialized');
    return (
      this.#currentTransactionCompletion?.promise.then(
        completion => completion.watermark,
      ) ?? committed
    );
  }

  #selectSQLiteCatchup(
    ctx: SubscriberContext,
  ): SQLiteChangeLogCatchup | undefined {
    const opts = this.#sqliteCatchupOptions;
    // Slice 11 supplies a non-default selector. Until then, this keeps all
    // production subscriptions on PG even though the complete SQLite handoff
    // path is wired and testable.
    if (this.#lastForwardedCommitWatermark === undefined || !opts) {
      return undefined;
    }
    // SQLite catchup is only for disposable serving replicas. Backup
    // subscribers retain the existing PG recovery policy until RMv2 can
    // resume the canonical replica directly from replica/backup/slot state.
    // Enforce this before the selector so no canary policy can override it.
    if (ctx.mode !== 'serving') {
      this.#lc.info?.(
        `not serving backup subscriber ${ctx.id} from SQLite catchup`,
      );
      return undefined;
    }
    if (ctx.logsChangeStream) {
      // The reason for this exclusion is gone: the writer is no longer a
      // subscriber, so nothing can wait on its own ACK, and no replicator this
      // version ships sets the parameter. It stays until slice 11 lifts it
      // deliberately, because a subscriber that predates the writer's move does
      // set it, and serving one from SQLite is a change in behavior rather than
      // a cleanup.
      this.#lc.warn?.(
        `not serving legacy change-log writer ${ctx.id} from SQLite catchup`,
      );
      return undefined;
    }
    if (!opts.shouldUse?.(ctx)) {
      return undefined;
    }
    return this.#sqliteCatchup ?? this.#openSQLiteCatchup(opts, ctx);
  }

  /**
   * Opens the change log and, if it can serve, wraps it in the coordinator that
   * subsequent subscriptions reuse.
   *
   * The log may not exist yet -- the writer creates it at the stream loop's
   * first reconcile, and deletes it when it fails soft -- or may exist without
   * content, or may be unreadable. None of those can be allowed to fail a
   * subscription:
   * this is the last point at which PG catchup is still available -- past
   * `Forwarder.add()` the subscriber is committed to SQLite -- so each declines
   * here instead. Neither the failure nor the reader is retained, so a later
   * subscription retries from scratch.
   */
  #openSQLiteCatchup(
    opts: SQLiteCatchupOptions,
    ctx: SubscriberContext,
  ): SQLiteChangeLogCatchup | undefined {
    let reader: SQLiteChangeLogReader | undefined;
    try {
      reader = new SQLiteChangeLogReader(this.#lc, opts.changeLogFile);
      // `plan()` doubles as the readiness check: it reports `not-ready` when
      // the writer has not created or seeded the stream table. The barrier
      // cannot wait its way out of that, since a log with no head can never
      // reach the required one.
      if (reader.plan(ctx.watermark).kind === 'not-ready') {
        reader.close();
        this.#declineSQLiteCatchup(
          ctx,
          opts,
          `${opts.changeLogFile} has no changes to serve yet`,
        );
        return undefined;
      }
    } catch (e) {
      // An absent file is the common case, since a readonly handle cannot
      // create one. A corrupt or truncated file lands here too, and is equally
      // not a reason to fail the subscription.
      reader?.close();
      this.#declineSQLiteCatchup(
        ctx,
        opts,
        `cannot read ${opts.changeLogFile}`,
        e,
      );
      return undefined;
    }
    this.#changeLogUnavailableSince = undefined;
    // Readiness is checked once, on the way to the cached coordinator: a log
    // with content keeps it. Purging preserves the latest transaction as a
    // catchup boundary, and `reconcileChangeLog` reseeds inside a single
    // transaction, so a reader never observes an emptied log.
    this.#sqliteCatchup = new SQLiteChangeLogCatchup(
      this.#lc,
      this.#forwarder,
      reader,
      {
        batchSize: opts.readBatchRows,
        barrierTimeoutMs: opts.barrierTimeoutMs,
        barrierPollIntervalMs: opts.barrierPollIntervalMs,
        cleanupGuard: opts.cleanupGuard,
      },
    );
    return this.#sqliteCatchup;
  }

  /**
   * Reports falling back to PG catchup, tracking how long the log has been
   * unavailable so that a subscription that merely arrived before the writer's
   * first reconcile does not look like an incident.
   */
  #declineSQLiteCatchup(
    ctx: SubscriberContext,
    opts: SQLiteCatchupOptions,
    reason: string,
    error?: unknown,
  ): void {
    const now = Date.now();
    this.#changeLogUnavailableSince ??= now;
    const unavailableMs = now - this.#changeLogUnavailableSince;
    const threshold =
      opts.notReadyWarnThresholdMs ??
      DEFAULT_CHANGE_LOG_UNAVAILABLE_WARN_THRESHOLD_MS;
    this.#lc[unavailableMs >= threshold ? 'warn' : 'debug']?.(
      `serving ${ctx.id} from PG catchup: ${reason} ` +
        `(unavailable for ${unavailableMs} ms)`,
      ...(error === undefined ? [] : [error]),
    );
  }
}

type ForwardedTransactionCompletion =
  | {kind: 'committed'; watermark: string}
  | {kind: 'rolled-back'; watermark: string};

// The delay between receiving an initial, backup-based watermark
// and performing a check of whether to purge records before it.
// This delay should be long enough to handle situations like the following:
//
// 1. `litestream restore` downloads a backup for the `replication-manager`
// 2. `replication-manager` starts up and runs this `change-streamer`
// 3. `zero-cache`s that are running on a different replica connect to this
//    `change-streamer` after exponential backoff retries.
//
// It is possible for a `zero-cache`[3] to be behind the backup restored [1].
// This cleanup delay (30 seconds) is thus set to be a value comfortably
// longer than the max delay for exponential backoff (10 seconds) in
// `services/running-state.ts`. This allows the `zero-cache` [3] to reconnect
// so that the `change-streamer` can track its progress and know when it has
// surpassed the initial watermark of the backup [1].
const CLEANUP_DELAY_MS = DEFAULT_MAX_RETRY_DELAY_MS * 3;

const ROLLBACK_JSON = JSON.stringify([
  'rollback',
  {tag: 'rollback'},
] satisfies Rollback);
