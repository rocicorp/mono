import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {mapPostgresToLiteIndex} from '../../db/pg-to-lite.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ReservationFollowup} from '../change-streamer/change-streamer-http.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type SubscriberContext,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {WrongReplicaVersion} from '../change-streamer/error-type-enum.ts';
import type {SubscribeDownstream} from '../change-streamer/subscribe.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {
  DrainHandler,
  type Downstream,
  type SwitchoverOptions,
} from './drain-handler.ts';
import {Notifier} from './notifier.ts';
import {
  IndexingProgress,
  type ReplicationStatusPublisher,
} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {
  ReplicationReportRecorder,
  type ReplicationLagReadinessOptions,
} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import {readReplicaBackfills} from './schema/backfilling.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

const MAX_WORKER_BATCH_MESSAGES = 256;
const MAX_WORKER_BATCH_SIZE = 1024 * 1024;

// The number of times to reconnect after being rejected by a
// replication-manager with an older replica version (e.g. one that has yet
// to be replaced by a resync), before accepting the rejection and restarting
// (e.g. if the upstream was legitimately replaced by one with a lower replica
// version).
const MAX_OLDER_REPLICA_VERSION_REJECTIONS = 20;

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link ChangeProcessor}, which runs
 * in a worker thread via the {@link WriteWorkerClient}.
 */
export class IncrementalSyncer {
  readonly #lc: LogContext;
  readonly #taskID: string;
  readonly #id: string;
  readonly #changeStreamer: ChangeStreamer;
  readonly #worker: WriteWorkerClient;
  readonly #mode: ReplicatorMode;
  readonly #statusPublisher: ReplicationStatusPublisher | null;
  readonly #notifier: Notifier;
  readonly #reporter: ReplicationReportRecorder;
  // Consumed (and cleared) by the first subscribe() in run()'s loop; every
  // subsequent (re)connection goes through #changeStreamer.subscribe().
  #initialConnection: ReservationFollowup | undefined;

  // Only set for serving replicas that switch over between
  // replication-managers.
  readonly #drainHandler: DrainHandler | undefined;
  #olderReplicaVersionRejections = 0;

  readonly #state = new RunningState('IncrementalSyncer');

  readonly #replicationEvents = getOrCreateCounter(
    'replication',
    'events',
    'Number of replication events processed',
  );

  constructor(
    lc: LogContext,
    taskID: string,
    id: string,
    changeStreamer: ChangeStreamer,
    worker: WriteWorkerClient,
    mode: ReplicatorMode,
    statusPublisher: ReplicationStatusPublisher | null,
    replicationLagReadinessOptions: ReplicationLagReadinessOptions = {},
    initialConnection?: ReservationFollowup,
    switchover?: SwitchoverOptions,
  ) {
    this.#lc = lc;
    this.#taskID = taskID;
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#worker = worker;
    this.#mode = mode;
    this.#statusPublisher = statusPublisher;
    this.#notifier = new Notifier();
    this.#reporter = new ReplicationReportRecorder(
      lc,
      replicationLagReadinessOptions,
    );
    this.#initialConnection = initialConnection;
    // Only serving replicas switch over between replication-managers. In
    // particular, the in-RM backup-replicator never does.
    this.#drainHandler =
      mode === 'serving' && switchover
        ? new DrainHandler(lc, changeStreamer, this.#state, switchover, () =>
            this.#committedContext(switchover.replicaFile),
          )
        : undefined;
  }

  ready(): Promise<void> {
    return this.#reporter.ready();
  }

  async run() {
    const lc = this.#lc;
    let workerError: Error | undefined;
    this.#worker.onError(err => {
      workerError ??= err;
      this.#state.stop(lc, err);
    });
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} =
      await this.#worker.getSubscriptionState();

    // Notify any waiting subscribers that the replica is ready to be read.
    // This initial notification intentionally omits replicaReadyTimeMs because
    // it represents already-current state, not newly-unserved work.
    void this.#notifier.notifySubscribers({
      state: 'version-ready',
      watermark: initialWatermark,
    });

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark, backfills} =
        await this.#worker.getSubscriptionState();

      let downstream: Downstream | undefined;
      let unregister = () => {};
      let err: unknown | undefined;

      try {
        const ctx = this.#subscriberContext(
          replicaVersion,
          watermark,
          backfills,
        );
        // A successful switchover has already established the subscription
        // on a new connection.
        const switched = this.#drainHandler?.getNextConnection();
        // The first connection reuses the still-open reservation connection
        // (if any) from the backup restore, so the replication-manager that
        // reserved the change log is the one that serves the subscription.
        // Every reconnect after that opens a fresh connection.
        const initialConnection = this.#initialConnection;
        this.#initialConnection = undefined;
        downstream =
          switched ??
          (initialConnection
            ? await initialConnection.subscribe(ctx)
            : await this.#changeStreamer.subscribe(ctx));
        unregister = this.#state.cancelOnStop(downstream);
        this.#statusPublisher?.publish(
          lc,
          'Replicating',
          `Replicating from ${watermark}`,
        );

        let backfill:
          | {status: DownloadStatus; table: string; columns: string[]}
          | undefined;
        let writeBatch: ChangeStreamData[] = [];
        let writeBatchSize = 0;
        let inTransaction = false;
        // Indexes created in the current run of consecutive index creations.
        let indexing: IndexingProgress | undefined;

        const publishBackfillStatus = (table: string) =>
          this.#statusPublisher?.publish(
            lc,
            'Replicating',
            `Backfilling ${table} table`,
            3000,
            () =>
              backfill
                ? {
                    downloadStatus: [
                      {
                        ...backfill.status,
                        table: backfill.table,
                        columns: backfill.columns,
                      },
                    ],
                  }
                : {},
          );

        const flushWrites = async () => {
          if (writeBatch.length === 0) {
            return;
          }
          const batch = writeBatch;
          writeBatch = [];
          writeBatchSize = 0;

          const result = await this.#worker.processMessages(batch);
          this.#handleResult(lc, result);
          if (result?.completedBackfill) {
            backfill = undefined;
          }
        };

        // Whether the current transaction is being skipped.
        let skipping = false;

        // A subscription established by a switchover starts at the
        // watermark at which it was requested, and may thus include
        // transactions that were since committed from the previous
        // subscription.
        const shouldSkip = (message: SubscribeDownstream) => {
          const type = message[0];
          if (type === 'begin' && message[2].commitWatermark <= watermark) {
            lc.debug?.(
              `skipping committed transaction ${message[2].commitWatermark}`,
            );
            skipping = true;
          }
          if (
            skipping &&
            (type === 'begin' ||
              type === 'data' ||
              type === 'commit' ||
              type === 'rollback')
          ) {
            skipping = type !== 'commit' && type !== 'rollback';
            return true;
          }
          return false;
        };

        for await (const {data: message, size} of downstream) {
          if (shouldSkip(message)) {
            continue;
          }
          // Only reset backoff after receiving an actual message.
          this.#state.resetBackoff();

          this.#replicationEvents.add(1);
          switch (message[0]) {
            case 'reserved':
              lc.warn?.(`unexpected snapshot reservation message received`, {
                message: message[0],
              });
              break;
            case 'status': {
              const {lagReport, draining} = message[1];
              this.#olderReplicaVersionRejections = 0;
              if (draining) {
                this.#drainHandler?.onChangeStreamerDrain(downstream);
              }
              if (lagReport) {
                const report: ReplicationReport = {
                  nextSendTimeMs: lagReport.nextSendTimeMs,
                };
                if (lagReport.lastTimings) {
                  report.lastTimings = {
                    ...lagReport.lastTimings,
                    replicateTimeMs: Date.now(),
                  };
                }
                this.#reporter.record(report);
              }
              break;
            }
            case 'error': {
              const {
                type,
                message: msg,
                replicaVersion: rmVersion,
              } = message[1];
              if (
                type === WrongReplicaVersion &&
                rmVersion !== undefined &&
                rmVersion < replicaVersion &&
                this.#olderReplicaVersionRejections++ <
                  MAX_OLDER_REPLICA_VERSION_REJECTIONS
              ) {
                // The replication-manager is behind this replica (e.g. it
                // has yet to be replaced by a resync). Rather than restoring
                // its older replica, reconnect (with backoff), which may
                // reach a different replication-manager.
                lc.warn?.(
                  `replication-manager is at an older replica version ` +
                    `(${rmVersion}) than ${replicaVersion}. reconnecting`,
                );
                break;
              }
              // Signal from the replication-manager that the view-syncer must
              // shut down and restore a new backup from litestream.
              this.stop(
                lc,
                // Note: The AbortError indicates a clean / intentional shutdown.
                new AbortError(
                  `${errorTypeToReadableName(type as ErrorType)}: ${msg}`,
                ),
              );
              break;
            }
            default: {
              const msg = message[1];
              if (msg.tag === 'backfill' && msg.status) {
                const {status, relation} = msg;
                const first = !backfill;
                backfill = {
                  status, // Update the current status
                  table: relation.name,
                  columns: [...relation.rowKey.columns, ...msg.columns],
                };
                if (first) {
                  // Start publishing the status every 3 seconds.
                  publishBackfillStatus(relation.name);
                }
              }

              if (msg.tag === 'create-index' && this.#statusPublisher) {
                // Creating an index on an existing table can take a long
                // time. Flush preceding changes so that the index creation
                // is processed (and timed) on its own.
                await flushWrites();
                const index = mapPostgresToLiteIndex(msg.spec);
                indexing ??= new IndexingProgress();
                indexing.start(index);
                this.#statusPublisher.publish(
                  lc,
                  'Replicating',
                  `Creating index ${index.name} on ${index.tableName}`,
                  3000,
                  indexing.state,
                );
                // Exclude the time spent publishing the status.
                indexing.restartTimer();
                writeBatch.push(message as ChangeStreamData);
                writeBatchSize += size;
                await flushWrites();
                const elapsed = indexing.finish();
                lc.info?.(
                  `Created index ${index.name} (${elapsed.toFixed(3)} ms)`,
                );
                this.#statusPublisher.publish(
                  lc,
                  'Replicating',
                  `Created index ${index.name} on ${index.tableName}`,
                  0,
                  indexing.state,
                );
                if (backfill) {
                  // Resume reporting the progress of the ongoing backfill.
                  publishBackfillStatus(backfill.table);
                }
                break;
              }
              indexing = undefined;

              const type = message[0];
              const invalidTransactionSequence =
                type === 'begin' ? inTransaction : !inTransaction;
              if (type === 'begin') {
                inTransaction = true;
              } else if (type === 'commit' || type === 'rollback') {
                inTransaction = false;
              }

              writeBatch.push(message as ChangeStreamData);
              writeBatchSize += size;
              if (
                // Waiting here ensures that consuming the commit, which ACKs
                // it upstream, only happens after the SQLite commit finishes.
                type === 'commit' ||
                type === 'rollback' ||
                // Promptly surface malformed transaction sequences instead
                // of leaving a partial batch waiting for another message.
                invalidTransactionSequence ||
                writeBatch.length >= MAX_WORKER_BATCH_MESSAGES ||
                writeBatchSize >= MAX_WORKER_BATCH_SIZE
              ) {
                await flushWrites();
              }
              break;
            }
          }
        }
        this.#worker.abort();
      } catch (e) {
        err = e;
        this.#worker.abort();
      } finally {
        downstream?.cancel();
        unregister();
        this.#statusPublisher?.stop();
      }
      if (err === undefined && this.#drainHandler?.hasNextConnection) {
        continue; // Switched over to a new subscription; no backoff.
      }
      this.#drainHandler?.getNextConnection()?.cancel();
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
    if (workerError) {
      throw workerError;
    }
  }

  #handleResult(lc: LogContext, result: CommitResult | null) {
    if (!result) {
      return;
    }
    if (result.completedBackfill) {
      // Publish the final status
      const status = result.completedBackfill;
      this.#statusPublisher?.publish(
        lc,
        'Replicating',
        `Backfilled ${status.table} table`,
        0,
        () => ({downloadStatus: [status]}),
      );
    } else if (result.schemaUpdated) {
      this.#statusPublisher?.publish(lc, 'Replicating', 'Schema updated');
    }
    if (result.watermark && result.changeLogUpdated) {
      void this.#notifier.notifySubscribers({
        state: 'version-ready',
        watermark: result.watermark,
        replicaReadyTimeMs: Date.now(),
        upstreamCommitTimeMs: result.upstreamCommitTimeMs,
      });
    }
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  #subscriberContext(
    replicaVersion: string,
    watermark: string,
    backfills: SubscriberContext['backfills'],
  ): SubscriberContext {
    return {
      protocolVersion: PROTOCOL_VERSION,
      taskID: this.#taskID,
      id: this.#id,
      mode: this.#mode,
      watermark,
      replicaVersion,
      backfills,
      ...(this.#drainHandler ? {drainable: true} : {}),
    };
  }

  /**
   * Returns the subscriber context as of the last committed transaction,
   * read from the replica file (i.e. not from the write worker, whose
   * connection may be in the middle of a transaction).
   */
  #committedContext(replicaFile: string): SubscriberContext {
    const db = new Database(this.#lc, replicaFile, {readonly: true});
    try {
      const {replicaVersion, watermark, backfills} = db.transaction(() => ({
        ...getSubscriptionState(new StatementRunner(db)),
        backfills: readReplicaBackfills(db),
      }));
      return this.#subscriberContext(replicaVersion, watermark, backfills);
    } finally {
      db.close();
    }
  }

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
    // Abort any polling loop that the write worker may be in.
    this.#worker.abort();
  }
}
