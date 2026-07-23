import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type SerializedDownstream,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import type {ReplicationStatusPublisher} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {ReplicationReportRecorder} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import {
  validateSQLiteChangeLogMaintenance,
  type SQLiteChangeLogMaintenance,
} from './sqlite-change-log-maintenance.ts';
import type {SQLiteChangeLogObserver} from './sqlite-change-log-observability.ts';
import type {SQLiteChangeLogPurgeResult} from './sqlite-change-log-purger.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

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
  readonly #sqliteChangeLogObserver: SQLiteChangeLogObserver | undefined;

  readonly #state = new RunningState('IncrementalSyncer');

  #workerCallPending = false;
  #transactionOpen = false;
  #pendingMaintenance: PendingMaintenance | undefined;
  #maintenanceDrain: Promise<void> | undefined;
  #stopping = false;

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
    sqliteChangeLogObserver: SQLiteChangeLogObserver | undefined,
  ) {
    this.#lc = lc;
    this.#taskID = taskID;
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#worker = worker;
    this.#mode = mode;
    this.#statusPublisher = statusPublisher;
    this.#notifier = new Notifier();
    this.#reporter = new ReplicationReportRecorder(lc);
    this.#sqliteChangeLogObserver = sqliteChangeLogObserver;
  }

  async run() {
    const lc = this.#lc;
    this.#worker.onError(err => void this.stop(lc, err));
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} = await this.#getSubscriptionState();

    // Notify any waiting subscribers that the replica is ready to be read.
    // This initial notification intentionally omits replicaReadyTimeMs because
    // it represents already-current state, not newly-unserved work.
    void this.#notifier.notifySubscribers({
      state: 'version-ready',
      watermark: initialWatermark,
    });

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} = await this.#getSubscriptionState();

      let downstream: Source<SerializedDownstream> | undefined;
      let unregister = () => {};
      let err: unknown | undefined;

      try {
        downstream = await this.#changeStreamer.subscribe({
          protocolVersion: PROTOCOL_VERSION,
          taskID: this.#taskID,
          id: this.#id,
          mode: this.#mode,
          watermark,
          replicaVersion,
          initial: watermark === initialWatermark,
        });
        this.#state.resetBackoff();
        unregister = this.#state.cancelOnStop(downstream);
        this.#statusPublisher?.publish(
          lc,
          'Replicating',
          `Replicating from ${watermark}`,
        );

        let backfillStatus: DownloadStatus | undefined;

        for await (const {data: message, json} of downstream) {
          this.#replicationEvents.add(1);
          switch (message[0]) {
            case 'status': {
              const {lagReport} = message[1];
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
              // Signal from the replication-manager that the view-syncer must
              // shut down and restore a new backup from litestream.
              const {type, message: msg} = message[1];
              void this.stop(
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
                const {status} = msg;
                if (!backfillStatus) {
                  // Start publishing the status every 3 seconds.
                  backfillStatus = status;
                  this.#statusPublisher?.publish(
                    lc,
                    'Replicating',
                    `Backfilling ${msg.relation.name} table`,
                    3000,
                    () =>
                      backfillStatus
                        ? {
                            downloadStatus: [
                              {
                                ...backfillStatus,
                                table: msg.relation.name,
                                columns: [
                                  ...msg.relation.rowKey.columns,
                                  ...msg.columns,
                                ],
                              },
                            ],
                          }
                        : {},
                  );
                }
                backfillStatus = status; // Update the current status
              }

              const data = message as ChangeStreamData;
              const start = performance.now();
              this.#sqliteChangeLogObserver?.messageReceived(data);
              let result: CommitResult | null;
              try {
                result = await this.#processMessage(data, json);
              } catch (e) {
                this.#sqliteChangeLogObserver?.messageFailed(
                  data,
                  e,
                  performance.now() - start,
                );
                throw e;
              }
              this.#sqliteChangeLogObserver?.messageProcessed(
                data,
                result,
                performance.now() - start,
              );

              this.#handleResult(lc, result);
              if (result?.completedBackfill) {
                backfillStatus = undefined;
              }
              break;
            }
          }
        }
        await this.#abortTransaction();
      } catch (e) {
        err = e;
        await this.#abortTransaction();
      } finally {
        downstream?.cancel();
        unregister();
        this.#statusPublisher?.stop();
      }
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
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
      });
    }
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  purgeChangeLog(
    maintenance: SQLiteChangeLogMaintenance,
  ): Promise<SQLiteChangeLogPurgeResult> {
    validateSQLiteChangeLogMaintenance(maintenance);
    if (this.#stopping) {
      return Promise.reject(
        new AbortError('IncrementalSyncer is stopping; maintenance rejected'),
      );
    }

    return new Promise((resolve, reject) => {
      const waiter = {resolve, reject};
      if (this.#pendingMaintenance) {
        // A request that has not reached the worker yet is deliberately
        // replaceable. In particular, a newer request may carry a lower safe
        // floor after an older subscriber registers.
        this.#pendingMaintenance.maintenance = maintenance;
        this.#pendingMaintenance.waiters.push(waiter);
      } else {
        this.#pendingMaintenance = {maintenance, waiters: [waiter]};
      }
      this.#kickMaintenanceDrain();
    });
  }

  stop(lc: LogContext, err?: unknown): Promise<void> {
    this.#stopping = true;
    this.#state.stop(lc, err);
    this.#rejectPendingMaintenance(
      new AbortError('IncrementalSyncer stopped before maintenance ran'),
    );
    return this.#maintenanceDrain ?? Promise.resolve();
  }

  #getSubscriptionState() {
    return this.#runWorkerCall(() => this.#worker.getSubscriptionState());
  }

  async #processMessage(
    data: ChangeStreamData,
    json: string,
  ): Promise<CommitResult | null> {
    if (data[0] === 'begin') {
      // Block maintenance before entering the worker. If begin fails after
      // opening SQLite's transaction, the outer error path must abort it
      // before a purge is allowed to run.
      this.#transactionOpen = true;
    }
    const result = await this.#runWorkerCall(async () => {
      const result = await this.#worker.processMessage({data, json});
      switch (data[0]) {
        case 'commit':
        case 'rollback':
          this.#transactionOpen = false;
          break;
      }
      return result;
    });
    this.#kickMaintenanceDrain();
    return result;
  }

  async #abortTransaction(): Promise<void> {
    this.#sqliteChangeLogObserver?.abort();
    if (this.#maintenanceDrain !== undefined) {
      await this.#waitForMaintenance();
    }
    assert(!this.#workerCallPending, 'worker call pending during abort');
    this.#worker.abort();
    this.#transactionOpen = false;
    this.#kickMaintenanceDrain();
  }

  async #runWorkerCall<T>(call: () => Promise<T>): Promise<T> {
    if (this.#maintenanceDrain !== undefined) {
      await this.#waitForMaintenance();
    }
    assert(!this.#workerCallPending, 'concurrent replication worker call');
    this.#workerCallPending = true;
    try {
      return await call();
    } finally {
      this.#workerCallPending = false;
      this.#kickMaintenanceDrain();
    }
  }

  async #waitForMaintenance(): Promise<void> {
    while (this.#maintenanceDrain !== undefined) {
      await this.#maintenanceDrain;
    }
  }

  #kickMaintenanceDrain(): void {
    if (
      this.#maintenanceDrain !== undefined ||
      this.#pendingMaintenance === undefined ||
      this.#workerCallPending ||
      this.#transactionOpen ||
      this.#stopping
    ) {
      return;
    }

    const drain = this.#runMaintenance().catch(error => {
      this.#lc.warn?.('error draining SQLite change-log maintenance', error);
      this.#rejectPendingMaintenance(error);
    });
    this.#maintenanceDrain = drain;
    void drain.finally(() => {
      if (this.#maintenanceDrain === drain) {
        this.#maintenanceDrain = undefined;
      }
      // Give a replication message already awaiting this batch the first
      // chance to claim the worker. With no source work, this still provides
      // the idle-drain trigger for the next coalesced request.
      queueMicrotask(() => this.#kickMaintenanceDrain());
    });
  }

  async #runMaintenance(): Promise<void> {
    assert(!this.#workerCallPending, 'concurrent maintenance worker call');
    assert(!this.#transactionOpen, 'maintenance during source transaction');
    const pending = this.#pendingMaintenance;
    assert(pending, 'maintenance drain started without a request');
    this.#pendingMaintenance = undefined;
    this.#workerCallPending = true;
    const {safeFloor, requestTimeMs, retentionMs, maxRows} =
      pending.maintenance;
    try {
      const result = await this.#worker.purgeChangeLog({
        externalFloor: safeFloor,
        retentionCutoffMs: requestTimeMs - retentionMs,
        maxRows,
      });
      for (const waiter of pending.waiters) {
        waiter.resolve(result);
      }
    } catch (error) {
      for (const waiter of pending.waiters) {
        waiter.reject(error);
      }
    } finally {
      this.#workerCallPending = false;
    }
  }

  #rejectPendingMaintenance(error: unknown): void {
    const pending = this.#pendingMaintenance;
    this.#pendingMaintenance = undefined;
    for (const waiter of pending?.waiters ?? []) {
      waiter.reject(error);
    }
  }
}

type PendingMaintenance = {
  maintenance: SQLiteChangeLogMaintenance;
  readonly waiters: Array<{
    resolve: (result: SQLiteChangeLogPurgeResult) => void;
    reject: (error: unknown) => void;
  }>;
};
