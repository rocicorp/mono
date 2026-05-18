import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type ChangeStreamerDownstream,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import type {ReplicationStatusPublisher} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {ReplicationReportRecorder} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import {WorkerMessageBatcher} from './worker-message-batcher.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

// Batch replication messages before crossing into the write worker so
// data-heavy transactions do not pay one IPC round trip per row. The
// rm-vs-load benchmark covers the 1 RM / 16 VS case this is meant to protect;
// the cap keeps unusually large upstream transactions from sitting in the
// syncer heap while the worker is idle.
const MAX_WORKER_BATCH_MESSAGES = 64;

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
  }

  async run() {
    const lc = this.#lc;
    this.#worker.onError(err => this.#state.stop(lc, err));
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} =
      await this.#worker.getSubscriptionState();

    // Notify any waiting subscribers that the replica is ready to be read.
    void this.#notifier.notifySubscribers();

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} =
        await this.#worker.getSubscriptionState();

      let downstream: Source<ChangeStreamerDownstream> | undefined;
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
        const workerBatch = new WorkerMessageBatcher(
          this.#worker,
          MAX_WORKER_BATCH_MESSAGES,
        );
        const handleWorkerResult = (result: CommitResult | null) => {
          this.#handleResult(lc, result);
          if (result?.completedBackfill) {
            backfillStatus = undefined;
          }
        };
        const processChangeStreamData = (
          message: ChangeStreamData,
        ): Promise<void> | undefined => {
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

          return workerBatch.push(message)?.then(handleWorkerResult);
        };
        const processChangeStreamDataBatch = async (
          messages: readonly ChangeStreamData[],
        ) => {
          for (const message of messages) {
            const result = processChangeStreamData(message);
            if (result) {
              await result;
            }
          }
        };

        for await (const message of downstream) {
          this.#replicationEvents.add(
            message[0] === 'change-batch' ? message[1].changes.length : 1,
          );
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
              this.stop(
                lc,
                // Note: The AbortError indicates a clean / intentional shutdown.
                new AbortError(
                  `${errorTypeToReadableName(type as ErrorType)}: ${msg}`,
                ),
              );
              break;
            }
            case 'change-batch':
              // #6001: https://github.com/rocicorp/mono/pull/6001
              // The v7 RM -> VS protocol sends row-heavy traffic as one
              // ordered batch, so a VS applies the same changes with fewer
              // parse/ACK/worker-dispatch units. The outer stream message is
              // not ACKed until this loop finishes applying the batch and
              // requests the next message.
              await processChangeStreamDataBatch(message[1].changes);
              break;
            default: {
              const result = processChangeStreamData(message);
              if (result) {
                await result;
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
      void this.#notifier.notifySubscribers({state: 'version-ready'});
    }
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
  }
}
