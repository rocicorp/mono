import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {mapPostgresToLiteIndex} from '../../db/pg-to-lite.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type SizedDownstream,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import {
  IndexingProgress,
  type ReplicationStatusPublisher,
} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {ReplicationReportRecorder} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

const MAX_WORKER_BATCH_MESSAGES = 256;
const MAX_WORKER_BATCH_SIZE = 1024 * 1024;

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
      const {replicaVersion, watermark} =
        await this.#worker.getSubscriptionState();

      let downstream: Source<SizedDownstream> | undefined;
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
          // The SQLite change log is written by the change-streamer itself, so
          // no replicator logs the change stream any more. The parameter stays
          // on the wire for change-streamers that still exclude a writer from
          // SQLite catchup.
          logsChangeStream: false,
        });
        this.#state.resetBackoff();
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

        for await (const {data: message, size} of downstream) {
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

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
    // Abort any polling loop that the write worker may be in.
    this.#worker.abort();
  }
}
