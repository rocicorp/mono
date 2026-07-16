import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {
  Change,
  DownloadStatus,
} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type Downstream,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import type {ReplicationStatusPublisher} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {ReplicationReportRecorder} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

const MAX_COALESCED_TRANSACTIONS = 256;
const MAX_COALESCED_MESSAGES = 4096;

type PipelinedDownstream = {
  value: Downstream;
  consumed: () => void;
};

type BackfillState = {current: DownloadStatus | undefined};

function queuedMessages(source: Source<Downstream>): number {
  return (
    (
      source as Source<Downstream> & {
        readonly queued?: number | undefined;
      }
    ).queued ?? 0
  );
}

function isCoalescibleTag(tag: Change['tag']): boolean {
  switch (tag) {
    case 'begin':
    case 'insert':
    case 'update':
    case 'delete':
    case 'truncate':
    case 'commit':
      return true;
    default:
      return false;
  }
}

function assertTransactionBoundary(
  transaction: readonly PipelinedDownstream[],
  boundary: string,
) {
  assert(
    transaction.length === 0,
    `encountered ${boundary} in the middle of a transaction`,
  );
}

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
    // This initial notification intentionally omits replicaReadyTimeMs because
    // it represents already-current state, not newly-unserved work.
    void this.#notifier.notifySubscribers({
      state: 'version-ready',
      watermark: initialWatermark,
    });

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} =
        await this.#worker.getSubscriptionState();

      let downstream: Source<Downstream> | undefined;
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

        if (downstream.pipeline) {
          await this.#processPipelined(lc, downstream, downstream.pipeline);
        } else {
          await this.#processSynchronous(lc, downstream);
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

  async #processSynchronous(lc: LogContext, downstream: Source<Downstream>) {
    const backfill: BackfillState = {current: undefined};
    for await (const message of downstream) {
      this.#replicationEvents.add(1);
      if (this.#handleControlMessage(lc, message)) {
        continue;
      }
      this.#trackBackfill(lc, message as ChangeStreamData, backfill);
      const result = await this.#worker.processMessage(
        message as ChangeStreamData,
      );
      this.#handleResult(lc, result);
      if (result?.completedBackfill) {
        backfill.current = undefined;
      }
    }
  }

  async #processPipelined(
    lc: LogContext,
    downstream: Source<Downstream>,
    pipeline: AsyncIterable<PipelinedDownstream>,
  ) {
    const backfill: BackfillState = {current: undefined};
    let currentTransaction: PipelinedDownstream[] = [];
    let currentTransactionCoalescible = true;
    let batch: PipelinedDownstream[] = [];
    let batchTransactions = 0;

    const handleResults = (results: readonly CommitResult[]) => {
      for (const result of results) {
        this.#handleResult(lc, result);
        if (result.completedBackfill) {
          backfill.current = undefined;
        }
      }
    };

    const handleCoalescedResults = (results: readonly CommitResult[]) => {
      const last = results.at(-1);
      if (!last) {
        return;
      }
      this.#handleResult(lc, {
        ...last,
        schemaUpdated: results.some(result => result.schemaUpdated),
        changeLogUpdated: results.some(result => result.changeLogUpdated),
      });
    };

    const flushBatch = async () => {
      if (batch.length === 0) {
        return;
      }
      const entries = batch;
      batch = [];
      batchTransactions = 0;
      const results = await this.#worker.processMessages(
        entries.map(({value}) => value as ChangeStreamData),
      );
      handleCoalescedResults(results);
    };

    const processTransaction = async (entries: PipelinedDownstream[]) => {
      const results = await this.#worker.processMessages(
        entries.map(({value}) => value as ChangeStreamData),
      );
      handleResults(results);
    };

    for await (const entry of pipeline) {
      const {value: message} = entry;
      this.#replicationEvents.add(1);

      // This acknowledgement is transport flow control: the message has been
      // copied into this process and can be replayed after a crash from the
      // durable SQLite watermark. Waiting for the coalesced physical commit can
      // deadlock when the transport's flow-control window ends mid-transaction.
      entry.consumed();

      if (message[0] === 'status' || message[0] === 'error') {
        assertTransactionBoundary(currentTransaction, message[0]);
        await flushBatch();
        this.#handleControlMessage(lc, message);
        continue;
      }

      this.#trackBackfill(lc, message as ChangeStreamData, backfill);
      currentTransaction.push(entry);
      const tag = message[1].tag;
      if (!isCoalescibleTag(tag)) {
        currentTransactionCoalescible = false;
      }

      if (tag === 'rollback') {
        await flushBatch();
        // Rollbacks cannot share an outer transaction because rolling one back
        // would also discard earlier logical commits in the group.
        for (const txEntry of currentTransaction) {
          const result = await this.#worker.processMessage(
            txEntry.value as ChangeStreamData,
          );
          this.#handleResult(lc, result);
        }
        currentTransaction = [];
        currentTransactionCoalescible = true;
        continue;
      }

      if (tag !== 'commit') {
        continue;
      }

      if (!currentTransactionCoalescible) {
        await flushBatch();
        await processTransaction(currentTransaction);
      } else {
        batch.push(...currentTransaction);
        batchTransactions++;
      }
      currentTransaction = [];
      currentTransactionCoalescible = true;

      if (
        batchTransactions >= MAX_COALESCED_TRANSACTIONS ||
        batch.length >= MAX_COALESCED_MESSAGES ||
        queuedMessages(downstream) === 0
      ) {
        await flushBatch();
      }
    }

    assertTransactionBoundary(currentTransaction, 'end of stream');
    await flushBatch();
  }

  #handleControlMessage(lc: LogContext, message: Downstream): boolean {
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
        return true;
      }
      case 'error': {
        // Signal from the replication-manager that the view-syncer must shut
        // down and restore a new backup from litestream.
        const {type, message: msg} = message[1];
        this.stop(
          lc,
          // AbortError indicates a clean / intentional shutdown.
          new AbortError(
            `${errorTypeToReadableName(type as ErrorType)}: ${msg}`,
          ),
        );
        return true;
      }
      default:
        return false;
    }
  }

  #trackBackfill(
    lc: LogContext,
    message: ChangeStreamData,
    state: BackfillState,
  ): void {
    const msg = message[1];
    if (msg.tag !== 'backfill' || !msg.status) {
      return;
    }
    const {status} = msg;
    if (!state.current) {
      // Start publishing the status every 3 seconds.
      this.#statusPublisher?.publish(
        lc,
        'Replicating',
        `Backfilling ${msg.relation.name} table`,
        3000,
        () =>
          state.current
            ? {
                downloadStatus: [
                  {
                    ...state.current,
                    table: msg.relation.name,
                    columns: [...msg.relation.rowKey.columns, ...msg.columns],
                  },
                ],
              }
            : {},
      );
    }
    state.current = status;
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

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
  }
}
