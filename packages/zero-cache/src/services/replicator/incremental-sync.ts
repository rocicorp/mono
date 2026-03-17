import type {LogContext} from '@rocicorp/logger';
import type {Database} from '../../../../zqlite/src/db.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import {
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type Downstream,
} from '../change-streamer/change-streamer.ts';
import type {CommitResult} from './change-processor.ts';
import {RunningState} from '../running-state.ts';
import {Notifier} from './notifier.ts';
import {ReplicationStatusPublisher} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link ChangeProcessor}, which runs
 * in a worker thread via the {@link WriteWorkerClient}.
 */
export class IncrementalSyncer {
  readonly #taskID: string;
  readonly #id: string;
  readonly #changeStreamer: ChangeStreamer;
  readonly #statusDb: Database;
  readonly #worker: WriteWorkerClient;
  readonly #mode: ReplicatorMode;
  readonly #publishReplicationStatus: boolean;
  readonly #notifier: Notifier;

  readonly #state = new RunningState('IncrementalSyncer');

  readonly #replicationEvents = getOrCreateCounter(
    'replication',
    'events',
    'Number of replication events processed',
  );

  constructor(
    taskID: string,
    id: string,
    changeStreamer: ChangeStreamer,
    statusDb: Database,
    worker: WriteWorkerClient,
    mode: ReplicatorMode,
    publishReplicationStatus: boolean,
  ) {
    this.#taskID = taskID;
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#statusDb = statusDb;
    this.#worker = worker;
    this.#mode = mode;
    this.#publishReplicationStatus = publishReplicationStatus;
    this.#notifier = new Notifier();
  }

  async run(lc: LogContext) {
    this.#worker.onError(err => this.#state.stop(lc, err));
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} =
      await this.#worker.getSubscriptionState();

    // Notify any waiting subscribers that the replica is ready to be read.
    void this.#notifier.notifySubscribers();

    // Only the backup replicator publishes replication status events.
    const statusPublisher = this.#publishReplicationStatus
      ? new ReplicationStatusPublisher(this.#statusDb)
      : undefined;

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
        statusPublisher?.publish(
          lc,
          'Replicating',
          `Replicating from ${watermark}`,
        );

        let backfillStatus: DownloadStatus | undefined;

        for await (const message of downstream) {
          this.#replicationEvents.add(1);
          switch (message[0]) {
            case 'status':
              // Used for checking if a replica can be caught up. Not
              // relevant here.
              lc.debug?.(`Received initial status`, message[1]);
              break;
            case 'error':
              // Unrecoverable error. Stop the service.
              this.stop(lc, message[1]);
              break;
            default: {
              const msg = message[1];
              if (msg.tag === 'backfill' && msg.status) {
                const {status} = msg;
                if (!backfillStatus) {
                  // Start publishing the status every 3 seconds.
                  backfillStatus = status;
                  statusPublisher?.publish(
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

              const result = await this.#worker.processMessage(
                message as ChangeStreamData,
              );

              this.#handleResult(lc, result, statusPublisher);
              if (result?.completedBackfill) {
                backfillStatus = undefined;
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
        statusPublisher?.stop();
      }
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  #handleResult(
    lc: LogContext,
    result: CommitResult | null,
    statusPublisher: ReplicationStatusPublisher | undefined,
  ) {
    if (!result) {
      return;
    }
    if (result.completedBackfill) {
      // Publish the final status
      const status = result.completedBackfill;
      statusPublisher?.publish(
        lc,
        'Replicating',
        `Backfilled ${status.table} table`,
        0,
        () => ({downloadStatus: [status]}),
      );
    } else if (result.schemaUpdated) {
      statusPublisher?.publish(lc, 'Replicating', 'Schema updated');
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
