import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {Source} from '../../types/streams.ts';
import type {
  ChangeStreamer,
  Downstream,
} from '../change-streamer/change-streamer.ts';
import {RunningState} from '../running-state.ts';
import {ChangeProcessor, type TransactionMode} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {getSubscriptionState} from './schema/replication-state.ts';

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link ChangeProcessor}.
 */
export class IncrementalSyncer {
  readonly #id: string;
  readonly #changeStreamer: ChangeStreamer;
  readonly #replica: StatementRunner;
  readonly #mode: ReplicatorMode;
  readonly #txMode: TransactionMode;
  readonly #notifier: Notifier;

  readonly #state = new RunningState('IncrementalSyncer');

  constructor(
    id: string,
    changeStreamer: ChangeStreamer,
    replica: Database,
    mode: ReplicatorMode,
  ) {
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#replica = new StatementRunner(replica);
    this.#mode = mode;
    this.#txMode = mode === 'serving' ? 'CONCURRENT' : 'IMMEDIATE';
    this.#notifier = new Notifier();
  }

  async run(lc: LogContext) {
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} = getSubscriptionState(this.#replica);

    // Notify any waiting subscribers that the replica is ready to be read.
    this.#notifier.notifySubscribers();

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} = getSubscriptionState(this.#replica);
      const processor = new ChangeProcessor(
        this.#replica,
        this.#txMode,
        (lc: LogContext, err: unknown) => this.stop(lc, err),
      );

      let downstream: Source<Downstream> | undefined;
      let unregister = () => {};
      let err: unknown | undefined;

      try {
        downstream = await this.#changeStreamer.subscribe({
          id: this.#id,
          mode: this.#mode,
          watermark,
          replicaVersion,
          initial: watermark === initialWatermark,
        });
        this.#state.resetBackoff();
        unregister = this.#state.cancelOnStop(downstream);

        for await (const message of downstream) {
          if (message[0] === 'error') {
            // Unrecoverable error. Stop the service.
            this.stop(lc, message[1]);
            break;
          }
          if (processor.processMessage(lc, message)) {
            this.#notifier.notifySubscribers({state: 'version-ready'});
          }
        }
        processor.abort(lc);
      } catch (e) {
        err = e;
        processor.abort(lc);
      } finally {
        downstream?.cancel();
        unregister();
      }
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
  }
}
