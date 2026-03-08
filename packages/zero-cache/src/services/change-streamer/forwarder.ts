import type {LogContext} from '@rocicorp/logger';
import {joinIterables, wrapIterable} from '../../../../shared/src/iterables.ts';
import type {ChangeStreamData} from '../change-source/protocol/current.ts';
import {Broadcast} from './broadcast.ts';
import type {WatermarkedChange} from './change-streamer-service.ts';
import type {Subscriber} from './subscriber.ts';

export type ProgressMonitorOptions = {
  flowControlConsensusPaddingSeconds: number;
};

export class Forwarder {
  readonly #lc: LogContext;
  readonly #progressMonitorOptions: ProgressMonitorOptions;
  readonly #active = new Set<Subscriber>();
  readonly #queued = new Set<Subscriber>();
  #inTransaction = false;

  #currentBroadcast: Broadcast | undefined;
  #progressMonitor: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    opts: ProgressMonitorOptions = {flowControlConsensusPaddingSeconds: 1},
  ) {
    this.#lc = lc.withContext('component', 'progress-monitor');
    this.#progressMonitorOptions = opts;
  }

  startProgressMonitor() {
    clearInterval(this.#progressMonitor);
    this.#progressMonitor = setInterval(this.#trackProgress, 1000);
  }

  readonly #trackProgress = () => {
    const now = performance.now();
    for (const sub of this.#active) {
      sub.sampleProcessRate(now);
    }

    const {flowControlConsensusPaddingSeconds} = this.#progressMonitorOptions;
    // A negative number disables early flow control release.
    if (flowControlConsensusPaddingSeconds >= 0) {
      this.#currentBroadcast?.checkProgress(
        this.#lc,
        flowControlConsensusPaddingSeconds * 1000,
        now,
      );
    }
  };

  stopProgressMonitor() {
    clearInterval(this.#progressMonitor);
  }

  /**
   * `add()` is called in lock step with `Storer.catchup()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  add(sub: Subscriber) {
    if (this.#inTransaction) {
      this.#queued.add(sub);
    } else {
      this.#active.add(sub);
    }
  }

  remove(sub: Subscriber) {
    this.#active.delete(sub);
    this.#queued.delete(sub);
    sub.close();
  }

  /**
   * `forward()` is called in lockstep with `Storer.store()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   *
   * This version of forward is fire-and-forget, with no flow control. The
   * change-streamer should call and await {@link forwardWithFlowControl()}
   * occasionally to avoid memory blowup.
   */
  forward(entry: WatermarkedChange) {
    Broadcast.withoutTracking(this.#active.values(), entry);
    this.#updateActiveSubscribers(entry[1]);
  }

  /**
   * The flow-control-aware equivalent of {@link forward()}, returning a
   * Promise that resolves when replication should continue.
   */
  async forwardWithFlowControl(entry: WatermarkedChange) {
    const broadcast = new Broadcast(this.#active.values(), entry);
    this.#updateActiveSubscribers(entry[1]);

    // set for progress tracking
    this.#currentBroadcast = broadcast;

    await broadcast.done;

    // Technically #currentBroadcast may have changed, so only
    // unset if it if is still the same.
    if (this.#currentBroadcast === broadcast) {
      this.#currentBroadcast = undefined;
    }
  }

  #updateActiveSubscribers([type]: ChangeStreamData) {
    switch (type) {
      case 'begin':
        // While in a Transaction, all added subscribers are "queued" so that no
        // messages are forwarded to them. This state corresponds to being queued
        // for catchup in the Storer, which will retrieve historic changes
        // and call catchup() once the current transaction is committed.
        this.#inTransaction = true;
        break;
      case 'commit':
      case 'rollback':
        // Upon commit or rollback, all queued subscribers are transferred to
        // the active set. This means that they can receive messages starting
        // from the next transaction.
        //
        // Note that if catchup is still in progress (in the Storer), these messages
        // will be buffered in the backlog until catchup completes.
        this.#inTransaction = false;
        for (const sub of this.#queued.values()) {
          this.#active.add(sub);
        }
        this.#queued.clear();
        break;
    }
  }

  getAcks(): Set<string> {
    return new Set(
      joinIterables(
        wrapIterable(this.#active).map(s => s.acked),
        wrapIterable(this.#queued).map(s => s.acked),
      ),
    );
  }
}
