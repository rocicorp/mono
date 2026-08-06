import {EventEmitter} from 'eventemitter3';
import {
  type PendingResult,
  type Result,
  Subscription,
} from '../../types/subscription.ts';
import type {ReplicaState, ReplicaStateNotifier} from './replicator.ts';

/**
 * Handles the semantics of {@link ReplicatorVersionNotifier.subscribe()}
 * notifications, namely:
 *
 * * New subscribers are notified immediately with the latest received
 *   ReplicaState.
 *
 * * Non-latest notifications are discarded if the subscriber is too
 *   busy to consume them.
 *
 * By encapsulating the state for the first behavior (essentially, whether
 * the first notification has been sent by the Replicator), Notifier objects
 * can be chained to simplify fanout from Replicator to View Syncers.
 *
 * In particular, each Syncer Thread can manage a single Subscription to
 * the Replicator across a MessageChannel, which it uses for its own Notifier
 * instance to manage subscriptions from View Syncers within its thread. This
 * way a Replicator only deals with sending notifications to a bounded set
 * of MessageChannel-based subscribers (Syncer Threads), while the dynamic
 * subscribe and unsubscribe traffic from View Syncers remains within each
 * Syncer Thread.
 */
function oldest(
  curr: number | undefined,
  prev: number | undefined,
): number | undefined {
  if (curr === undefined) {
    return prev;
  }
  if (prev === undefined) {
    return curr;
  }
  return Math.min(curr, prev);
}

export class Notifier implements ReplicaStateNotifier {
  readonly #eventEmitter = new EventEmitter();
  #lastStateReceived: ReplicaState | undefined;

  get latestState(): ReplicaState | undefined {
    return this.#lastStateReceived;
  }

  #newSubscription() {
    const notify = (state: ReplicaState) => subscription.push(state);
    const subscription = Subscription.create<ReplicaState>({
      // A coalesced notification stands in for every notification it
      // subsumed, so both timestamps keep the *oldest* value: they measure how
      // long work has been outstanding, and the subsumed watermarks are still
      // unserved.
      coalesce: (curr, prev) => ({
        ...curr,
        replicaReadyTimeMs: oldest(
          curr.replicaReadyTimeMs,
          prev.replicaReadyTimeMs,
        ),
        upstreamCommitTimeMs: oldest(
          curr.upstreamCommitTimeMs,
          prev.upstreamCommitTimeMs,
        ),
      }),
      cleanup: () => this.#eventEmitter.off('version', notify),
    });
    return {notify, subscription};
  }

  subscribe(): Subscription<ReplicaState> {
    const {notify, subscription} = this.#newSubscription();
    this.#eventEmitter.on('version', notify);
    if (this.#lastStateReceived) {
      // Per Replicator.subscribe() semantics, the current state of the
      // replica, if known, is immediately sent on subscribe.
      notify(this.#lastStateReceived);
    }
    return subscription;
  }

  notifySubscribers(
    state: ReplicaState = {state: 'version-ready'},
  ): Promise<Result>[] {
    this.#lastStateReceived = state;
    return this.#eventEmitter
      .listeners('version')
      .map(notify => notify(state) as unknown as PendingResult)
      .map(pending => pending.result);
  }
}
