import {EventEmitter} from 'eventemitter3';
import {
  type PendingResult,
  type Result,
  Subscription,
} from '../../types/subscription.js';
import type {ReplicaState, ReplicaStateNotifier} from './replicator.js';

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
export class Notifier implements ReplicaStateNotifier {
  readonly #eventEmitter = new EventEmitter();
  #lastStateReceived: ReplicaState | undefined;

  #newSubscription() {
    const notify = (state: ReplicaState) => subscription.push(state);
    const subscription = Subscription.create<ReplicaState>({
      coalesce: curr => curr,
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
