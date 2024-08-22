import {EventEmitter} from 'eventemitter3';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from './replicator.js';

/**
 * Handles the semantics of {@link Replicator.subscribe()} notifications,
 * namely:
 *
 * * New subscribers are notified immediately if the Replicator has already
 *   sent one notification (indicating that the Replica is ready to be read).
 *
 * * Notifications to a subscriber are coalesced is the subscriber is too
 *   busy to receive them.
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
export class Notifier {
  readonly #eventEmitter = new EventEmitter();
  #firstNotificationReceived = false;

  #newSubscription() {
    const notify = (payload: ReplicaVersionReady) => subscription.push(payload);
    const subscription = Subscription.create<ReplicaVersionReady>({
      coalesce: curr => curr,
      cleanup: () => this.#eventEmitter.off('version', notify),
    });
    return {notify, subscription};
  }

  addSubscription(): CancelableAsyncIterable<ReplicaVersionReady> {
    const {notify, subscription} = this.#newSubscription();
    this.#eventEmitter.on('version', notify);
    if (this.#firstNotificationReceived) {
      // Per Replicator.subscribe() semantics, once a notification has been
      // sent, the Replica is ready to be read, and henceforth new
      // subscribers receive a notification immediately upon subscribe().
      notify({});
    }
    return subscription;
  }

  // Note: The payload is only used for testing coalesce-behavior.
  notifySubscribers(payload: ReplicaVersionReady = {}) {
    this.#firstNotificationReceived = true;
    this.#eventEmitter.listeners('version').forEach(notify => notify(payload));
  }
}
