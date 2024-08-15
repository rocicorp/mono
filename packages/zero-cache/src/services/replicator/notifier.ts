import {EventEmitter} from 'eventemitter3';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import {ReplicaVersionReady} from './replicator.js';

export class Notifier {
  readonly #eventEmitter = new EventEmitter();

  #newSubscription() {
    const notify = (payload: ReplicaVersionReady) => subscription.push(payload);
    const subscription = Subscription.create<ReplicaVersionReady>({
      coalesce: curr => curr,
      cleanup: () => this.#eventEmitter.off('version', notify),
    });
    return {notify, subscription};
  }

  addSubscription(
    notifyImmediately: boolean,
  ): CancelableAsyncIterable<ReplicaVersionReady> {
    const {notify, subscription} = this.#newSubscription();
    this.#eventEmitter.on('version', notify);
    if (notifyImmediately) {
      notify({});
    }
    return subscription;
  }

  // Note: The payload is only used for testing coalesce-behavior.
  notifySubscribers(payload: ReplicaVersionReady = {}) {
    this.#eventEmitter.listeners('version').forEach(notify => notify(payload));
  }
}
