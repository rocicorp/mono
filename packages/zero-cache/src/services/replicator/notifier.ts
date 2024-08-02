import {EventEmitter} from 'eventemitter3';
import type {TransactionPool} from '../../db/transaction-pool.js';
import type {CancelableAsyncIterable} from '../../types/streams.js';
import {Subscription} from '../../types/subscription.js';
import type {VersionChange} from './replicator.js';

export type InternalVersionChange = VersionChange & {
  readers: TransactionPool;
};

export class Notifier {
  readonly #eventEmitter = new EventEmitter();

  #newSubscription() {
    const subscribe = (v: InternalVersionChange) => subscription.push(v);
    const subscription: Subscription<VersionChange, InternalVersionChange> =
      new Subscription<VersionChange, InternalVersionChange>(
        {
          consumed: prev => prev.readers.unref(),
          coalesce: (curr, prev) => {
            curr.readers.unref();
            return {
              newVersion: curr.newVersion,
              prevVersion: prev.prevVersion,
              prevSnapshotID: prev.prevSnapshotID,
              readers: prev.readers,
              invalidations:
                !prev.invalidations || !curr.invalidations
                  ? undefined
                  : {
                      ...prev.invalidations,
                      ...curr.invalidations,
                    },
              changes:
                !prev.changes || !curr.changes
                  ? undefined
                  : [...prev.changes, ...curr.changes],
            };
          },
          cleanup: unconsumed => {
            this.#eventEmitter.off('version', subscribe);
            unconsumed.forEach(m => m.readers?.unref());
          },
        },
        ivc => {
          const {readers: _excluded, ...vc} = ivc;
          return vc;
        },
      );
    return {subscribe, subscription};
  }

  addSubscription(): Promise<CancelableAsyncIterable<VersionChange>> {
    const {subscribe, subscription} = this.#newSubscription();
    this.#eventEmitter.on('version', subscribe);
    return Promise.resolve(subscription);
  }

  notifySubscribers(v: InternalVersionChange) {
    this.#eventEmitter.listeners('version').forEach(listener => {
      v.readers.ref();
      listener(v);
    });
  }
}
