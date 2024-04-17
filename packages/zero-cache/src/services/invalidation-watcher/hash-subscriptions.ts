import type {Subscription} from '../../types/subscription.js';
import type {
  QueryInvalidationUpdate,
  WatchRequest,
} from './invalidation-watcher.js';

type SubscriptionsToQueryIDs = Map<
  Subscription<QueryInvalidationUpdate>,
  Set<string>
>;

/**
 * Tracks the hashes that are relevant to Subscriptions and the queries to which
 * they correspond, for constructing {@link QueryInvalidationUpdate} messages
 * from a set of invalidated hashes.
 */
export class HashSubscriptions {
  readonly #hashToSubscription = new Map<string, SubscriptionsToQueryIDs>();

  empty() {
    return this.#hashToSubscription.size === 0;
  }

  add(sub: Subscription<QueryInvalidationUpdate>, req: WatchRequest) {
    for (const [queryID, {hashes}] of Object.entries(req.queries)) {
      hashes.forEach(hash => {
        const queryIDs = ensureMap(this.#hashToSubscription, hash);
        ensureSet(queryIDs, sub).add(queryID);
      });
    }
  }

  remove(sub: Subscription<QueryInvalidationUpdate>, req: WatchRequest) {
    for (const {hashes} of Object.values(req.queries)) {
      hashes.forEach(hash => {
        const subscriptions = this.#hashToSubscription.get(hash);
        if (subscriptions) {
          subscriptions.delete(sub);
          if (subscriptions.size === 0) {
            this.#hashToSubscription.delete(hash);
          }
        }
      });
    }
  }

  /**
   * Computes the invalidated query IDs for all Subscriptions based on
   * the specified invalidation `hashes`.
   */
  computeInvalidationUpdates(
    hashes: Set<string>,
  ): Map<Subscription<QueryInvalidationUpdate>, Set<string>> {
    const updates = new Map<
      Subscription<QueryInvalidationUpdate>,
      Set<string>
    >();

    for (const hash of hashes) {
      const subscriptionsToQueryIDs = this.#hashToSubscription.get(hash);
      if (subscriptionsToQueryIDs) {
        for (const [subscription, queryIDs] of subscriptionsToQueryIDs) {
          queryIDs.forEach(queryID => {
            ensureSet(updates, subscription).add(queryID);
          });
        }
      }
    }

    return updates;
  }

  /**
   * Computes the invalidated query IDs for the specified `subscription`
   * based on the specified invalidation `hashes`.
   */
  computeInvalidationUpdate(
    hashes: Set<string>,
    subscription: Subscription<QueryInvalidationUpdate>,
  ): Set<string> {
    const queryIDs = new Set<string>();

    for (const hash of hashes) {
      this.#hashToSubscription
        .get(hash)
        ?.get(subscription)
        ?.forEach(queryID => queryIDs.add(queryID));
    }
    return queryIDs;
  }
}

function ensureMap<K1, K2, V>(m: Map<K1, Map<K2, V>>, k: K1): Map<K2, V> {
  let map = m.get(k);
  if (map === undefined) {
    map = new Map<K2, V>();
    m.set(k, map);
  }
  return map;
}

export function ensureSet<K, V>(m: Map<K, Set<V>>, k: K): Set<V> {
  let s = m.get(k);
  if (s === undefined) {
    s = new Set<V>();
    m.set(k, s);
  }
  return s;
}
