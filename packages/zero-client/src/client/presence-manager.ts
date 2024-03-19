import type {LogContext} from '@rocicorp/logger';
import type {Patch} from 'reflect-protocol';
import type {ClientID} from 'replicache';

export type SubscribeToPresenceCallback = (
  presentClientIDs: ReadonlyArray<ClientID>,
) => void;

type PresenceSubscription = {
  readonly callback: SubscribeToPresenceCallback;
};

export class PresenceManager {
  readonly #clientID: ClientID;
  readonly #lc: LogContext;
  readonly #subscriptions = new Set<PresenceSubscription>();
  readonly #pendingInitial = new Set<PresenceSubscription>();
  #initialRunsScheduled = false;
  #presentClientIDs: ClientID[];

  constructor(clientID: ClientID, lc: LogContext) {
    this.#clientID = clientID;
    this.#presentClientIDs = [clientID];
    this.#lc = lc;
  }

  updatePresence(patch: Patch) {
    if (patch.length === 0) {
      return;
    }
    const prior = this.#presentClientIDs;
    const updated = new Set(this.#presentClientIDs);
    for (const op of patch) {
      switch (op.op) {
        case 'clear':
          updated.clear();
          break;
        case 'put':
          updated.add(op.key);
          break;
        case 'del':
          updated.delete(op.key);
          break;
      }
    }
    updated.add(this.#clientID);
    if (!setEqual(prior, updated)) {
      this.#presentClientIDs = Array.from(updated);
      for (const sub of this.#subscriptions) {
        callSubscriptionCallback(sub, this.#presentClientIDs, this.#lc);
        this.#pendingInitial.delete(sub);
      }
    }
  }

  addSubscription(callback: SubscribeToPresenceCallback): () => void {
    const subscription = {
      callback,
    };
    this.#subscriptions.add(subscription);
    this.#scheduleInitialSubscriptionRun(subscription);
    return () => {
      this.#subscriptions.delete(subscription);
    };
  }

  #scheduleInitialSubscriptionRun(subscription: PresenceSubscription): void {
    this.#pendingInitial.add(subscription);
    if (!this.#initialRunsScheduled) {
      this.#initialRunsScheduled = true;
      // Ensure initial run is always async
      // blog.ometer.com/2011/07/24/callbacks-synchronous-and-asynchronous/
      queueMicrotask(() => {
        const lc = this.#lc;
        this.#initialRunsScheduled = false;
        for (const sub of this.#pendingInitial) {
          if (this.#subscriptions.has(sub)) {
            callSubscriptionCallback(sub, this.#presentClientIDs, lc);
            this.#pendingInitial.delete(sub);
          }
        }
      });
    }
  }

  clearSubscriptions(): void {
    this.#subscriptions.clear();
  }

  handleDisconnect(): void {
    this.#updateToOnlySelfPresent();
  }

  #updateToOnlySelfPresent(): void {
    this.updatePresence([{op: 'clear'}]);
  }
}

function callSubscriptionCallback(
  sub: PresenceSubscription,
  presentClientIDs: ReadonlyArray<ClientID>,
  lc: LogContext,
) {
  try {
    sub.callback(presentClientIDs);
  } catch (e) {
    // Log then rethrow the error in a separate microtask in order to surface
    // it to customer error reporting without disrupting calling back
    // other presence subscriptions.
    lc.error?.('Error in presence subscription callback:', e);
    // eslint-disable-next-line require-await
    queueMicrotask(() => {
      throw new Error('Error in presence subscription callback.', {cause: e});
    });
  }
}

function setEqual(a: ReadonlyArray<unknown>, b: ReadonlySet<unknown>): boolean {
  if (a.length !== b.size) {
    return false;
  }
  for (const el of a) {
    if (!b.has(el)) {
      return false;
    }
  }
  return true;
}
