import type {LogContext} from '@rocicorp/logger';
import type {Patch} from 'reflect-protocol';
import type {ClientID} from 'replicache';
import {Lock} from '@rocicorp/lock';

export type SubscribeToPresenceCallback = (
  presentClientIDs: ReadonlySet<ClientID>,
) => void;

type PresenceSubscription = {
  readonly callback: SubscribeToPresenceCallback;
};

export class PresenceManager {
  readonly #clientIDPromise: Promise<ClientID>;
  readonly #lcPromise: Promise<LogContext>;
  readonly #subscriptions = new Set<PresenceSubscription>();
  readonly #pendingInitial = new Set<PresenceSubscription>();
  readonly #lock = new Lock();
  #initialRunsScheduled = false;
  #presentClientIDsInitialized = false;
  #presentClientIDs: Set<ClientID> = new Set();

  constructor(
    clientIDPromise: Promise<ClientID>,
    lcPromise: Promise<LogContext>,
  ) {
    this.#clientIDPromise = clientIDPromise;
    this.#lcPromise = lcPromise;
    void this.#updateToOnlySelfPresent().catch(async e => {
      (await lcPromise).error?.(
        'Unexpected error initializing presence manager',
        e,
      );
    });
  }

  updatePresence(patch: Patch) {
    return this.#lock.withLock(async () => {
      const clientID = await this.#clientIDPromise;
      const lc = await this.#lcPromise;
      if (patch.length === 0) {
        return;
      }
      this.#presentClientIDsInitialized = true;
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
      updated.add(clientID);
      if (!setEqual(prior, updated)) {
        this.#presentClientIDs = updated;
        for (const sub of this.#subscriptions) {
          callSubscriptionCallback(sub, updated, lc);
          this.#pendingInitial.delete(sub);
        }
      }
    });
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

  #scheduleInitialSubscriptionRun(subscription: PresenceSubscription) {
    if (!this.#presentClientIDsInitialized) {
      return;
    }
    this.#pendingInitial.add(subscription);
    if (!this.#initialRunsScheduled) {
      this.#initialRunsScheduled = true;
      // Ensure initial run is always async
      // blog.ometer.com/2011/07/24/callbacks-synchronous-and-asynchronous/
      queueMicrotask(async () => {
        const lc = await this.#lcPromise;
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

  handleDisconnect(): Promise<void> {
    return this.#updateToOnlySelfPresent();
  }

  #updateToOnlySelfPresent(): Promise<void> {
    return this.updatePresence([{op: 'clear'}]);
  }
}

function callSubscriptionCallback(
  sub: PresenceSubscription,
  presentClientIDs: ReadonlySet<ClientID>,
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

function setEqual(a: ReadonlySet<unknown>, b: ReadonlySet<unknown>): boolean {
  if (a.size !== b.size) {
    return false;
  }
  for (const el of a) {
    if (!b.has(el)) {
      return false;
    }
  }
  return true;
}
