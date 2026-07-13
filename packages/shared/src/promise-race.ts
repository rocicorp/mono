import {resolver} from '@rocicorp/resolver';

type PromiseRaceResult<T extends Record<string, PromiseLike<unknown>>> = {
  [K in Extract<keyof T, string>]: {
    key: K;
    status: 'fulfilled';
    result: Awaited<T[K]>;
  };
}[Extract<keyof T, string>];

const NO_PROMISES_MESSAGE = 'No promises to race';

type Settlement =
  | {status: 'fulfilled'; result: unknown}
  | {status: 'rejected'; reason: unknown};

type Waiter = (settlement: Settlement) => void;

/**
 * Observes the settlement of a single promise using one `then` reaction and
 * fans it out to the `promiseRace` calls currently waiting on it. Reactions
 * attached to a pending promise can never be detached, so attaching one per
 * race would grow without bound when a long-lived pending promise keeps
 * losing races. Waiters, on the other hand, are removed as soon as their race
 * settles.
 */
class Subscription {
  settlement: Settlement | undefined;
  readonly waiters = new Set<Waiter>();

  constructor(promise: PromiseLike<unknown>) {
    Promise.resolve(promise).then(
      result => this.#settle({status: 'fulfilled', result}),
      reason => this.#settle({status: 'rejected', reason}),
    );
  }

  #settle(settlement: Settlement): void {
    this.settlement = settlement;
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) {
      waiter(settlement);
    }
  }
}

const subscriptions = new WeakMap<PromiseLike<unknown>, Subscription>();

function getSubscription(promise: PromiseLike<unknown>): Subscription {
  let subscription = subscriptions.get(promise);
  if (subscription === undefined) {
    subscription = new Subscription(promise);
    subscriptions.set(promise, subscription);
  }
  return subscription;
}

/**
 * The number of unsettled `promiseRace` calls currently waiting on `promise`.
 * Exported only so tests can assert that losing races detach from long-lived
 * pending promises.
 */
export function raceWaiterCountForTesting(
  promise: PromiseLike<unknown>,
): number {
  return subscriptions.get(promise)?.waiters.size ?? 0;
}

function toRaceResult<T extends Record<string, PromiseLike<unknown>>>(
  key: Extract<keyof T, string>,
  settlement: Settlement & {status: 'fulfilled'},
): PromiseRaceResult<T> & {} {
  return {
    key,
    status: 'fulfilled',
    result: settlement.result,
  } as PromiseRaceResult<T> & {};
}

/**
 * Race a record of promises and resolve with the first resolved entry.
 *
 * Unlike a plain `Promise.race`, this attaches at most one reaction to each
 * promise ever (shared across calls via a cache), and a settled race detaches
 * from the losing entries. It is therefore safe to repeatedly race the same
 * long-lived pending promise (e.g. in a loop) without leaking a reaction per
 * call.
 *
 * @param promises Record of promises to race.
 * @returns Promise resolving to a discriminated union of key/result pairs.
 * @throws An error if the record is empty or if a promise is rejected.
 */
export async function promiseRace<
  T extends Record<string, PromiseLike<unknown>>,
>(promises: T): Promise<PromiseRaceResult<T> & {}> {
  const keys = Object.keys(promises) as Array<Extract<keyof T, string>>;

  if (keys.length === 0) {
    throw new Error(NO_PROMISES_MESSAGE);
  }

  // Subscribe to every entry up front so each promise gets its single
  // rejection handler, mirroring how Promise.race suppresses unhandled
  // rejections for all entries.
  const subs = keys.map(key => getSubscription(promises[key]));

  // If an entry has already settled, the first such entry in key order wins,
  // mirroring Promise.race, and no waiters need to be registered.
  for (let i = 0; i < keys.length; i++) {
    const {settlement} = subs[i];
    if (settlement !== undefined) {
      if (settlement.status === 'rejected') {
        throw settlement.reason;
      }
      return toRaceResult(keys[i], settlement);
    }
  }

  const raceResolver = resolver<PromiseRaceResult<T> & {}>();
  const registrations: [Subscription, Waiter][] = [];
  let settled = false;

  const settleRace = (
    key: Extract<keyof T, string>,
    settlement: Settlement,
  ): void => {
    if (settled) {
      return;
    }
    settled = true;
    // Detach from the losing promises so they do not retain this race.
    for (const [subscription, waiter] of registrations) {
      subscription.waiters.delete(waiter);
    }
    registrations.length = 0;
    if (settlement.status === 'rejected') {
      raceResolver.reject(settlement.reason);
    } else {
      raceResolver.resolve(toRaceResult(key, settlement));
    }
  };

  keys.forEach((key, i) => {
    const subscription = subs[i];
    const waiter: Waiter = settlement => settleRace(key, settlement);
    subscription.waiters.add(waiter);
    registrations.push([subscription, waiter]);
  });

  return await raceResolver.promise;
}
