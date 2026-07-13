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
  | {status: 'fulfilled'; value: unknown}
  | {status: 'rejected'; reason: unknown};

type Subscriber = (settlement: Settlement) => void;

type Observer = {
  settlement: Settlement | undefined;
  readonly subscribers: Set<Subscriber>;
};

// A reaction attached to a pending promise is only released when the promise
// settles, so racing a long-lived pending promise must not call `.then` on it
// per race. Each promise instead gets one cached observer holding the only
// reaction we ever attach to it; races subscribe to the observer and
// unsubscribe when they settle.
const observers = new WeakMap<PromiseLike<unknown>, Observer>();

const observe = (promise: PromiseLike<unknown>): Observer => {
  let observer = observers.get(promise);
  if (observer === undefined) {
    const created: Observer = {
      settlement: undefined,
      subscribers: new Set(),
    };
    const settle = (settlement: Settlement) => {
      created.settlement = settlement;
      const current = [...created.subscribers];
      created.subscribers.clear();
      for (const subscriber of current) {
        subscriber(settlement);
      }
    };
    // Promise.resolve normalizes non-native thenables.
    Promise.resolve(promise).then(
      value => settle({status: 'fulfilled', value}),
      reason => settle({status: 'rejected', reason}),
    );
    observers.set(promise, created);
    observer = created;
  }
  return observer;
};

/**
 * Race a record of promises and resolve with the first resolved entry.
 *
 * Unlike `Promise.race`, racing the same promise instance repeatedly (e.g. a
 * memoized pending promise raced by a run loop on every server message) does
 * not accumulate reactions on it: each promise gets at most one reaction for
 * its lifetime, and a race that settles unsubscribes itself from the losers.
 *
 * @param promises Record of promises to race.
 * @returns Promise resolving to a discriminated union of key/result pairs.
 * @throws An error if the record is empty or if a promise is rejected.
 */
export function promiseRace<T extends Record<string, PromiseLike<unknown>>>(
  promises: T,
): Promise<PromiseRaceResult<T> & {}> {
  const keys = Object.keys(promises) as Array<Extract<keyof T, string>>;

  if (keys.length === 0) {
    return Promise.reject(new Error(NO_PROMISES_MESSAGE));
  }

  const {promise, resolve, reject} = resolver<PromiseRaceResult<T> & {}>();
  const subscriptions: Array<[Observer, Subscriber]> = [];
  let settled = false;

  const settleWith = (
    key: Extract<keyof T, string>,
    settlement: Settlement,
  ) => {
    if (settled) {
      return;
    }
    settled = true;
    for (const [observer, subscriber] of subscriptions) {
      observer.subscribers.delete(subscriber);
    }
    subscriptions.length = 0;
    if (settlement.status === 'fulfilled') {
      resolve({
        key,
        status: 'fulfilled',
        result: settlement.value,
      } as PromiseRaceResult<T> & {});
    } else {
      reject(settlement.reason);
    }
  };

  for (const key of keys) {
    const observer = observe(promises[key]);
    if (observer.settlement !== undefined) {
      settleWith(key, observer.settlement);
      break;
    }
    const subscriber: Subscriber = s => settleWith(key, s);
    observer.subscribers.add(subscriber);
    subscriptions.push([observer, subscriber]);
  }

  return promise;
}

/**
 * Number of races currently subscribed to `promise`. A race that settles
 * unsubscribes from its losers, so this returns to 0 between races.
 */
export function getSubscriberCountForTesting(
  promise: PromiseLike<unknown>,
): number {
  return observers.get(promise)?.subscribers.size ?? 0;
}
