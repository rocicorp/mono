import type {LogContext} from '@rocicorp/logger';

/**
 * A valid TimeoutID will always be a positive integer.
 * `0` can be used as a "null" value.
 */
export type TimeoutID = number;

/**
 * An AlarmScheduler is a (mostly) drop-in replacement for `setTimeout()` that schedules
 * callbacks to be run in a DurableObject `alarm()` handler.
 *
 * Running callbacks in Alarm invocations makes the produced Tail Items (logs, errors,
 * diagnostic channels) available to Tail Workers in an `AlarmEvent` when the
 * invocation completes. This is critical for timely processing of asynchronous
 * events in Tail Workers. (Tail Items produced in the context of a fetch request, on the
 * other hand, are not surfaced until the fetch completes, which can be arbitrarily long
 * for websocket connections).
 *
 * To ensure timely publishing of Tail Items, the `AlarmScheduler` should be used to
 * schedule all timeout-based callbacks in the code producing the Tail Items, as timeouts
 * created by the standard `setTimeout()` or `setInterval()` will delay the completion of
 * the AlarmEvent.
 */
export interface AlarmScheduler {
  /**
   * Analog of Javascript's `setTimeout()` with the following differences:
   * - The first argument is always the LogContext, passed in from the `alarm()` invocation.
   * - The callback can return a Promise which the `alarm()` invocation will await
   *   (recommended).
   *
   * Prefer `promiseTimeout()` when the caller is able to await the setting
   * of the Durable Object Alarm.
   */
  setTimeout<Args extends unknown[]>(
    callback: (lc: LogContext, ...args: Args) => void | Promise<void>,
    msDelay?: number | undefined,
    ...args: Args
  ): TimeoutID;

  /**
   * Promise-returning equivalent of `setTimeout()` that allows the caller to
   * wait for the DurableStorage alarm to be updated (if necessary).
   */
  promiseTimeout<Args extends unknown[]>(
    callback: (lc: LogContext, ...args: Args) => void | Promise<void>,
    msDelay?: number | undefined,
    ...args: Args
  ): Promise<TimeoutID>;

  /**
   * Equivalent to Javascript's `clearTimeout()` with the exception that the
   * caller can wait on the returned promise to ensure that the Durable Object
   * Alarm has been updated if necessary.
   */
  clearTimeout(timeoutID: TimeoutID | null): Promise<void>;
}

type Timeout = {
  readonly fireTime: number;
  readonly fire: (lc: LogContext) => void | Promise<void>;
};

export class AlarmManager {
  readonly #storage: DurableObjectStorage;
  readonly #timeouts: Map<TimeoutID, Timeout> = new Map();

  // The AlarmScheduler component is constrained to its own interface (and
  // object) to make it obvious to developers that components that only need
  // scheduling logic should never need or have access to the AlarmManager.
  readonly scheduler: AlarmScheduler;

  // To keep setTimeout() and clearTimeout() non-blocking, alarm scheduling is
  // done asynchronously but serialized on this `#nextAlarm` Promise. Changes
  // to the next alarm should always reset the variable with a Promise
  // that makes modifications based on the value of the previous Promise.
  #nextAlarm: Promise<number | null>;
  #nextID: TimeoutID = 1;

  constructor(storage: DurableObjectStorage) {
    this.#storage = storage;
    this.#nextAlarm = storage.getAlarm();

    this.scheduler = {
      setTimeout: <Args extends unknown[]>(
        callback: (lc: LogContext, ...args: Args) => void | Promise<void>,
        msDelay?: number | undefined,
        ...args: Args
      ): TimeoutID =>
        this.#promiseTimeout(callback, msDelay, ...args).timeoutID,

      promiseTimeout: <Args extends unknown[]>(
        callback: (lc: LogContext, ...args: Args) => void | Promise<void>,
        msDelay?: number | undefined,
        ...args: Args
      ): Promise<TimeoutID> =>
        this.#promiseTimeout(callback, msDelay, ...args).promise,

      clearTimeout: (timeoutID: TimeoutID | null) =>
        this.#clearTimeout(timeoutID),
    };
  }

  #promiseTimeout<Args extends unknown[]>(
    cb: (lc: LogContext, ...args: Args) => void | Promise<void>,
    msDelay: number = 0,
    ...args: Args
  ): {promise: Promise<TimeoutID>; timeoutID: TimeoutID} {
    const fireTime = Date.now() + msDelay;
    const timeoutID = this.#nextID++;
    this.#timeouts.set(timeoutID, {fireTime, fire: lc => cb(lc, ...args)});
    return {promise: this.#schedule().then(() => timeoutID), timeoutID};
  }

  async #clearTimeout(timeoutID: TimeoutID | null): Promise<void> {
    if (timeoutID && this.#timeouts.delete(timeoutID)) {
      await this.#schedule();
    }
  }

  #schedule(): Promise<number | null> {
    if (this.#timeouts.size === 0) {
      return (this.#nextAlarm = this.#nextAlarm.then(next =>
        next === null
          ? null // No Alarm to delete
          : this.#storage.deleteAlarm().then(() => null),
      ));
    }
    const now = Date.now();
    const fireTimes = [...this.#timeouts.values()].map(val => val.fireTime);
    const earliestFireTime = Math.min(...fireTimes);
    const nextFireTime = Math.max(now, earliestFireTime);

    return (this.#nextAlarm = this.#nextAlarm.then(fireTime =>
      fireTime === nextFireTime
        ? nextFireTime // Already set (common case).
        : this.#storage.setAlarm(nextFireTime).then(() => nextFireTime),
    ));
  }

  async fireScheduled(lc: LogContext): Promise<void> {
    // When the DO Alarm is fired, refresh the value from storage. It should
    // generally be null, but it's possible for a timeout to have been
    // asynchronously scheduled.
    this.#nextAlarm = this.#nextAlarm.then(() => this.#storage.getAlarm());

    const now = Date.now();
    const timeouts = [...this.#timeouts].filter(
      ([_, val]) => val.fireTime <= now,
    );

    // Remove the timeouts to fire from the Map.
    timeouts.forEach(([timeoutID]) => this.#timeouts.delete(timeoutID));
    lc.debug?.(`Firing ${timeouts.length} timeout(s)`);

    // Errors or rejections from timeouts should not be bubbled up, as that would put
    // the Durable Object Alarm framework into exponential-backoff-retry mode.
    // Instead we follow a behavior closer to that of setTimeout() by catching and
    // logging errors / rejections, and proceeding with remaining timeouts as scheduled.
    const results = await Promise.allSettled(
      timeouts.map(([_, timeout]) => {
        try {
          return timeout.fire(lc);
        } catch (e) {
          return Promise.reject(e);
        }
      }),
    );
    results.forEach(result => {
      if (result.status === 'rejected') {
        lc.error?.(result.reason);
      }
    });

    const next = await this.#schedule();
    if (next) {
      lc.debug?.(`Next alarm fires in ${next - Date.now()} ms`);
    } else {
      lc.debug?.(`No more timeouts scheduled`);
    }
  }

  // For testing / debugging.
  nextAlarmTime(): Promise<number | null> {
    return this.#nextAlarm;
  }
}
