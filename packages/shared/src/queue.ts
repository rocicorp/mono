import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from './asserts.ts';

/**
 * A Queue allows the consumers to await (possibly future) values,
 * and producers to await the consumption of their values.
 */
export class Queue<T> {
  // Consumers waiting for entries to be produced.
  readonly #consumers: Consumer<T>[] = [];
  // Produced entries waiting to be consumed.
  readonly #produced: Produced<T>[] = [];

  enqueue(value: T): void {
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolver.resolve(value);
      clearTimeout(consumer.timeoutID);
      return;
    }
    this.#produced.push({value});
  }

  enqueueRejection(reason?: unknown): void {
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolver.reject(reason);
      clearTimeout(consumer.timeoutID);
      return;
    }
    this.#produced.push({rejection: reason});
  }

  /**
   * Deletes all unconsumed entries matching the specified `value` based on identity equality.
   * The consumed callback(s) are resolved as if the values were dequeued.
   *
   * Note: deletion of `undefined` values is not supported. This method will assert
   * if `value` is undefined.
   *
   * @returns The number of entries deleted.
   */
  delete(value: T): number {
    assert(value !== undefined);

    let count = 0;
    for (let i = this.#produced.length - 1; i >= 0; i--) {
      const p = this.#produced[i];
      if (p.value === value) {
        this.#produced.splice(i, 1);
        count++;
      }
    }
    return count;
  }

  /**
   * @param timeoutValue An optional value to resolve if `timeoutMs` is reached.
   * @param timeoutMs The milliseconds after which the `timeoutValue` is resolved
   *                  if nothing is produced for the consumer.
   * @returns A Promise that resolves to the next enqueued value.
   */
  dequeue(timeoutValue?: T, timeoutMs: number = 0): Promise<T> | T {
    const produced = this.#produced.shift();
    if (produced) {
      return produced.value ?? Promise.reject(produced.rejection);
    }
    const r = resolver<T>();
    const timeoutID =
      timeoutValue === undefined
        ? undefined
        : setTimeout(() => {
            const i = this.#consumers.findIndex(c => c.resolver === r);
            if (i >= 0) {
              const [consumer] = this.#consumers.splice(i, 1);
              consumer.resolver.resolve(timeoutValue);
            }
          }, timeoutMs);
    this.#consumers.push({resolver: r, timeoutID});
    return r.promise;
  }

  /**
   * Drains the entire queue.
   *
   * Usage example:
   * ```ts
   * // A consumer that, when awoken, drains
   * // all entries in the queue in order to
   * // process them in a batch.
   * for (;;) {
   *   const value = await queue.dequeue();
   *   const rest = queue.drain();
   * }
   * ```
   */
  drain(): (T | undefined)[] {
    const ret: (T | undefined)[] = [];
    for (const p of this.#produced) {
      ret.push(p.value);
    }
    this.#produced.length = 0;

    return ret;
  }

  /**
   * @returns The instantaneous number of outstanding values waiting to be
   *          dequeued. Note that if a value was enqueued while a consumer
   *          was waiting (with `await dequeue()`), the value is immediately
   *          handed to the consumer and the Queue's size remains 0.
   */
  size(): number {
    return this.#produced.length;
  }

  asAsyncIterable(cleanup = NOOP): AsyncIterable<T> {
    return {[Symbol.asyncIterator]: () => this.asAsyncIterator(cleanup)};
  }

  asAsyncIterator(cleanup = NOOP): AsyncIterator<T> {
    return {
      next: async () => {
        try {
          const value = await this.dequeue();
          return {value};
        } catch (e) {
          cleanup();
          throw e;
        }
      },
      return: value => {
        cleanup();
        return Promise.resolve({value, done: true});
      },
    };
  }
}

const NOOP = () => {};

type Consumer<T> = {
  resolver: Resolver<T>;
  timeoutID: ReturnType<typeof setTimeout> | undefined;
};

type Produced<T> =
  | {value: T; rejection?: undefined}
  | {value?: undefined; rejection: unknown};
