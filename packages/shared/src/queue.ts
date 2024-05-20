import {resolver, type Resolver} from '@rocicorp/resolver';

/**
 * A Queue allows the consumers to await (possibly future) values,
 * and producers to await the consumption of their values.
 */
export class Queue<T> {
  // Consumers waiting for entries to be produced.
  readonly #consumers: Consumer<T>[] = [];
  // Produced entries waiting to be consumed.
  readonly #produced: {value: Promise<T>; consumed: () => void}[] = [];

  /** @returns A Promise that resolves when the value is consumed. */
  enqueue(value: T): Promise<void> {
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolver.resolve(value);
      clearTimeout(consumer.timeoutID);
      return Promise.resolve();
    }
    return this.#enqueueProduced(Promise.resolve(value));
  }

  /** @returns A Promise that resolves when the rejection is consumed. */
  enqueueRejection(reason?: unknown): Promise<void> {
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolver.reject(reason);
      clearTimeout(consumer.timeoutID);
      return Promise.resolve();
    }
    return this.#enqueueProduced(Promise.reject(reason));
  }

  #enqueueProduced(value: Promise<T>): Promise<void> {
    const {promise, resolve: consumed} = resolver<void>();
    this.#produced.push({value, consumed});
    return promise;
  }

  /**
   * @param timeoutValue An optional value to resolve if `timeoutMs` is reached.
   * @param timeoutMs The milliseconds after which the `timeoutValue` is resolved
   *                  if nothing is produced for the consumer.
   * @returns A Promise that resolves to the next enqueued value.
   */
  dequeue(timeoutValue?: T, timeoutMs: number = 0): Promise<T> {
    const produced = this.#produced.shift();
    if (produced) {
      produced.consumed();
      return produced.value;
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
