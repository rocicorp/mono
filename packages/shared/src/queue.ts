import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from './asserts.ts';

/**
 * A Queue allows the consumers to await (possibly future) values,
 * and producers to await the consumption of their values.
 */
export class Queue<T> {
  // Consumers waiting for entries to be produced.
  readonly #consumers: (Consumer<T> | undefined)[] = [];
  #consumerHead = 0;
  #consumerCount = 0;
  // Produced entries waiting to be consumed.
  readonly #produced: (Produced<T> | undefined)[] = [];
  #producedHead = 0;
  #producedCount = 0;

  enqueue(value: T): void {
    const consumer = this.#dequeueConsumer();
    if (consumer) {
      consumer.resolver.resolve(value);
      clearTimeout(consumer.timeoutID);
      return;
    }
    this.#produced.push({value});
    this.#producedCount++;
  }

  enqueueRejection(reason?: unknown): void {
    const consumer = this.#dequeueConsumer();
    if (consumer) {
      consumer.resolver.reject(reason);
      clearTimeout(consumer.timeoutID);
      return;
    }
    this.#produced.push({rejection: reason});
    this.#producedCount++;
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
    assert(value !== undefined, 'Queue delete value must not be undefined');

    let count = 0;
    for (let i = this.#produced.length - 1; i >= this.#producedHead; i--) {
      const p = this.#produced[i];
      if (p?.value === value) {
        this.#produced[i] = undefined;
        this.#producedCount--;
        count++;
      }
    }
    this.#maybeCompactProduced();
    return count;
  }

  /**
   * @param timeoutValue An optional value to resolve if `timeoutMs` is reached.
   * @param timeoutMs The milliseconds after which the `timeoutValue` is resolved
   *                  if nothing is produced for the consumer.
   * @returns A Promise that resolves to the next enqueued value.
   */
  dequeue(timeoutValue?: T, timeoutMs: number = 0): Promise<T> | T {
    const produced = this.#dequeueProduced();
    if (produced) {
      return produced.value ?? Promise.reject(produced.rejection);
    }
    const r = resolver<T>();
    const consumer: Consumer<T> = {resolver: r, timeoutID: undefined};
    consumer.timeoutID =
      timeoutValue === undefined
        ? undefined
        : setTimeout(() => {
            const i = this.#consumers.indexOf(consumer, this.#consumerHead);
            if (i >= 0) {
              this.#consumers[i] = undefined;
              this.#consumerCount--;
              consumer.resolver.resolve(timeoutValue);
              this.#maybeCompactConsumers();
            }
          }, timeoutMs);
    this.#consumers.push(consumer);
    this.#consumerCount++;
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
    for (let i = this.#producedHead; i < this.#produced.length; i++) {
      const p = this.#produced[i];
      if (p) {
        ret.push(p.value);
      }
    }
    this.#resetProduced();

    return ret;
  }

  /**
   * @returns The instantaneous number of outstanding values waiting to be
   *          dequeued. Note that if a value was enqueued while a consumer
   *          was waiting (with `await dequeue()`), the value is immediately
   *          handed to the consumer and the Queue's size remains 0.
   */
  size(): number {
    return this.#producedCount;
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

  #dequeueConsumer(): Consumer<T> | undefined {
    if (this.#consumerCount === 0) {
      this.#resetConsumers();
      return undefined;
    }

    while (this.#consumerHead < this.#consumers.length) {
      const consumer = this.#consumers[this.#consumerHead];
      this.#consumers[this.#consumerHead] = undefined;
      this.#consumerHead++;
      if (consumer) {
        this.#consumerCount--;
        this.#maybeCompactConsumers();
        return consumer;
      }
    }

    this.#consumerCount = 0;
    this.#resetConsumers();
    return undefined;
  }

  #dequeueProduced(): Produced<T> | undefined {
    if (this.#producedCount === 0) {
      this.#resetProduced();
      return undefined;
    }

    while (this.#producedHead < this.#produced.length) {
      const produced = this.#produced[this.#producedHead];
      this.#produced[this.#producedHead] = undefined;
      this.#producedHead++;
      if (produced) {
        this.#producedCount--;
        this.#maybeCompactProduced();
        return produced;
      }
    }

    this.#producedCount = 0;
    this.#resetProduced();
    return undefined;
  }

  #maybeCompactConsumers(): void {
    if (this.#consumerCount === 0) {
      this.#resetConsumers();
    } else if (
      this.#consumerHead > 1024 &&
      this.#consumerHead * 2 > this.#consumers.length
    ) {
      this.#consumers.splice(0, this.#consumerHead);
      this.#consumerHead = 0;
    }
  }

  #maybeCompactProduced(): void {
    if (this.#producedCount === 0) {
      this.#resetProduced();
    } else if (
      this.#producedHead > 1024 &&
      this.#producedHead * 2 > this.#produced.length
    ) {
      this.#produced.splice(0, this.#producedHead);
      this.#producedHead = 0;
    }
  }

  #resetConsumers(): void {
    this.#consumers.length = 0;
    this.#consumerHead = 0;
    this.#consumerCount = 0;
  }

  #resetProduced(): void {
    this.#produced.length = 0;
    this.#producedHead = 0;
    this.#producedCount = 0;
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
