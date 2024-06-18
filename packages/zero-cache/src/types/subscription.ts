import {resolver, type Resolver} from '@rocicorp/resolver';
import type {CancelableAsyncIterable} from './streams.js';

/**
 * A Subscription abstracts a continuous, logically infinite stream of messages intended
 * for serial processing. Unlike the more general Node `Stream` API, a Subscription has
 * a limited API with specific semantics:
 *
 * * **Serial processing**: Messages must be consumed via the {@link AsyncIterable}
 *   interface, e.g.
 *   ```ts
 *   const subscription = server.subscribe(parameters);
 *
 *   for await (const message of subscription) {
 *     await process(message);  // fully process the message before consuming the next
 *   }
 *   ```
 *
 *   Moreover, the consumer is expected to completely process each message before
 *   requesting the next. This is important for cleanup semantics (explained later).
 *
 * * **cancel()**, not close(): The underlying data in a subscription is logically infinite
 *   and only terminated when the consumer is no longer interested in receiving the messages
 *   (or requires a Subscription with a different configuration). As such, there is no API
 *   for gracefully closing the subscription after pending messages are consumed; rather,
 *   cancellation is immediate, and upon cancellation, pending messages are dropped. A
 *   Subscription can also be terminated with exceptional (i.e. `Error`) circumstances,
 *   for which the behavior is equivalent.
 *
 * * **Coalescing** (optional): A producer can configure pending messages in the Subscription
 *   to be merged together with a {@link Options.coalesce coalesce} function. This is useful
 *   for semantics in which the consumer is not necessarily interested in every incremental
 *   change, but rather the cumulative change since the last processed message. A
 *   Subscription with coalescing is guaranteed to have at most one outstanding message,
 *   regardless of how quickly messages are produced and consumed. This effectively constrains
 *   the amount of outstanding work in the system.
 *
 * ### Resource Tracking and Cleanup
 *
 * Because message consumption is constrained to the async iteration API, standard
 * control flow mechanisms allow the producer to perform bookkeeping without any
 * explicit cleanup actions on the part of the consumer. This includes:
 *
 * * **Per-message cleanup**: Each request for the {@link AsyncIterator.next next}
 *   message, or the termination of the iteration, signals that the consumer has
 *   finished processing the previous message. The producer of a Subscription can
 *   supply a {@link Options.consumed consumed} callback to receive these processed
 *   messages, allowing it to clean up attached resources (e.g. TransactionPools, etc.).
 *
 * * **Per-subscription cleanup**: The producer of a Subscription can supply a
 *   {@link Options.cleanup cleanup} callback that is invoked when the Subscription
 *   is terminated, either explicitly via {@link Subscription.cancel cancel()} /
 *   {@link Subscription.fail fail()}, or implicitly when an iteration is exited via a
 *  `break`, `return`, or `throw` statement. All unconsumed messages are passed to the
 *   call back to facilitate bookkeeping.
 *
 * @param T The external message type, published to the AsyncIterable
 * @param M The internal message type used in the producer-side interfaces
 *          (e.g. {@link push}, {@link Options.consumed}, {@link Options.coalesce},
 *          and {@link Options.cleanup}). This is often the same as the external type
 *          T, but may be diverged to facilitate internal bookkeeping.
 */
export class Subscription<T, M = T> implements CancelableAsyncIterable<T> {
  /**
   * Convenience factory method for creating a {@link Subscription} with internal message type
   * `M` as a subtype of `T`, defaulting to the same type. The default `publish` method publishes
   * the message of type `M` directly to the AsyncIterable.
   */
  static create<T, M extends T = T>(
    options: Options<M> = {},
    publish: (m: M) => T = m => m,
  ) {
    return new Subscription(options, publish);
  }

  // Consumers waiting to consume messages (i.e. an async iteration awaiting the next message).
  readonly #consumers: Resolver<IteratorResult<M>>[] = [];
  // Messages waiting to be consumed.
  readonly #messages: M[] = [];
  // Sentinel value signaling that the subscription is "done" and no more
  // messages can be added.
  #sentinel: 'canceled' | Error | undefined = undefined;

  #coalesce: ((curr: M, prev: M) => M) | undefined;
  #consumed: (prev: M) => void;
  #cleanup: (unconsumed: M[], err?: Error) => void;
  #publish: (internal: M) => T;

  /**
   * @param publish function for converting the internally pushed / coalesced message
   *        of type `M` to the external type `T` exposed via async iteration.
   */
  constructor(options: Options<M> = {}, publish: (m: M) => T) {
    const {coalesce, consumed = () => {}, cleanup = () => {}} = options;
    this.#coalesce = coalesce;
    this.#consumed = consumed;
    this.#cleanup = cleanup;
    this.#publish = publish;
  }

  /**
   * Pushes the next message to be consumed. If there is an existing unconsumed message
   * and the Subscription has a {@link Options.coalesce coalesce} function, the specified
   * `value` will be coalesced with the pending message.
   *
   * If the subscription is in a terminal state, the message is dropped and the method
   * call is a noop.
   */
  push(value: M) {
    if (this.#sentinel) {
      return;
    }
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolve({value});
    } else if (this.#coalesce && this.#messages.length) {
      this.#messages[0] = this.#coalesce(value, this.#messages[0]);
    } else {
      this.#messages.push(value);
    }
  }

  /** Cancels the subscription, cleans up, and terminates any iteration. */
  cancel() {
    this.#terminate('canceled');
  }

  /** Fails the subscription, cleans up, and throws from any iteration. */
  fail(err: Error) {
    this.#terminate(err);
  }

  #terminate(sentinel: 'canceled' | Error) {
    if (!this.#sentinel) {
      this.#sentinel = sentinel;
      this.#cleanup(
        [...this.#messages],
        sentinel instanceof Error ? sentinel : undefined,
      );
      this.#messages.splice(0);

      for (
        let consumer = this.#consumers.shift();
        consumer;
        consumer = this.#consumers.shift()
      ) {
        sentinel === 'canceled'
          ? consumer.resolve({value: undefined, done: true})
          : consumer.reject(sentinel);
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    let prev: M | undefined;

    const notifyPrevConsumed = () => {
      if (prev !== undefined) {
        this.#consumed(prev);
        prev = undefined;
      }
    };

    return {
      next: async () => {
        notifyPrevConsumed();

        const value = this.#messages.shift();
        if (value !== undefined) {
          prev = value;
          return {value: this.#publish(value)};
        }
        if (this.#sentinel === 'canceled') {
          return {value: undefined, done: true};
        }
        if (this.#sentinel) {
          return Promise.reject(this.#sentinel);
        }
        const consumer = resolver<IteratorResult<M>>();
        this.#consumers.push(consumer);

        // Wait for push() (or termination) to resolve the consumer.
        const result = await consumer.promise;
        prev = result.done ? undefined : result.value;
        return result.done ? result : {value: this.#publish(result.value)};
      },

      return: value => {
        notifyPrevConsumed();

        this.cancel();
        return Promise.resolve({value, done: true});
      },
    };
  }
}

type Options<M> = {
  /**
   * Coalesces messages waiting to be consumed. This is useful for "watermark" type
   * subscriptions in which the consumer is only interested in the cumulative state
   * change since the last processed message. When a `coalesce` function is specified,
   * there is guaranteed to be at most one message waiting to be consumed.
   *
   * Note that the `curr` argument comes before `prev`. This facilitates a common
   * scenario in which coalescing just means using the newest value; in such a case,
   * `coalesce` can simply be the identity function (e.g. `msg => msg`).
   */
  coalesce?: (curr: M, prev: M) => M;

  /**
   * Called on the previous message in an iteration (1) when the next message is requested,
   * or (2) when the iteration is terminated. This allows the producer to perform
   * per-message cleanup.
   *
   * Note that when a {@link Options.coalesce coalesce} function is defined,
   * `consumed` is _not_ called on the `prev` message; it is the responsibility of
   * producers requiring both coalescing and consumption notification to perform any
   * necessary cleanup of `prev` messages when coalescing.
   */
  consumed?: (prev: M) => void;

  /**
   * `cleanup` is called exactly once when the subscription is terminated via a failure or
   * cancelation (whichever happens first), which includes implicit cancelation when
   * the consumer exits an iteration via a `break`, `return`, or `throw` statement.
   *
   * Note that the `err` argument will only reflect an explicit cancelation via a call
   * to {@link Subscription.fail()}. On the other hand, if the iteration is canceled via
   * a `throw` statement, the thrown reason is not reflected in the `err` parameter, as that
   * information is not made available to the AsyncIterator implementation.
   */
  cleanup?: (unconsumed: M[], err?: Error) => void;
};
