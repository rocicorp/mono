import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../shared/src/asserts.ts';
import type {Sink, Source} from './streams.ts';

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
export class Subscription<T, M = T> implements Source<T>, Sink<M> {
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
  readonly #consumers: Resolver<Entry<M> | null>[] = [];
  // Messages waiting to be consumed.
  readonly #messages: (Entry<M> | 'terminus')[] = [];
  readonly #pipelineEnabled: boolean;
  // Sentinel value signaling that the subscription is "done" and no more
  // messages can be added.
  #sentinel: 'canceled' | Error | undefined = undefined;

  #coalesce: ((curr: Entry<M>, prev: Entry<M>) => M) | undefined;
  #consumed: (prev: Entry<M>) => void;
  #cleanup: (unconsumed: Entry<M>[], err?: Error) => void;
  #publish: (internal: M) => T;

  /**
   * @param publish function for converting the internally pushed / coalesced message
   *        of type `M` to the external type `T` exposed via async iteration.
   */
  constructor(options: Options<M> = {}, publish: (m: M) => T) {
    const {
      coalesce,
      consumed = () => {},
      cleanup = () => {},
      pipeline = coalesce === undefined,
    } = options;

    this.#coalesce = !coalesce
      ? undefined
      : (curr, prev) => {
          try {
            return coalesce(curr.value, prev.value);
          } finally {
            prev.resolve('coalesced');
          }
        };

    this.#consumed = entry => {
      consumed(entry.value);
      entry.resolve('consumed');
    };

    this.#cleanup = (entries, err) => {
      cleanup(
        entries.map(e => e.value),
        err,
      );
      entries.forEach(e => e.resolve('unconsumed'));
    };

    this.#publish = publish;

    this.#pipelineEnabled = pipeline;
  }

  /**
   * Pushes the next message to be consumed, and returns a `result` that resolves to the
   * eventual {@link Result} of the `value`.
   *
   * If there is an existing unconsumed message and the Subscription has a
   * {@link Options#coalesce coalesce} function, the specified `value` will be coalesced
   * with the pending message. In this case, the result of the pending message
   * is resolved to `coalesced`, regardless of the `coalesce` function implementation.
   *
   * If the subscription is in a terminal state, the message is dropped and the
   * result resolves to `unconsumed`.
   */
  push(value: M): PendingResult {
    const {promise: result, resolve} = resolver<Result>();
    const entry = {value, resolve};

    if (this.#sentinel) {
      entry.resolve('unconsumed');
      return {result};
    }
    const consumer = this.#consumers.shift();
    if (consumer) {
      consumer.resolve(entry);
    } else if (
      this.#coalesce &&
      this.#messages.length &&
      this.#messages[this.#messages.length - 1] !== 'terminus'
    ) {
      const prev = this.#messages[this.#messages.length - 1];
      assert(prev !== 'terminus', 'prev should not be terminus after check');
      this.#messages[this.#messages.length - 1] = {
        value: this.#coalesce(entry, prev),
        resolve,
      };
    } else {
      this.#messages.push(entry);
    }
    return {result};
  }

  /** False if the subscription has been canceled or has failed. */
  get active(): boolean {
    return this.#sentinel === undefined;
  }

  /** The number messages waiting to be consumed. */
  get queued(): number {
    return this.#messages.length;
  }

  /**
   * Cancels the subscription after any queued messages are consumed. This is
   * meant for the producer-side code.
   *
   * Any messages pushed after calling `end()` will be unconsumed as if
   * `cancel()` were called (once the first set of pending messages is
   * consumed). In particular, if a coalesce function is defined, the new
   * messages will not be coalesced with the messages enqueued before `end()`
   * was called. However, to effect the intent of memory efficiency, multiple
   * messages pushed after calling `end()` will be coalesced together.
   *
   */
  end() {
    if (this.#sentinel) {
      // already terminated
    } else if (this.#messages.length === 0) {
      this.cancel();
    } else {
      this.#messages.push('terminus');
    }
  }

  /**
   * Cancels the subscription immediately, cleans up, and terminates any iteration.
   */
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
        this.#messages.filter(m => m !== 'terminus'),
        sentinel instanceof Error ? sentinel : undefined,
      );
      this.#messages.splice(0);

      for (
        let consumer = this.#consumers.shift();
        consumer;
        consumer = this.#consumers.shift()
      ) {
        sentinel === 'canceled'
          ? consumer.resolve(null)
          : consumer.reject(sentinel);
      }
    }
  }

  get pipeline(): AsyncIterable<{value: T; consumed: () => void}> | undefined {
    return this.#pipelineEnabled
      ? {[Symbol.asyncIterator]: () => this.#pipeline()}
      : undefined;
  }

  #pipeline(): AsyncIterator<{value: T; consumed: () => void}> {
    return {
      next: async () => {
        const entry = this.#messages.shift();
        if (entry === 'terminus') {
          this.cancel();
          return {value: undefined, done: true};
        }
        if (entry !== undefined) {
          return {
            value: {
              value: this.#publish(entry.value),
              consumed: () => this.#consumed(entry),
            },
          };
        }
        if (this.#sentinel === 'canceled') {
          return {value: undefined, done: true};
        }
        if (this.#sentinel) {
          return Promise.reject(this.#sentinel);
        }
        const consumer = resolver<Entry<M> | null>();
        this.#consumers.push(consumer);

        // Wait for push() (or termination) to resolve the consumer.
        const result = await consumer.promise;
        return result
          ? {
              value: {
                value: this.#publish(result.value),
                consumed: () => this.#consumed(result),
              },
            }
          : {value: undefined, done: true};
      },

      return: value => {
        this.cancel();
        return Promise.resolve({value, done: true});
      },
    };
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    const delegate = this.#pipeline();

    let prevConsumed = () => {};
    return {
      next: async () => {
        prevConsumed();

        const entry = await delegate.next();
        if (entry.done) {
          return entry;
        }

        const {value, consumed} = entry.value;
        prevConsumed = consumed;
        return {value};
      },

      return: value => {
        prevConsumed();

        this.cancel();
        return Promise.resolve({value, done: true});
      },
    };
  }
}

export type Options<M> = {
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

  /**
   * Enable or disable pipelining when streaming messages over a websocket.
   *
   * If unspecified, pipelining is enabled if there is no {@link Options.coalesce coalesce}
   * method, as pipelining is counter to the semantics of coalescing. However, the
   * application can explicitly enable pipelining even if there is a coalesce method
   * by specifying `true` for this option. This assumes that coalescing is either
   * not important for websocket semantics, or that the receiving end of the websocket
   * transport performs the desired coalescing.
   */
  pipeline?: boolean;
};

/** Post-queueing results of messages. */
export type Result = 'consumed' | 'coalesced' | 'unconsumed';

/**
 * {@link Subscription.subscribe()} wraps the `Promise<Result>` in a `PendingResult`
 * object to avoid forcing all callers to handle the Promise, as most logic does not
 * need to.
 */
export type PendingResult = {result: Promise<Result>};

type Entry<M> = {
  readonly value: M;
  readonly resolve: (r: Result) => void;
};
