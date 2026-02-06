import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../../shared/src/asserts.ts';
import type {Source} from '../../../types/streams.ts';
import {Subscription} from '../../../types/subscription.ts';
import type {ChangeStreamMessage, StatusMessage} from '../protocol/current.ts';

type Cancelable = {
  cancel(): void;
};

type Listener = {
  onChange(change: ChangeStreamMessage): void;
};

type Waiter = {
  producer: string;
  startTime: number;
  grantReservation: (watermark: string) => void;
};

/**
 * Facilitates cooperative multiplexing of transactions from
 * multiple producers into a single change stream.
 */
export class ChangeStreamMultiplexer {
  readonly #lc: LogContext;
  readonly #sub: Subscription<ChangeStreamMessage>;
  readonly #listeners: Listener[];

  /**
   * The `#lastWatermark` tracks the watermark of the last transaction
   * committed to stream. It is set to null when a producer has reserved
   * the stream, and set to the new watermark when the producer releases
   * the reservation.
   */
  #lastWatermark: string | null;

  /**
   * Tracks the queue of producers waiting for a reservation.
   */
  readonly #waiters: Waiter[] = [];

  constructor(
    lc: LogContext,
    lastWatermark: string,
    producers: Cancelable[],
    listeners: Listener[],
  ) {
    this.#lc = lc;
    this.#sub = Subscription.create<ChangeStreamMessage>({
      cleanup: () => producers.forEach(p => p.cancel()),
    });
    this.#listeners = listeners;
    this.#lastWatermark = lastWatermark;
  }

  /**
   * Called by a producer to "reserve" the exclusive right to push data
   * changes to the stream. If the stream is currently reserved,
   * the resulting Promise must be awaited.
   *
   * The producer must then {@link release()} the reservation when it sees
   * fit.
   *
   * @param producer The name of the producer, purely for debugging output
   */
  reserve(producer: string): string | Promise<string> {
    if (this.#lastWatermark !== null) {
      // If the stream is not reserved, reserve it and return the
      // watermark.
      const lastWatermark = this.#lastWatermark;
      this.#lastWatermark = null;
      return lastWatermark;
    }

    // Otherwise, wait for the current reservation to be released.
    const startTime = performance.now();
    const {promise, resolve: grantReservation} = resolver<string>();
    this.#waiters.push({producer, startTime, grantReservation});

    return promise;
  }

  /**
   * If there are producers currently awaiting a reservation, returns
   * the duration (in milliseconds) of the oldest reservation request.
   * Returns a negative number if there are no waiters.
   */
  waiterDelay(): number {
    if (this.#waiters.length === 0) {
      return -1;
    }
    return performance.now() - this.#waiters[0].startTime;
  }

  /**
   * Called by a producer to release its reservation after committing its
   * last transaction.
   */
  release(newWatermark: string) {
    const waiter = this.#waiters.shift();
    if (!waiter) {
      this.#lastWatermark = newWatermark;
    } else {
      const {producer, startTime, grantReservation} = waiter;
      grantReservation(newWatermark);
      const elapsed = performance.now() - startTime;
      this.#lc.info?.(
        `${producer} waited ${elapsed.toFixed(3)} ms for stream reservation`,
      );
    }
  }

  /**
   * `pushStatus()` can be called without a reservation, as it
   * does not constitute a data change and can appear anywhere
   * in the stream.
   */
  pushStatus(message: StatusMessage) {
    this.#sub.push(message);
  }

  /**
   * `push()` must only be called by a producer after it has
   * {@link reserve}d the stream.
   */
  push(message: ChangeStreamMessage): Promise<unknown> {
    assert(
      this.#lastWatermark === null,
      `push() called without reserve()-ing the stream`,
    );
    this.#listeners.forEach(l => l.onChange(message));
    return this.#sub.push(message).result;
  }

  fail(err: Error) {
    this.#sub.fail(err);
  }

  asSource(): Source<ChangeStreamMessage> {
    return this.#sub;
  }
}
