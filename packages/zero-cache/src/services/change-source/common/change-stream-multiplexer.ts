import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {assert} from '../../../../../shared/src/asserts.ts';
import type {Source} from '../../../types/streams.ts';
import {Subscription} from '../../../types/subscription.ts';
import type {
  ChangeStreamMessage,
  DownstreamStatusMessage,
} from '../protocol/current.ts';

export type Cancelable = {
  cancel(): void;
};

export type Listener = {
  onChange(change: ChangeStreamMessage): void;
};

type Waiter = {
  producer: string;
  startTime: number;
  grantReservation: (watermark: string) => void;
  rejectReservation: (err: Error) => void;
};

/**
 * Facilitates cooperative multiplexing of transactions from
 * multiple producers into a single change stream.
 */
export class ChangeStreamMultiplexer {
  readonly #lc: LogContext;
  readonly #sub: Subscription<ChangeStreamMessage>;
  readonly #producers: Cancelable[] = [];
  readonly #listeners: Listener[] = [];

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

  #canceled = false;

  constructor(lc: LogContext, lastWatermark: string) {
    this.#lc = lc;
    this.#sub = Subscription.create<ChangeStreamMessage>({
      cleanup: () => this.#cancel(),
    });
    this.#lastWatermark = lastWatermark;
  }

  #cancel() {
    this.#canceled = true;
    this.#producers.forEach(p => p.cancel());

    // A producer awaiting a reservation would otherwise remain suspended
    // forever, since the reservation held when the stream was canceled is
    // never released. Reject the pending reservations so that the producers
    // unwind and release the resources (e.g. upstream connections and
    // transactions) that they were holding for the stream.
    for (const {producer, rejectReservation} of this.#waiters.splice(0)) {
      this.#lc.info?.(
        `rejecting reservation request from ${producer}: stream canceled`,
      );
      rejectReservation(new AbortError('change stream canceled'));
    }
  }

  addProducers(...p: Cancelable[]): this {
    this.#producers.push(...p);
    return this;
  }

  addListeners(...l: Listener[]): this {
    this.#listeners.push(...l);
    return this;
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
    if (this.#canceled) {
      return Promise.reject(new AbortError('change stream canceled'));
    }
    if (this.#lastWatermark !== null) {
      // If the stream is not reserved, reserve it and return the
      // watermark.
      const lastWatermark = this.#lastWatermark;
      this.#lastWatermark = null;
      return lastWatermark;
    }

    // Otherwise, wait for the current reservation to be released.
    const startTime = performance.now();
    const {
      promise,
      resolve: grantReservation,
      reject: rejectReservation,
    } = resolver<string>();
    this.#waiters.push({
      producer,
      startTime,
      grantReservation,
      rejectReservation,
    });

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
  pushStatus(message: DownstreamStatusMessage) {
    // Let listeners know about all status messages.
    this.#listeners.forEach(l => l.onChange(message));
    // The ChangeStreamer only cares about status messages requiring an ack
    // or containing a lagReport. To reduce churn, avoid sending other status
    // messages.
    if (message[1].ack || message[1].lagReport) {
      this.#sub.push(message);
    }
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
