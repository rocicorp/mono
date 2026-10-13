import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import type {Subscription} from '../../types/subscription.ts';
import type {PreSerializedBatch} from './broadcast.ts';
import type {Subscriber} from './subscriber.ts';

const PROGRESS_LOG_INTERVAL_MS = 30_000;

/**
 * Tracks the {@link Subscriber.drainable drainable} subscribers of the
 * change-streamer, i.e. those that switch over to another
 * replication-manager when this one drains.
 *
 * Upon {@link drain}, drainable subscribers are sent a `draining` status
 * (as are drainable subscribers that subscribe while draining), and the
 * returned promise resolves once they have all disconnected. Subscribers
 * continue to be served in the meantime.
 */
export class DrainTracker {
  readonly #lc: LogContext;
  readonly #subscribers = new Set<Subscriber>();
  readonly #setInterval: typeof setInterval;
  readonly #clearInterval: typeof clearInterval;
  #draining: Resolver<void> | undefined;

  constructor(
    lc: LogContext,
    // Injectable timers, for deterministic testing.
    setIntervalFn: typeof setInterval = setInterval,
    clearIntervalFn: typeof clearInterval = clearInterval,
  ) {
    this.#lc = lc.withContext('component', 'drain-tracker');
    this.#setInterval = setIntervalFn;
    this.#clearInterval = clearIntervalFn;
  }

  get draining(): boolean {
    return this.#draining !== undefined;
  }

  register(
    subscriber: Subscriber,
    downstream: Subscription<string | PreSerializedBatch>,
  ) {
    if (!subscriber.drainable) {
      return;
    }
    this.#subscribers.add(subscriber);
    if (this.#draining) {
      subscriber.sendControlStatus({tag: 'status', draining: true});
    }
    downstream.addCloseHandler(() => {
      if (this.#subscribers.delete(subscriber)) {
        this.#checkDrained();
      }
    });
  }

  drain(): Promise<void> {
    if (!this.#draining) {
      const draining = (this.#draining = resolver<void>());
      this.#lc.info?.(
        `draining: signaling ${this.#subscribers.size} subscribers ` +
          `to switch over`,
      );
      for (const sub of this.#subscribers) {
        sub.sendControlStatus({tag: 'status', draining: true});
      }
      const progress = this.#setInterval(
        () =>
          this.#lc.info?.(
            `draining: waiting for ${this.#subscribers.size} ` +
              `subscribers to switch over`,
            Array.from(this.#subscribers, sub => sub.id),
          ),
        PROGRESS_LOG_INTERVAL_MS,
      );
      void draining.promise.finally(() => this.#clearInterval(progress));
      this.#checkDrained();
    }
    return this.#draining.promise;
  }

  /** Resolves any pending drain, e.g. when the change-streamer stops. */
  stop() {
    this.#draining?.resolve();
  }

  #checkDrained() {
    if (this.#draining && this.#subscribers.size === 0) {
      this.#lc.info?.(`drained all subscribers`);
      this.#draining.resolve();
    }
  }
}
