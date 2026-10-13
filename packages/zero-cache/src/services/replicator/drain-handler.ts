import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {readHead, type Sized, type Source} from '../../types/streams.ts';
import type {
  ChangeStreamer,
  Status,
  SubscriberContext,
} from '../change-streamer/change-streamer.ts';
import type {SubscribeDownstream} from '../change-streamer/subscribe.ts';
import type {RunningState} from '../running-state.ts';

// Bounds the time to wait for a switchover subscription to be established
// and to receive its first message.
const DEFAULT_SWITCHOVER_TIMEOUT_MS = 10_000;
const MIN_SWITCHOVER_RETRY_DELAY_MS = 1_000;
const MAX_SWITCHOVER_RETRY_DELAY_MS = 30_000;

export type SwitchoverOptions = {
  /**
   * Called when a replication-manager with a newer replica version grants
   * permission to restore its replica. This should initiate a graceful
   * restart of the server (during which the current subscription continues
   * to be served).
   */
  onRestoreGranted: () => void;

  /**
   * The replica file, from which the state of the last committed
   * transaction is read for switchover subscriptions.
   */
  replicaFile: string;

  /** For testing. */
  timeoutMs?: number | undefined;
};

export type Downstream = Source<Sized<SubscribeDownstream>>;

type AttemptOutcome = 'switched' | 'restoring' | 'retry';

/**
 * Handles the switchover of a serving replica's subscription from a draining
 * replication-manager to another one, without interrupting the current
 * subscription.
 *
 * Upon {@link onChangeStreamerDrain}, the handler repeatedly (with backoff)
 * opens a new `switchover` subscription, at the watermark of the replica's
 * last committed transaction, to whichever replication-manager the
 * connection is routed to. The first message on the new subscription
 * determines the outcome:
 * * `status`: The subscription was accepted (i.e. the replica is compatible).
 *   The current subscription is cancelled (aborting any transaction in
 *   progress), and the new one is made available via
 *   {@link getNextConnection}. Note that the new subscription may start with
 *   transactions that were committed from the current subscription in the
 *   meantime, which the consumer must skip.
 * * `status` with `restore: queued`: The replication-manager has a newer
 *   replica version, and is holding the subscription until it grants
 *   permission to restore. The held subscription is monitored for the grant.
 * * `status` with `restore: granted`: Permission to restore is granted, and
 *   {@link SwitchoverOptions.onRestoreGranted} is called. The granted
 *   subscription is kept open until the process exits, so that the
 *   replication-manager can track the grant until the restore begins.
 * * Anything else (e.g. an error, or a `draining` replication-manager):
 *   The switchover is retried later.
 *
 * The current subscription continues to be consumed throughout. A
 * switchover is scoped to the subscription from which it switches: If that
 * subscription ends first (e.g. the replication-manager shuts down), the
 * switchover is abandoned, including any held (but not granted)
 * subscription, as the consumer reconnects on its own.
 */
export class DrainHandler {
  readonly #lc: LogContext;
  readonly #changeStreamer: ChangeStreamer;
  readonly #state: RunningState;
  readonly #options: SwitchoverOptions;
  readonly #committedContext: () => SubscriberContext;

  // The subscription currently being switched from.
  #draining: Downstream | undefined;
  #restoreGranted = false;
  #next: Downstream | undefined;

  /**
   * @param committedContext Returns the subscriber context as of the
   *        replica's last committed transaction.
   */
  constructor(
    lc: LogContext,
    changeStreamer: ChangeStreamer,
    state: RunningState,
    options: SwitchoverOptions,
    committedContext: () => SubscriberContext,
  ) {
    this.#lc = lc.withContext('component', 'drain-handler');
    this.#changeStreamer = changeStreamer;
    this.#state = state;
    this.#options = options;
    this.#committedContext = committedContext;
  }

  /**
   * Called when the change-streamer of the `current` subscription is
   * draining.
   */
  onChangeStreamerDrain(current: Downstream) {
    if (
      this.#restoreGranted ||
      this.#draining === current ||
      !this.#isActive(current)
    ) {
      return;
    }
    this.#draining = current;
    this.#lc.info?.(
      `replication-manager is draining. switching over to another`,
    );
    void this.#switchover(current);
  }

  get hasNextConnection(): boolean {
    return this.#next !== undefined;
  }

  /**
   * Returns (and clears) the subscription that replaces the (now cancelled)
   * current one, if a switchover succeeded.
   */
  getNextConnection(): Downstream | undefined {
    const next = this.#next;
    this.#next = undefined;
    return next;
  }

  #isActive(current: Downstream) {
    return !current.signal.aborted && this.#state.shouldRun();
  }

  async #switchover(current: Downstream) {
    let failures = 0;
    while (this.#isActive(current)) {
      const outcome = await this.#attempt(current);
      if (outcome !== 'retry') {
        return;
      }
      const delay = Math.min(
        MIN_SWITCHOVER_RETRY_DELAY_MS * 2 ** failures++,
        MAX_SWITCHOVER_RETRY_DELAY_MS,
      );
      await this.#state.sleep(delay);
    }
  }

  async #attempt(current: Downstream): Promise<AttemptOutcome> {
    const lc = this.#lc;
    const timeoutMs = this.#options.timeoutMs ?? DEFAULT_SWITCHOVER_TIMEOUT_MS;
    let candidate: Downstream | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let timedOut = false;
    try {
      const ctx = this.#committedContext();
      lc.info?.(`attempting switchover at ${ctx.watermark}`);
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          timedOut = true;
          reject(new Error(`timed out after ${timeoutMs} ms`));
        }, timeoutMs);
      });
      const subscribing = this.#changeStreamer.subscribe({
        ...ctx,
        switchover: true,
      });
      // Clean up a connection that is established after a timeout.
      void subscribing.then(
        sub => (timedOut ? sub.cancel() : undefined),
        () => {},
      );
      candidate = await Promise.race([subscribing, timeout]);
      const {head, rest} = readHead(candidate);
      const first = await Promise.race([head.next(), timeout]);
      const msg = first.done ? undefined : first.value.data;

      if (!this.#isActive(current)) {
        // e.g. The current subscription failed in the meantime.
      } else if (msg?.[0] === 'status') {
        const {restore, draining} = msg[1];
        if (restore) {
          const granted = await this.#holdForRestore(current, rest, restore);
          if (granted) {
            return 'restoring';
          }
          lc.info?.(`held switchover subscription closed. will retry`);
          return 'retry';
        }
        if (!draining) {
          lc.info?.(`switched over to a new subscription at ${ctx.watermark}`);
          this.#next = rest;
          current.cancel();
          return 'switched';
        }
      }
      lc.warn?.(`switchover rejected. continuing current subscription`, msg);
    } catch (e) {
      lc.warn?.(`switchover failed. continuing current subscription`, e);
    } finally {
      clearTimeout(timer);
    }
    candidate?.cancel();
    return 'retry';
  }

  /**
   * Keeps the held (or granted) subscription open, and initiates a graceful
   * restart when permission to restore is granted. A held subscription that
   * has yet to be granted is cancelled if the `current` subscription ends.
   *
   * @returns `true` once permission is granted, or `false` if the held
   *          subscription closes before then.
   */
  #holdForRestore(
    current: Downstream,
    held: Downstream,
    restore: NonNullable<Status['restore']>,
  ): Promise<boolean> {
    const lc = this.#lc;
    const result = resolver<boolean>();
    const unregister = this.#state.cancelOnStop(held);
    const onCurrentEnded = () => {
      if (!this.#restoreGranted) {
        lc.info?.(`abandoning held switchover subscription`);
        held.cancel();
      }
    };
    current.signal.addEventListener('abort', onCurrentEnded, {once: true});

    const onRestore = (r: NonNullable<Status['restore']>) => {
      if (r.state === 'queued') {
        lc.info?.(
          `waiting for permission to restore a new replica (position ${r.position})`,
        );
      } else if (!this.#restoreGranted) {
        lc.info?.(
          `granted permission to restore a new replica. restarting gracefully`,
        );
        this.#restoreGranted = true;
        this.#options.onRestoreGranted();
        result.resolve(true);
      }
    };
    onRestore(restore);
    void (async () => {
      try {
        // Note: The loop must not be exited early, as that would close the
        // connection.
        for await (const {data: msg} of held) {
          if (msg[0] === 'status' && msg[1].restore) {
            onRestore(msg[1].restore);
          }
        }
      } catch (e) {
        lc.warn?.(`held switchover subscription failed`, e);
      } finally {
        unregister();
        current.signal.removeEventListener('abort', onCurrentEnded);
        result.resolve(false);
      }
    })();
    return result.promise;
  }
}
