import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {WatermarkedChange} from './change-streamer.ts';
import type {Subscriber} from './subscriber.ts';

export type BroadcastReleaseMode = 'all-subscribers' | 'consensus-timeout';

/**
 * Enables event-driven early release of a {@link Broadcast}. When provided and a
 * majority has acked, the broadcast schedules its own consensus-timeout release
 * proportional to how long the majority took to ack, instead of relying on the
 * Forwarder's periodic progress tick.
 */
export type EarlyReleaseOptions = {
  /** Time to wait, proportional to majority completion, before releasing. */
  consensusTimeoutProportion: number;
  /** Injectable timer, for deterministic testing. */
  setTimeoutFn?: typeof setTimeout;
  /** Injectable timer cancellation, for deterministic testing. */
  clearTimeoutFn?: typeof clearTimeout;
};

/**
 * Initiates and tracks the progress of a change broadcasted to
 * a set of subscribers, implementing **consensus-based timeouts**
 * to effect short-term graceful degradation when a minority of
 * subscribers are behind.
 *
 * ### Background
 *
 * The purpose of flow control is to pull upstream replication changes
 * no faster than the rate they are processed by downstream subscribers
 * in the steady state. In the change-streamer, this is done by occasionally
 * waiting for ACKs from subscribers before continuing; without doing so,
 * I/O buffers fill up and cause the system to spend most of its time in GC.
 *
 * However, the naive algorithm of always waiting for all subscribers (e.g.
 * `Promise.all()`) can behave poorly in scenarios where subscribers
 * are imbalanced:
 * * New subscribers may have a backlog of changes to catch up with.
 *   Having all subscribers wait for the new subscriber to catch up results
 *   in delaying the entire application.
 * * Broken TCP connections similarly require all subscribers to wait until
 *   connection liveness checks kick in and disconnect the subscriber.
 *
 * A simplistic approach is to add a limit to the amount of time waiting for
 * subscribers, i.e. an ack timeout. However, deciding what this timeout
 * should be is non-trivial because of the heterogeneous nature of changes;
 * while most changes operate on single rows and are relatively predictable
 * in terms of running time, some changes are table-wide operations and can
 * legitimately take an arbitrary amount of time. In such scenarios, a
 * timeout that is too short can stop progress on replication altogether.
 *
 * ### Consensus-based Timeout Algorithm
 *
 * To address these shortcomings, a "consensus-based timeout" algorithm is
 * used:
 * * Wait for more than half of the subscribers to finish. (In
 *   case of a single node, or the case of one replication-manager
 *   and one view-syncer, this reduces to waiting for all subscribers.)
 * * Once more than half of the subscribers have finished, proceed after
 *   the configured timeout, proportional to the time it took for the
 *   majority of subscribers to ack, even if not all subscribers have
 *   finished.
 *
 * In other words, the subscribers themselves are used to determine the
 * timeout of each batch of changes; the majority determines this when
 * they complete, upon which a timeout is logically started.
 *
 * In the common case, the remaining subscribers finish soon afterward and
 * the timeout never elapses. However, in pathological cases where a minority
 * of subscribers have a disproportionate amount of load, some will still
 * be processing (or otherwise unresponsive). These subscribers are given
 * a bounded amount of time to catch up at each flushed batch, up to the
 * timeout interval. This guarantees eventual catchup because the
 * subscribers with a backlog of changes necessarily have a higher
 * processing rate than the subscribers that finished (and are made to wait).
 */
export class Broadcast {
  /**
   * Sends the change to the subscribers without the tracking machinery.
   * This is suitable for fire-and-forget (i.e. pipelined) sends.
   */
  static withoutTracking(
    subscribers: Iterable<Subscriber>,
    change: WatermarkedChange,
  ) {
    for (const sub of subscribers) {
      void sub.send(change);
    }
  }

  readonly #lc: LogContext;
  readonly #pending: Set<Subscriber>;
  readonly #completed: Completed[];
  readonly #done = resolver();
  #isDone = false;
  #releaseMode: BroadcastReleaseMode | undefined;

  readonly #watermark: string;
  readonly #majority: number;

  readonly #start = performance.now();

  // When set, the broadcast schedules its own consensus-timeout release instead
  // of waiting for the Forwarder's periodic progress tick.
  readonly #earlyReleaseTimeoutProportion: number | undefined;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  #earlyReleaseTimer: ReturnType<typeof setTimeout> | undefined;

  // Tracks whether the backup-replicator responded. It is a required member
  // of the consensus timeout since it cannot be dropped as a laggard (since
  // that would shutdown the replication-manager itself).
  #needsBackupResponse = false;

  /**
   * Broadcasts the `change` to the `subscribers` and tracks their
   * completion.
   */
  constructor(
    lc: LogContext,
    subscribers: Iterable<Subscriber>,
    change: WatermarkedChange,
    earlyRelease?: EarlyReleaseOptions,
  ) {
    this.#lc = lc;
    this.#pending = new Set(subscribers);
    this.#completed = [];
    this.#watermark = change[0];
    this.#majority = Math.floor(this.#pending.size / 2) + 1;
    this.#earlyReleaseTimeoutProportion =
      earlyRelease?.consensusTimeoutProportion;
    this.#setTimeout = earlyRelease?.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = earlyRelease?.clearTimeoutFn ?? clearTimeout;

    for (const sub of this.#pending) {
      const changes = sub.numPending + 1; // add one for this `change`
      if (sub.mode === 'backup') {
        // Only gate consensus on the backup-replicator after it has finished
        // its initial catchup.
        this.#needsBackupResponse ||= !sub.isBacklogged();
      }
      void sub
        .send(change)
        .catch(() => {})
        .finally(() => this.#markCompleted(sub, changes));
    }

    // set done if there are no subscribers (mainly for tests)
    if (this.#pending.size === 0) {
      this.#setDone('all-subscribers');
    }
  }

  #markCompleted(sub: Subscriber, changes: number) {
    if (this.#isDone) {
      return;
    }
    const elapsed = performance.now() - this.#start;
    sub.trackResponseResult('on-time');
    if (sub.mode === 'backup') {
      this.#needsBackupResponse = false;
    }
    this.#completed.push({sub, changes, elapsed});
    this.#pending.delete(sub);
    if (this.#pending.size === 0) {
      this.#setDone('all-subscribers');
      return;
    }
    // Event-driven consensus-timeout: the moment a majority acks, and it
    // includes the backup-replicator, arm a single release timer
    // proportional to how long the majority took to ack. The broadcast
    // resolves when it fires, unless all subscribers ack first.
    if (
      this.#earlyReleaseTimeoutProportion !== undefined &&
      this.#earlyReleaseTimer === undefined &&
      this.#completed.length >= this.#majority &&
      !this.#needsBackupResponse
    ) {
      const timeoutMs = Math.ceil(
        elapsed * this.#earlyReleaseTimeoutProportion,
      );
      this.#earlyReleaseTimer = this.#setTimeout(
        () => this.#setDone('consensus-timeout'),
        timeoutMs,
      );
    }
  }

  #setDone(releaseMode: BroadcastReleaseMode) {
    if (this.#isDone) {
      return;
    }
    if (releaseMode === 'consensus-timeout') {
      const elapsed = performance.now() - this.#start;
      this.#logWithState(
        `continuing with ${this.#pending.size} subscriber(s) still pending`,
        elapsed,
      );
    }
    this.#pending.forEach(sub => sub.trackResponseResult('timed-out'));
    this.#isDone = true;
    this.#releaseMode = releaseMode;
    this.#clearTimeout(this.#earlyReleaseTimer);
    this.#earlyReleaseTimer = undefined;
    this.#done.resolve();
  }

  get isDone(): boolean {
    return this.#isDone;
  }

  get done(): Promise<void> {
    return this.#done.promise;
  }

  get releaseMode(): BroadcastReleaseMode {
    return this.#releaseMode ?? 'all-subscribers';
  }

  logSlowRequests(now: number) {
    if (this.#isDone || this.#pending.size === 0) {
      return;
    }
    const elapsed = now - this.#start;
    if (this.#completed.length < this.#majority) {
      if (elapsed >= 1000) {
        this.#logWithState(
          `waiting for at least ${this.#majority} subscribers to finish`,
          elapsed,
        );
      }
    }
  }

  #logWithState(msg: string, elapsed: number) {
    this.#lc
      .withContext('watermark', this.#watermark)
      .info?.(`${msg} (${elapsed.toFixed(3)} ms)`, {
        completed: this.#completed.map(d => ({
          id: d.sub.id,
          processed: d.changes,
          elapsed: d.elapsed,
        })),
        pending: Array.from(this.#pending, sub => ({
          id: sub.id,
          ...sub.getStats(),
        })),
      });
  }
}

/** Tracks the completed result of a single subscriber. */
type Completed = {
  sub: Subscriber;
  /** The number of changes processed. */
  changes: number;
  /** The elapsed milliseconds. */
  elapsed: number;
};
