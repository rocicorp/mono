import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import type {WatermarkedChange} from './change-streamer-service.ts';
import type {Subscriber} from './subscriber.ts';

type BroadcastChange = WatermarkedChange | readonly WatermarkedChange[];

/**
 * Initiates and tracks the progress of a change broadcasted to
 * a set of subscribers.
 *
 * Creating a `Broadcast` automatically initiates the send.
 *
 * By default, {@link Broadcast.done} resolves when all subscribers
 * have acked the change. When flow control options are supplied,
 * {@link Broadcast.done} resolves after majority consensus plus the
 * configured padding.
 */
export class Broadcast {
  /**
   * Sends the change to the subscribers without the tracking machinery.
   * This is suitable for fire-and-forget (i.e. pipelined) sends.
   */
  static withoutTracking(
    subscribers: Iterable<Subscriber>,
    change: BroadcastChange,
  ) {
    const changes = normalizeChanges(change);
    for (const sub of subscribers) {
      void sub.sendBatch(changes);
    }
  }

  readonly #pending: Set<Subscriber>;
  readonly #completed: Completed[];
  readonly #done = resolver();
  readonly #flowControl: FlowControlOptions | undefined;
  #isDone = false;
  #flowControlTimer: ReturnType<typeof setTimeout> | undefined;

  readonly #watermark: string;
  readonly #majority: number;

  readonly #start = performance.now();
  #latestCompleted = Number.MAX_VALUE;

  /**
   * Broadcasts the `change` to the `subscribers` and tracks their
   * completion.
   */
  constructor(
    subscribers: Iterable<Subscriber>,
    change: BroadcastChange,
    flowControl: FlowControlOptions | undefined = undefined,
  ) {
    const changes = normalizeChanges(change);
    this.#pending = new Set(subscribers);
    this.#completed = [];
    this.#flowControl = flowControl;
    this.#watermark = changes.at(-1)?.[0] ?? 'none';
    this.#majority = Math.floor(this.#pending.size / 2) + 1;

    for (const sub of this.#pending) {
      const numChanges = sub.numPending + changes.length;
      void sub
        .sendBatch(changes)
        .catch(() => {})
        .finally(() => this.#markCompleted(sub, numChanges));
    }

    // set done if there are no subscribers (mainly for tests)
    if (this.#pending.size === 0) {
      this.#setDone();
    }
  }

  #markCompleted(sub: Subscriber, changes: number) {
    const elapsed = (this.#latestCompleted = performance.now()) - this.#start;
    this.#completed.push({sub, changes, elapsed});
    this.#pending.delete(sub);
    if (this.#pending.size === 0) {
      this.#setDone();
    } else {
      this.#resetConsensusTimer();
    }
  }

  #setDone(): boolean {
    if (this.#isDone) {
      return false;
    }
    this.#isDone = true;
    this.#clearConsensusTimer();
    this.#done.resolve();
    return true;
  }

  #resetConsensusTimer() {
    if (
      this.#isDone ||
      this.#flowControl === undefined ||
      !(this.#flowControl.flowControlConsensusPaddingMs >= 0) ||
      this.#completed.length < this.#majority
    ) {
      return;
    }
    this.#clearConsensusTimer();
    this.#flowControlTimer = setTimeout(
      this.#releaseAfterConsensusPadding,
      this.#flowControl.flowControlConsensusPaddingMs,
    );
  }

  readonly #releaseAfterConsensusPadding = () => {
    this.#flowControlTimer = undefined;
    const flowControl = this.#flowControl;
    if (
      this.#isDone ||
      flowControl === undefined ||
      this.#pending.size === 0 ||
      this.#completed.length < this.#majority
    ) {
      return;
    }
    const now = performance.now();
    if (
      now - this.#latestCompleted <
      flowControl.flowControlConsensusPaddingMs
    ) {
      this.#resetConsensusTimer();
      return;
    }
    this.#logWithState(
      flowControl.lc,
      `continuing with ${this.#pending.size} subscriber(s) still pending`,
      now - this.#start,
    );
    this.#setDone();
  };

  #clearConsensusTimer() {
    clearTimeout(this.#flowControlTimer);
    this.#flowControlTimer = undefined;
  }

  get isDone(): boolean {
    return this.#isDone;
  }

  get done(): Promise<void> {
    return this.#done.promise;
  }

  /**
   * Checks for pathological situations in which flow should be reenabled
   * before all subscribers have acked.
   *
   * ### Background
   *
   * The purpose of flow control is to pull upstream replication changes
   * no faster than the rate as they are processed by downstream subscribers
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
   *   a fixed timeout elapses (e.g. 1 second), even if not all subscribers
   *   have finished.
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
   *
   * ### Not implemented: Broken connection detection
   *
   * If a subscriber has not made progress for a certain interval, the
   * algorithm could theoretically drop it preemptively, supplementing the
   * existing websocket-level liveness checks.
   *
   * However, a more reliable approach would be to change the replicator
   * to use non-blocking writes, and subsequently increase the frequency of
   * connection-level liveness checks. The current synchronous replica writes
   * can delay both ping responsiveness and change progress arbitrarily (e.g.
   * a large index creation); an independently liveness check that is not
   * delayed by synchronous writes on the subscriber would be a more failsafe
   * solution.
   *
   * @returns `true` if the broadcast was already done or was marked done.
   */
  checkProgress(
    lc: LogContext,
    flowControlConsensusPaddingMs: number,
    now: number,
  ) {
    if (this.#isDone) {
      return true;
    }
    if (this.#pending.size === 0) {
      return true;
    }
    if (!(flowControlConsensusPaddingMs >= 0)) {
      return false;
    }
    const elapsed = now - this.#start;
    if (this.#completed.length < this.#majority) {
      if (elapsed >= 1000) {
        this.#logWithState(
          lc,
          `waiting for at least ${this.#majority} subscribers to finish`,
          elapsed,
        );
      }
      return false;
    }
    // Note: In the implementation, #latestCompleted is always updated,
    // even after the majority is reached. This is fine and does not affect
    // the important properties of the algorithm.
    if (now - this.#latestCompleted >= flowControlConsensusPaddingMs) {
      this.#logWithState(
        lc,
        `continuing with ${this.#pending.size} subscriber(s) still pending`,
        elapsed,
      );
      this.#setDone();
      return true;
    }
    return false;
  }

  #logWithState(lc: LogContext, msg: string, elapsed: number) {
    lc.withContext('watermark', this.#watermark).info?.(
      `${msg} (${elapsed.toFixed(3)} ms)`,
      {
        completed: this.#completed.map(d => ({
          id: d.sub.id,
          processed: d.changes,
          elapsed: d.elapsed,
        })),
        pending: Array.from(this.#pending, sub => ({
          id: sub.id,
          ...sub.getStats(),
        })),
      },
    );
  }
}

function normalizeChanges(
  change: BroadcastChange,
): readonly WatermarkedChange[] {
  return isWatermarkedChange(change) ? [change] : change;
}

function isWatermarkedChange(
  change: BroadcastChange,
): change is WatermarkedChange {
  return typeof change[0] === 'string';
}

/** Tracks the completed result of a single subscriber. */
type Completed = {
  sub: Subscriber;
  /** The number of changes processed. */
  changes: number;
  /** The elapsed milliseconds. */
  elapsed: number;
};

export type FlowControlOptions = {
  readonly lc: LogContext;
  readonly flowControlConsensusPaddingMs: number;
};
