import {performance} from 'node:perf_hooks';
import type {LogContext} from '@rocicorp/logger';

/** How often the multiplier is updated. */
const UPDATE_INTERVAL_MS = 1000;

/**
 * The weight of the latest second in the worker's smoothed busy share.
 * Client groups that wait for seconds between batches make the share of
 * any one second jump between idle and saturated, and a worker that keeps
 * up has the odd busy second. Neither says what the load is.
 */
const SHARE_SMOOTHING = 0.3;

/**
 * How much of the difference between the smoothed busy share and the target
 * each update corrects for. The smoothed share only shows part of what the
 * previous updates did, so correcting for all of the difference each time
 * would overshoot and oscillate.
 */
const CORRECTION_EXPONENT = 0.3;

/**
 * The share of its time above which a worker has no time to spare. A share
 * target has to stay well clear of this to be held, which is why
 * `syncUpdateShareTarget` is at most 0.9 (or 1, which no worker is over).
 */
const SATURATED_SHARE = 0.95;

/** The seconds that a worker has to be saturated for to be treated as such. */
const SATURATED_SECONDS = 2;

/**
 * How much `1 + k` is scaled by each second that the worker is saturated.
 * A saturated worker does not show how much more its client groups would
 * use, which can be many times what it has, so the multiplier is raised by
 * more than the difference from the target calls for until it does.
 */
const SATURATED_CORRECTION = 1.3;

/**
 * How often a worker that is pacing its client groups says so in its log,
 * and how long it has to have stopped for to say that it has.
 */
const PACING_LOG_INTERVAL_MS = 60_000;

/**
 * The most updates that are made up for when nothing has asked the pacer
 * for a wait in a while, which is enough for the multiplier to get to 0.
 */
const MAX_CATCH_UP_UPDATES = 60;

/**
 * Bounds the multiplier, which otherwise has no limit while a worker stays
 * over the target however long its client groups wait.
 */
const MAX_MULTIPLIER = 1_000_000;

export type BatchPacerOptions = {
  /**
   * The largest share of the worker's time that it may be busy before its
   * client groups are paced.
   */
  readonly shareTarget: number;
  /** The longest a client group waits between batches. */
  readonly maxIntervalMs: number;
  readonly now?: (() => number) | undefined;
  /**
   * The share of the time since the previous call that the worker was busy.
   * By default, the utilization of the event loop.
   */
  readonly utilization?: (() => number) | undefined;
  /** Where the worker says that it is pacing its client groups. */
  readonly lc?: LogContext | undefined;
};

function eventLoopUtilization(): () => number {
  let last = performance.eventLoopUtilization();
  return () => {
    const current = performance.eventLoopUtilization();
    const {utilization} = performance.eventLoopUtilization(current, last);
    last = current;
    return utilization;
  };
}

/**
 * Paces the batches (advancements, including the queries they hydrate
 * again) of the client groups of a sync worker, so that the worker is left
 * with time for new hydrations and connections, and gives the hydrations of
 * new queries priority over the batches.
 *
 * **Pacing.** After each batch, a client group waits {@link waitMs} before
 * it starts the next. The wait is a multiple of the batch's processing
 * time, so a client group that always has changes waiting is busy at most
 * `1 / (1 + k)` of the time, where `k` is the {@link multiplier}. Because
 * the wait is in proportion to a client group's own processing time, those
 * that cause the load are slowed the most, and cheap ones barely at all.
 *
 * The multiplier is shared by the client groups of the worker. Once per
 * second, it is scaled by how far the share of its time that the worker was
 * busy, smoothed over a few seconds, is from the target, and by more while
 * the worker has no time to spare. It never goes below 0, where no client
 * group waits. The share is the utilization of the worker's event loop, i.e. all
 * of what the worker does: what a batch costs is spread over the pipelines,
 * the CVR, the pokes and garbage collection, and is not measurable batch by
 * batch.
 *
 * **Priority of new hydrations.** The worker's time slices are handed out
 * by the pacer (see {@link yieldSlice}), and while the hydration of new
 * queries is waiting for one, most of them go to it rather than to batches.
 */
export class BatchPacer {
  readonly #shareTarget: number;
  readonly #maxIntervalMs: number;
  readonly #now: () => number;
  readonly #utilization: () => number;
  readonly #lc: LogContext | undefined;

  /** When the worker last logged that it is pacing, while it still is. */
  #pacingLoggedAt: number | undefined;
  /** Since when the worker has not been pacing, after it logged that it was. */
  #notPacingSince: number | undefined;

  #intervalStart: number;
  #multiplier = 0;
  /** The smoothed share of its time that the worker was busy. */
  #share = 0;
  #saturatedSeconds = 0;
  /** The waits handed out since the last update, by whether at the cap. */
  #waitsBelowMax = 0;
  #waitsAtMax = 0;

  /** The work that is waiting for a time slice, in the order it yielded. */
  readonly #waiting = {
    hydration: [] as (() => void)[],
    batch: [] as (() => void)[],
  };
  #handingOutSlices = false;
  /** The slices that have gone to hydrations since one went to a batch. */
  #hydrationSlices = 0;

  constructor({
    shareTarget,
    maxIntervalMs,
    now,
    utilization,
    lc,
  }: BatchPacerOptions) {
    this.#shareTarget = shareTarget;
    this.#maxIntervalMs = maxIntervalMs;
    this.#now = now ?? (() => performance.now());
    this.#utilization = utilization ?? eventLoopUtilization();
    this.#lc = lc;
    this.#intervalStart = this.#now();
  }

  /**
   * How long a client group should wait before its next batch, after one
   * that took `processingMs` to process. The multiplier changes while a
   * client group waits, so a long wait should be taken in steps, asking
   * again after each.
   */
  waitMs(processingMs: number): number {
    this.#update();
    const wait = this.#multiplier * processingMs;
    if (wait < this.#maxIntervalMs) {
      this.#waitsBelowMax++;
      return wait;
    }
    this.#waitsAtMax++;
    return this.#maxIntervalMs;
  }

  /** `k`: a client group waits `k` times the processing time of a batch. */
  get multiplier(): number {
    this.#update();
    return this.#multiplier;
  }

  /**
   * The share of its time that the worker was busy, smoothed over the last
   * few seconds, as of the last update of the multiplier.
   */
  get share(): number {
    this.#update();
    return this.#share;
  }

  /** Updates the multiplier for each interval that has ended. */
  #update(): void {
    const now = this.#now();
    const ended = Math.floor((now - this.#intervalStart) / UPDATE_INTERVAL_MS);
    if (ended <= 0) {
      return;
    }
    this.#intervalStart += ended * UPDATE_INTERVAL_MS;
    // If more than one interval has ended, nothing asked the pacer for a
    // wait in the meantime, and the utilization since then is that of each.
    const latest = this.#utilization();
    // Once every client group is told to wait the longest it can, raising
    // the multiplier changes nothing, and only makes it take longer to come
    // back down. A worker that is still over its target then is busy with
    // something that making its client groups wait does not relieve.
    const allAtMax = this.#waitsAtMax > 0 && this.#waitsBelowMax === 0;
    this.#waitsAtMax = 0;
    this.#waitsBelowMax = 0;
    for (let i = Math.min(ended, MAX_CATCH_UP_UPDATES); i > 0; i--) {
      this.#share += SHARE_SMOOTHING * (latest - this.#share);
      // A worker that is allowed to be this busy is only raised the
      // multiplier for what it is over its target by.
      this.#saturatedSeconds =
        latest >= SATURATED_SHARE && latest > this.#shareTarget
          ? this.#saturatedSeconds + 1
          : 0;
      let correction =
        this.#saturatedSeconds >= SATURATED_SECONDS
          ? SATURATED_CORRECTION
          : (this.#share / this.#shareTarget) ** CORRECTION_EXPONENT;
      if (allAtMax && correction > 1) {
        correction = 1;
      }
      this.#multiplier = Math.min(
        Math.max((1 + this.#multiplier) * correction - 1, 0),
        MAX_MULTIPLIER,
      );
    }
    this.#logPacing(now, allAtMax);
  }

  /**
   * Says in the worker's log when it starts to pace its client groups, once
   * a minute while it does, and when it has stopped. The pacing of a client
   * group is logged by its view syncer.
   */
  #logPacing(now: number, allAtMax: boolean): void {
    const lc = this.#lc;
    if (lc === undefined) {
      return;
    }
    const state = {
      busyShare: this.#share,
      shareTarget: this.#shareTarget,
      waitMultiplier: this.#multiplier,
      maxBatchIntervalMs: this.#maxIntervalMs,
    };
    if (this.#multiplier > 0) {
      this.#notPacingSince = undefined;
      if (this.#pacingLoggedAt === undefined) {
        lc.info?.(
          'Sync worker is over its share target: its client groups wait between batches',
          state,
        );
        this.#pacingLoggedAt = now;
      } else if (now - this.#pacingLoggedAt >= PACING_LOG_INTERVAL_MS) {
        if (allAtMax && this.#share > this.#shareTarget) {
          // Waiting longer is not an option, so pacing cannot bring the
          // worker to its target: it has more to do than it has time for.
          lc.warn?.(
            'Sync worker is over its share target although every client group waits the longest it may',
            state,
          );
        } else {
          lc.info?.('Sync worker is pacing its client groups', state);
        }
        this.#pacingLoggedAt = now;
      }
    } else if (this.#pacingLoggedAt !== undefined) {
      // A worker around its target goes in and out of pacing. It only says
      // that it has stopped once it has stayed out.
      this.#notPacingSince ??= now;
      if (now - this.#notPacingSince >= PACING_LOG_INTERVAL_MS) {
        lc.info?.(
          'Sync worker is under its share target: its client groups no longer wait',
          state,
        );
        this.#pacingLoggedAt = undefined;
        this.#notPacingSince = undefined;
      }
    }
  }

  /**
   * Yields the worker to the other work that is waiting for a time slice,
   * and resolves when it is the caller's turn again. One piece of work runs
   * per turn of the event loop, as when the work of a worker's client groups
   * simply takes turns, except that the hydration of new queries goes first:
   * while both kinds of work are waiting, `HYDRATION_PRIORITY` slices go to
   * hydrations for each one that goes to a batch.
   *
   * Batches are never made to wait for anything but a slice. If no
   * hydration is waiting for one (because none is running, or because those
   * that are running are waiting for I/O), batches get every slice. And
   * however many hydrations there are, batches keep a share of the slices,
   * so they keep moving, and so do the snapshots of the replica that they
   * hold.
   */
  yieldSlice(kind: 'hydration' | 'batch'): Promise<void> {
    return new Promise(resolve => {
      this.#waiting[kind].push(resolve);
      if (!this.#handingOutSlices) {
        this.#handingOutSlices = true;
        setImmediate(this.#nextSlice);
      }
    });
  }

  readonly #nextSlice = () => {
    const {hydration, batch} = this.#waiting;
    let next: (() => void) | undefined;
    if (
      hydration.length > 0 &&
      (batch.length === 0 || this.#hydrationSlices < HYDRATION_PRIORITY)
    ) {
      next = hydration.shift();
      this.#hydrationSlices++;
    } else if (batch.length > 0) {
      next = batch.shift();
      this.#hydrationSlices = 0;
    }
    if (next === undefined) {
      this.#handingOutSlices = false;
      return;
    }
    next();
    // After the work that was just resumed has run its slice.
    setImmediate(this.#nextSlice);
  };
}

/**
 * How many time slices go to the hydration of new queries for each one that
 * goes to a batch, while both are waiting for one.
 */
const HYDRATION_PRIORITY = 9;
