import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import type {Forwarder} from './forwarder.ts';
import type {CatchupPlan} from './sqlite-change-log-reader.ts';
import type {Subscriber} from './subscriber.ts';

export interface SQLiteChangeLogCatchupReader {
  plan(fromWatermark: string): CatchupPlan;
  read(
    fromWatermark: string,
    throughWatermark: string,
    batchSize: number,
    signal?: AbortSignal,
  ): AsyncIterable<readonly WatermarkedChange[]>;
  close(): void;
}

/**
 * Slice 9 replaces this no-op implementation with a guard that waits for an
 * in-flight purge and blocks new purge dispatch while the subscriber's ACK is
 * made visible to the Forwarder.
 */
export interface SQLiteChangeLogCleanupGuard {
  runWhilePurgeBlocked<T>(register: () => T): Promise<T>;
}

// How long the barrier waits for the change-log writer's commit notification
// before re-reading the log itself. The notification normally arrives first, so
// this is a backstop for the windows in which none is coming: the writer is
// disabled, or it is between stream connections.
const DEFAULT_BARRIER_POLL_INTERVAL_MS = 1000;

export type SQLiteChangeLogCatchupOptions = {
  batchSize: number;
  barrierTimeoutMs: number;
  barrierPollIntervalMs?: number | undefined;
  cleanupGuard?: SQLiteChangeLogCleanupGuard | undefined;
  sleep?: typeof sleep | undefined;
  now?: (() => number) | undefined;
};

const NOOP_CLEANUP_GUARD: SQLiteChangeLogCleanupGuard = {
  runWhilePurgeBlocked: register => Promise.resolve(register()),
};

/**
 * Coordinates a serving subscriber's gap-free transition from replica-local
 * SQLite catchup to the Forwarder's live stream.
 *
 * Registration and required-head capture happen in one synchronous callback.
 * The subscriber therefore either sees a transaction in SQLite catchup or in
 * its live backlog (duplicates across that boundary are filtered by
 * Subscriber), never in neither place.
 */
export class SQLiteChangeLogCatchup implements Disposable {
  readonly #lc: LogContext;
  readonly #forwarder: Forwarder;
  readonly #reader: SQLiteChangeLogCatchupReader;
  readonly #batchSize: number;
  readonly #barrierTimeoutMs: number;
  readonly #barrierPollIntervalMs: number;
  readonly #cleanupGuard: SQLiteChangeLogCleanupGuard;
  readonly #sleep: typeof sleep;
  readonly #now: () => number;
  readonly #barrierTimeouts = getOrCreateCounter(
    'replication',
    'sqlite_change_log.barrier_timeouts',
    'SQLite change-log catchups that timed out waiting for the required head.',
  );
  readonly #barrierBacklogOverflows = getOrCreateCounter(
    'replication',
    'sqlite_change_log.barrier_backlog_overflows',
    'SQLite change-log catchups abandoned because the subscriber backlog ' +
      'reached its high water mark while waiting for the required head.',
  );
  readonly #barrierWakeups = getOrCreateCounter(
    'replication',
    'sqlite_change_log.barrier_wakeups',
    'SQLite catchup barrier waits, labeled by what ended the wait. A ' +
      'population dominated by "poll" means the change-log writer\'s commit ' +
      'notification is not reaching the barrier.',
  );
  readonly #catchups = new Map<Subscriber, AbortController>();
  readonly #commitWaiters = new Set<CommitWaiter>();
  #closed = false;

  constructor(
    lc: LogContext,
    forwarder: Forwarder,
    reader: SQLiteChangeLogCatchupReader,
    opts: SQLiteChangeLogCatchupOptions,
  ) {
    this.#lc = lc.withContext('component', 'sqlite-change-log-catchup');
    this.#forwarder = forwarder;
    this.#reader = reader;
    this.#batchSize = opts.batchSize;
    this.#barrierTimeoutMs = opts.barrierTimeoutMs;
    this.#barrierPollIntervalMs =
      opts.barrierPollIntervalMs ?? DEFAULT_BARRIER_POLL_INTERVAL_MS;
    this.#cleanupGuard = opts.cleanupGuard ?? NOOP_CLEANUP_GUARD;
    this.#sleep = opts.sleep ?? sleep;
    this.#now = opts.now ?? Date.now;
  }

  /**
   * Registers the subscriber before resolving. Catchup itself continues in
   * the background so subscribe() does not wait for SQLite to reach the
   * required head.
   */
  async catchup(
    subscriber: Subscriber,
    captureRequiredHead: () => string | Promise<string>,
  ): Promise<void> {
    if (this.#closed) {
      subscriber.fail(new AbortError('SQLite change-log catchup is closed'));
      return;
    }

    const abort = new AbortController();
    this.#catchups.set(subscriber, abort);
    let requiredHead: string | Promise<string> | undefined;
    try {
      await this.#cleanupGuard.runWhilePurgeBlocked(() => {
        this.#throwIfAborted(abort.signal);
        requiredHead = captureRequiredHead();
        this.#forwarder.add(subscriber);
      });
    } catch (error) {
      this.#catchups.delete(subscriber);
      if (!abort.signal.aborted) {
        this.#lc.error?.(
          `error while registering SQLite catchup subscriber ${subscriber.id}`,
          error,
        );
        subscriber.fail(error);
      }
      return;
    }

    void this.#run(subscriber, requiredHead as string | Promise<string>, abort);
  }

  /**
   * Notes that the change-log writer has committed `watermark`, i.e. that the
   * log's head has advanced to it. The writer runs in this process, so this is
   * the event itself rather than a subscriber's ACK of it.
   *
   * It is a wakeup, not an authority: `plan()` still decides what can be read.
   * A notification that never arrives costs the barrier a poll interval, and one
   * that arrives early costs an extra `plan()` call.
   */
  onChangeLogCommit(watermark: string): void {
    for (const waiter of this.#commitWaiters) {
      if (watermark >= waiter.watermark) {
        this.#commitWaiters.delete(waiter);
        waiter.resolve();
      }
    }
  }

  remove(subscriber: Subscriber): void {
    this.#catchups.get(subscriber)?.abort();
    this.#catchups.delete(subscriber);
    this.#forwarder.remove(subscriber);
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    for (const [subscriber, abort] of this.#catchups) {
      abort.abort();
      this.#forwarder.remove(subscriber);
    }
    this.#catchups.clear();
    this.#reader.close();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  async #run(
    subscriber: Subscriber,
    requiredHead: string | Promise<string>,
    abort: AbortController,
  ): Promise<void> {
    const {signal} = abort;
    try {
      const requiredHeadStart = this.#now();
      const required = await this.#awaitRequiredHead(requiredHead, signal);
      const requiredHeadWaitMs = this.#now() - requiredHeadStart;
      if (requiredHeadWaitMs > this.#barrierTimeoutMs) {
        this.#lc.info?.(
          `waited ${requiredHeadWaitMs} ms for the forwarded transaction to ` +
            `commit before starting the SQLite barrier for ${subscriber.id}`,
        );
      }
      // The barrier bounds replica lag, not upstream transaction duration, so
      // its deadline starts once the required head is known. Sharing one
      // deadline with the wait above would fail subscribers that registered
      // during a transaction longer than barrierTimeoutMs, however current
      // the replica is.
      const deadline = this.#now() + this.#barrierTimeoutMs;
      const plan = await this.#waitForPlan(
        subscriber,
        required,
        deadline,
        signal,
      );
      this.#throwIfAborted(signal);

      if (plan.kind === 'too-old') {
        const message =
          `earliest supported watermark is ${plan.minWatermark} ` +
          `(requested ${subscriber.watermark})`;
        this.#lc.warn?.(
          `rejecting subscriber at watermark ${subscriber.watermark} ` +
            `(earliest watermark: ${plan.minWatermark})`,
        );
        subscriber.close(ErrorType.WatermarkTooOld, message);
        return;
      }

      let count = 0;
      const start = this.#now();
      if (plan.kind === 'range') {
        let lastBatchConsumed: Promise<unknown> | undefined;
        for await (const changes of this.#reader.read(
          subscriber.watermark,
          plan.headWatermark,
          this.#batchSize,
          signal,
        )) {
          const waitStart = this.#now();
          await lastBatchConsumed;
          const elapsed = this.#now() - waitStart;
          if (lastBatchConsumed) {
            this.#lc[elapsed > 100 ? 'info' : 'debug']?.(
              `waited ${elapsed.toFixed(3)} ms for ${subscriber.id} to consume ` +
                `the previous SQLite catchup batch`,
            );
          }
          this.#throwIfAborted(signal);
          for (const change of changes) {
            lastBatchConsumed = subscriber.catchup(change);
            count++;
          }
        }
        await lastBatchConsumed;
      } else {
        this.#lc.warn?.(
          `subscriber ${subscriber.id} at watermark ${subscriber.watermark} ` +
            `is ahead of the SQLite change-log head ${plan.headWatermark}; ` +
            `waiting for the replica to catch up`,
        );
      }

      this.#throwIfAborted(signal);
      this.#lc.info?.(
        `caught up ${subscriber.id} from SQLite with ${count} changes ` +
          `(${this.#now() - start} ms)`,
      );
      // Keep buffering live sends until the asynchronous backlog drain has
      // established its ordering boundary.
      void subscriber.setCaughtUp();
    } catch (error) {
      if (signal.aborted) {
        return;
      }
      if (error instanceof SQLiteChangeLogBarrierError) {
        if (error instanceof SQLiteChangeLogBarrierTimeoutError) {
          this.#barrierTimeouts.add(1);
        }
        // Giving up on the barrier means the replica has not caught up *yet*,
        // not that it cannot serve this subscriber. End the subscription
        // cleanly so the client reconnects and retries: IncrementalSyncer
        // treats any ['error', ...] message as terminal and restores a fresh
        // replica from litestream, whereas a clean end backs off and
        // re-subscribes. If retries keep failing until the change log is purged
        // past the subscriber's watermark, plan() returns 'too-old', which is
        // terminal by design.
        this.#lc.warn?.(
          `ending subscription for ${subscriber.id} to retry SQLite catchup`,
          error,
        );
        subscriber.close();
        return;
      }
      this.#lc.error?.(
        `error while catching up subscriber ${subscriber.id} from SQLite`,
        error,
      );
      subscriber.fail(error);
    } finally {
      if (this.#catchups.get(subscriber) === abort) {
        this.#catchups.delete(subscriber);
      }
    }
  }

  /**
   * Waits for the transaction that was in flight at registration to finish.
   *
   * This wait is bounded by the upstream transaction rather than by
   * `barrierTimeoutMs`: the completion settles on both commit and rollback,
   * and an interrupted change stream forwards a synthetic rollback, so it
   * always settles. Aborting -- the subscriber disconnecting or the service
   * shutting down -- is what releases it early.
   */
  async #awaitRequiredHead(
    requiredHead: string | Promise<string>,
    signal: AbortSignal,
  ): Promise<string> {
    if (typeof requiredHead === 'string') {
      return requiredHead;
    }
    this.#throwIfAborted(signal);
    let onAbort: (() => void) | undefined;
    try {
      return await Promise.race([
        requiredHead,
        new Promise<never>((_, reject) => {
          onAbort = () =>
            reject(new AbortError('SQLite change-log catchup aborted'));
          signal.addEventListener('abort', onAbort, {once: true});
        }),
      ]);
    } finally {
      if (onAbort) {
        signal.removeEventListener('abort', onAbort);
      }
    }
  }

  async #waitForPlan(
    subscriber: Subscriber,
    requiredHead: string,
    deadline: number,
    signal: AbortSignal,
  ): Promise<Exclude<CatchupPlan, {kind: 'not-ready'}>> {
    // Race backlog growth once around the whole polling loop. Attaching the
    // same unresolved promise to every per-poll race would retain one reaction
    // per iteration until the subscriber's backlog eventually filled.
    const backlogFull = subscriber.whenBacklogFull();
    const barrier = new AbortController();
    const abortBarrier = () => barrier.abort();
    signal.addEventListener('abort', abortBarrier, {once: true});
    if (signal.aborted) {
      barrier.abort();
    }

    let observedHead = subscriber.watermark;
    try {
      const planReady = (async () => {
        while (true) {
          this.#throwIfAborted(barrier.signal);
          const plan = this.#reader.plan(subscriber.watermark);
          // A not-ready log has no head to compare, so it can never satisfy
          // the required head: keep waiting for the writer to create and
          // reconcile it, and let the barrier deadline end the subscription
          // cleanly for a retry. Selection declines a not-ready log before a
          // subscriber gets here, and neither purging nor reconciliation can
          // empty a log that had content, so this is a belt-and-braces path.
          if (plan.kind !== 'not-ready') {
            observedHead = plan.headWatermark;
            if (plan.headWatermark >= requiredHead) {
              return plan;
            }
          }
          const remaining = deadline - this.#now();
          if (remaining <= 0) {
            throw new SQLiteChangeLogBarrierTimeoutError(
              `timed out waiting for SQLite head ${observedHead} to ` +
                `reach required head ${requiredHead}`,
            );
          }
          await this.#awaitChangeLogProgress(
            requiredHead,
            remaining,
            barrier.signal,
          );
        }
      })();

      const backlogOverflow = backlogFull.promise.then(() => {
        this.#throwIfAborted(signal);
        // close() also resolves backlog waiters so none are stranded. The
        // backlog is cleared first, which distinguishes close from overflow.
        if (!subscriber.backlogFull) {
          return new Promise<never>(() => {});
        }
        this.#barrierWakeups.add(1, {'wakeup.source': 'backlog'});
        // Bytes, not time, are what the barrier actually costs: past the high
        // water mark this subscriber's send() no longer resolves, so it holds
        // up every flush and can stall the replica this barrier waits on.
        this.#barrierBacklogOverflows.add(1);
        throw new SQLiteChangeLogBarrierBacklogError(
          `backlog for subscriber ${subscriber.id} reached its high water ` +
            `mark while waiting for SQLite head ${observedHead} to ` +
            `reach required head ${requiredHead}`,
        );
      });

      return await Promise.race([planReady, backlogOverflow]);
    } finally {
      signal.removeEventListener('abort', abortBarrier);
      barrier.abort();
      backlogFull.cancel();
    }
  }

  /**
   * Waits for the change-log writer to commit `requiredHead`, or the poll
   * interval as a backstop.
   *
   * No notification can be missed by registering after `plan()` was read: the
   * notification follows the commit it reports, so if the writer had already
   * committed `requiredHead`, `plan()` would have seen it.
   */
  async #awaitChangeLogProgress(
    requiredHead: string,
    remainingMs: number,
    signal: AbortSignal,
  ): Promise<void> {
    const committed = resolver<void>();
    const waiter = {watermark: requiredHead, resolve: committed.resolve};
    this.#commitWaiters.add(waiter);
    // Scopes the backstop timer to this wait so that a commit cancels it rather
    // than leaving it to fire against a barrier that has already moved on.
    const poll = new AbortController();
    const abortPoll = () => poll.abort();
    signal.addEventListener('abort', abortPoll, {once: true});
    try {
      const wokenBy = await Promise.race([
        committed.promise.then(() => 'commit' as const),
        this.#sleep(
          Math.min(this.#barrierPollIntervalMs, remainingMs),
          poll.signal,
        ).then(() => 'poll' as const),
      ]);
      this.#barrierWakeups.add(1, {'wakeup.source': wokenBy});
    } finally {
      this.#commitWaiters.delete(waiter);
      signal.removeEventListener('abort', abortPoll);
      poll.abort();
    }
  }

  #throwIfAborted(signal: AbortSignal): void {
    if (this.#closed || signal.aborted) {
      throw new AbortError('SQLite change-log catchup aborted');
    }
  }
}

type CommitWaiter = {watermark: string; resolve: () => void};

/**
 * A barrier that gave up on the replica reaching the required head. Both
 * reasons mean "not yet", not "never", so both end the subscription cleanly
 * for a retry rather than failing it.
 */
export class SQLiteChangeLogBarrierError extends Error {}

/** The replica did not reach the required head before the deadline. */
export class SQLiteChangeLogBarrierTimeoutError extends SQLiteChangeLogBarrierError {
  readonly name = 'SQLiteChangeLogBarrierTimeoutError';
}

/** Waiting any longer would have cost more than reconnecting. */
export class SQLiteChangeLogBarrierBacklogError extends SQLiteChangeLogBarrierError {
  readonly name = 'SQLiteChangeLogBarrierBacklogError';
}
