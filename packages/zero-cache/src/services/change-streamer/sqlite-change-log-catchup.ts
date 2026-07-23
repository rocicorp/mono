import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {WatermarkedChange} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import type {Forwarder} from './forwarder.ts';
import {AutoResetSignal} from './schema/tables.ts';
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

/** Guards subscriber registration against canonical-writer purge dispatch. */
export interface SQLiteChangeLogCleanupGuard {
  runWhilePurgeBlocked<T>(register: () => T): Promise<T>;
}

export type SQLiteChangeLogCatchupOptions = {
  batchSize: number;
  barrierTimeoutMs: number;
  barrierPollIntervalMs?: number | undefined;
  cleanupGuard?: SQLiteChangeLogCleanupGuard | undefined;
  onFatal: (error: AutoResetSignal) => Promise<void>;
  sleep?: typeof sleep | undefined;
  now?: (() => number) | undefined;
};

const NOOP_CLEANUP_GUARD: SQLiteChangeLogCleanupGuard = {
  runWhilePurgeBlocked: register => Promise.resolve(register()),
};

/**
 * Coordinates the gap-free transition from replica-local SQLite catchup to
 * the Forwarder's live stream.
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
  readonly #onFatal: (error: AutoResetSignal) => Promise<void>;
  readonly #sleep: typeof sleep;
  readonly #now: () => number;
  readonly #barrierTimeouts = getOrCreateCounter(
    'replication',
    'sqlite_change_log.barrier_timeouts',
    'SQLite change-log catchups that timed out waiting for the required head.',
  );
  readonly #catchups = new Map<Subscriber, AbortController>();
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
    this.#barrierPollIntervalMs = opts.barrierPollIntervalMs ?? 10;
    this.#cleanupGuard = opts.cleanupGuard ?? NOOP_CLEANUP_GUARD;
    this.#onFatal = opts.onFatal;
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
    mode: ReplicatorMode,
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

    void this.#run(
      subscriber,
      mode,
      requiredHead as string | Promise<string>,
      abort,
    );
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
    mode: ReplicatorMode,
    requiredHead: string | Promise<string>,
    abort: AbortController,
  ): Promise<void> {
    const {signal} = abort;
    const deadline = this.#now() + this.#barrierTimeoutMs;
    try {
      const required = await this.#awaitRequiredHead(
        requiredHead,
        deadline,
        signal,
      );
      const plan = await this.#waitForPlan(
        subscriber.watermark,
        required,
        deadline,
        signal,
      );
      this.#throwIfAborted(signal);

      if (plan.kind === 'too-old') {
        const message =
          `earliest supported watermark is ${plan.minWatermark} ` +
          `(requested ${subscriber.watermark})`;
        if (mode === 'backup') {
          throw new AutoResetSignal(
            `backup replica at watermark ${subscriber.watermark} is behind ` +
              `SQLite change log: ${plan.minWatermark}`,
          );
        }
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
      this.#lc.error?.(
        `error while catching up subscriber ${subscriber.id} from SQLite`,
        error,
      );
      if (error instanceof SQLiteChangeLogBarrierTimeoutError) {
        this.#barrierTimeouts.add(1);
      }
      if (error instanceof AutoResetSignal) {
        await this.#onFatal(error);
      }
      subscriber.fail(error);
    } finally {
      if (this.#catchups.get(subscriber) === abort) {
        this.#catchups.delete(subscriber);
      }
    }
  }

  async #awaitRequiredHead(
    requiredHead: string | Promise<string>,
    deadline: number,
    signal: AbortSignal,
  ): Promise<string> {
    if (typeof requiredHead === 'string') {
      return requiredHead;
    }
    while (true) {
      this.#throwIfAborted(signal);
      const remaining = deadline - this.#now();
      if (remaining <= 0) {
        throw new SQLiteChangeLogBarrierTimeoutError(
          'timed out waiting for the forwarded transaction to finish',
        );
      }
      const result = await Promise.race([
        requiredHead.then(value => ({kind: 'required', value}) as const),
        this.#sleep(
          Math.min(this.#barrierPollIntervalMs, remaining),
          signal,
        ).then(() => ({kind: 'pending'}) as const),
      ]);
      if (result.kind === 'required') {
        return result.value;
      }
    }
  }

  async #waitForPlan(
    fromWatermark: string,
    requiredHead: string,
    deadline: number,
    signal: AbortSignal,
  ): Promise<CatchupPlan> {
    while (true) {
      this.#throwIfAborted(signal);
      const plan = this.#reader.plan(fromWatermark);
      if (plan.headWatermark >= requiredHead) {
        return plan;
      }
      const remaining = deadline - this.#now();
      if (remaining <= 0) {
        throw new SQLiteChangeLogBarrierTimeoutError(
          `timed out waiting for SQLite head ${plan.headWatermark} to reach ` +
            `required head ${requiredHead}`,
        );
      }
      await this.#sleep(
        Math.min(this.#barrierPollIntervalMs, remaining),
        signal,
      );
    }
  }

  #throwIfAborted(signal: AbortSignal): void {
    if (this.#closed || signal.aborted) {
      throw new AbortError('SQLite change-log catchup aborted');
    }
  }
}

export class SQLiteChangeLogBarrierTimeoutError extends Error {
  readonly name = 'SQLiteChangeLogBarrierTimeoutError';
}
