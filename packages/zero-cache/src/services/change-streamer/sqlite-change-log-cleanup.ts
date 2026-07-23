import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {SQLiteChangeLogMaintenance} from '../replicator/sqlite-change-log-maintenance.ts';
import type {SQLiteChangeLogPurgeResult} from '../replicator/sqlite-change-log-purger.ts';
import type {SQLiteChangeLogCleanupGuard} from './sqlite-change-log-catchup.ts';

export type SQLiteChangeLogCleanupOptions = {
  readonly retentionMs: number;
  readonly maxRows: number;
  readonly request: (
    maintenance: SQLiteChangeLogMaintenance,
  ) => Promise<SQLiteChangeLogPurgeResult>;
  readonly getAcks: () => ReadonlySet<string>;
  readonly getHead: () => string | undefined;
  readonly retryDelayMs?: number | undefined;
  readonly idleDrainIntervalMs?: number | undefined;
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
};

const DEFAULT_RETRY_DELAY_MS = 1000;
const MAX_IDLE_DRAIN_INTERVAL_MS = 30_000;

/**
 * Owns policy and coordination for replica-local cleanup. It never opens the
 * SQLite file: each batch is dispatched to the canonical replicator process.
 */
export class SQLiteChangeLogCleanupCoordinator implements SQLiteChangeLogCleanupGuard {
  readonly #lc: LogContext;
  readonly #retentionMs: number;
  readonly #maxRows: number;
  readonly #request: SQLiteChangeLogCleanupOptions['request'];
  readonly #getAcks: SQLiteChangeLogCleanupOptions['getAcks'];
  readonly #getHead: SQLiteChangeLogCleanupOptions['getHead'];
  readonly #retryDelayMs: number;
  readonly #idleDrainIntervalMs: number;
  readonly #now: () => number;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #reservationCounts = new Map<string, number>();

  readonly #purges = getOrCreateCounter(
    'replication',
    'sqlite_change_log.purges',
    'SQLite change-log purge batches dispatched to the canonical writer.',
  );
  readonly #purgeLatency = getOrCreateLatencyHistogram(
    'replication',
    'sqlite_change_log.purge_duration',
    'Time spent waiting for a canonical-writer SQLite purge batch.',
  );

  #anonymousBlocks = 0;
  #verifiedWatermark: string | undefined;
  #inFlight: Promise<SQLiteChangeLogPurgeResult> | undefined;
  #afterInFlightDelayMs: number | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #timerDelayMs: number | undefined;
  #closed = false;

  constructor(lc: LogContext, opts: SQLiteChangeLogCleanupOptions) {
    assertPositiveSafeInteger(opts.retentionMs, 'retention');
    assertPositiveSafeInteger(opts.maxRows, 'batch size');
    this.#lc = lc.withContext('component', 'sqlite-change-log-cleanup');
    this.#retentionMs = opts.retentionMs;
    this.#maxRows = opts.maxRows;
    this.#request = opts.request;
    this.#getAcks = opts.getAcks;
    this.#getHead = opts.getHead;
    this.#retryDelayMs = opts.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    this.#idleDrainIntervalMs =
      opts.idleDrainIntervalMs ??
      Math.min(opts.retentionMs, MAX_IDLE_DRAIN_INTERVAL_MS);
    assertPositiveSafeInteger(this.#retryDelayMs, 'retry delay');
    assertPositiveSafeInteger(this.#idleDrainIntervalMs, 'idle drain interval');
    this.#now = opts.now ?? Date.now;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;
  }

  scheduleCleanup(verifiedWatermark: string): void {
    assert(
      verifiedWatermark.length > 0,
      'verified SQLite change-log cleanup watermark must not be empty',
    );
    if (
      this.#verifiedWatermark === undefined ||
      verifiedWatermark > this.#verifiedWatermark
    ) {
      this.#verifiedWatermark = verifiedWatermark;
    }
    this.#scheduleNext(0);
  }

  /**
   * Synchronously installs the reservation block, then asynchronously waits
   * for a batch that was already dispatched. Bounds may be read only after
   * the returned promise resolves.
   */
  async pauseForSnapshot(taskID: string): Promise<void> {
    this.#throwIfClosed();
    this.#reservationCounts.set(
      taskID,
      (this.#reservationCounts.get(taskID) ?? 0) + 1,
    );
    await this.#waitForInFlight();
    this.#throwIfClosed();
  }

  resumeAfterSnapshot(taskID: string): void {
    const count = this.#reservationCounts.get(taskID);
    if (count === undefined) {
      return;
    }
    if (count === 1) {
      this.#reservationCounts.delete(taskID);
    } else {
      this.#reservationCounts.set(taskID, count - 1);
    }
    this.#scheduleNext(0);
  }

  async runWhilePurgeBlocked<T>(register: () => T): Promise<T> {
    this.#throwIfClosed();
    this.#anonymousBlocks++;
    try {
      await this.#waitForInFlight();
      this.#throwIfClosed();
      return register();
    } finally {
      this.#anonymousBlocks--;
      this.#scheduleNext(0);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    if (this.#timer !== undefined) {
      this.#clearTimeout(this.#timer);
      this.#timer = undefined;
      this.#timerDelayMs = undefined;
    }
    await this.#waitForInFlight();
  }

  #scheduleNext(delayMs: number): void {
    if (
      this.#closed ||
      this.#verifiedWatermark === undefined ||
      this.#isBlocked()
    ) {
      return;
    }
    if (this.#inFlight !== undefined) {
      this.#afterInFlightDelayMs = Math.min(
        this.#afterInFlightDelayMs ?? Number.POSITIVE_INFINITY,
        delayMs,
      );
      return;
    }
    if (this.#timer !== undefined) {
      if ((this.#timerDelayMs ?? 0) <= delayMs) {
        return;
      }
      this.#clearTimeout(this.#timer);
    }
    this.#timerDelayMs = delayMs;
    this.#timer = this.#setTimeout(() => {
      this.#timer = undefined;
      this.#timerDelayMs = undefined;
      this.#dispatch();
    }, delayMs);
  }

  #dispatch(): void {
    if (this.#closed || this.#isBlocked() || this.#inFlight !== undefined) {
      return;
    }
    const safeFloor = this.#safeFloor();
    if (safeFloor === undefined) {
      this.#scheduleNext(this.#retryDelayMs);
      return;
    }
    const requestTimeMs = this.#now();
    const maintenance: SQLiteChangeLogMaintenance = {
      safeFloor,
      requestTimeMs,
      retentionMs: this.#retentionMs,
      maxRows: this.#maxRows,
    };
    const start = performance.now();
    // Deferring the call one microtask gives #inFlight a value before any
    // synchronous requester exception can escape. A newly installed block
    // sees and waits for this already-dispatched batch.
    const request = Promise.resolve().then(() => this.#request(maintenance));
    this.#inFlight = request;

    void request.then(
      result => {
        this.#purges.add(1, {result: 'success'});
        this.#purgeLatency.recordMs(performance.now() - start, {
          result: 'success',
        });
        this.#lc.debug?.('purged SQLite change-log batch', result);
        this.#finishDispatch(
          result.moreEligible ? 0 : this.#idleDrainIntervalMs,
        );
      },
      error => {
        this.#purges.add(1, {result: 'error'});
        this.#purgeLatency.recordMs(performance.now() - start, {
          result: 'error',
        });
        this.#lc.warn?.(
          'error routing SQLite change-log purge; retrying later',
          error,
        );
        this.#finishDispatch(this.#retryDelayMs);
      },
    );
  }

  #finishDispatch(nextDelayMs: number): void {
    const afterInFlightDelayMs = this.#afterInFlightDelayMs;
    this.#afterInFlightDelayMs = undefined;
    this.#inFlight = undefined;
    this.#scheduleNext(
      afterInFlightDelayMs === undefined
        ? nextDelayMs
        : Math.min(afterInFlightDelayMs, nextDelayMs),
    );
  }

  #safeFloor(): string | undefined {
    const verified = this.#verifiedWatermark;
    const head = this.#getHead();
    if (verified === undefined || head === undefined) {
      return undefined;
    }
    let floor = verified < head ? verified : head;
    for (const ack of this.#getAcks()) {
      if (ack < floor) {
        floor = ack;
      }
    }
    return floor;
  }

  #isBlocked(): boolean {
    return this.#anonymousBlocks > 0 || this.#reservationCounts.size > 0;
  }

  async #waitForInFlight(): Promise<void> {
    try {
      await this.#inFlight;
    } catch {
      // The dispatch path records and schedules failures. A pause only needs
      // to know that the writer-side attempt has finished.
    }
  }

  #throwIfClosed(): void {
    if (this.#closed) {
      throw new AbortError('SQLite change-log cleanup coordinator is closed');
    }
  }
}

function assertPositiveSafeInteger(value: number, name: string): void {
  assert(
    Number.isSafeInteger(value) && value > 0,
    `SQLite change-log cleanup ${name} must be a positive safe integer`,
  );
}
