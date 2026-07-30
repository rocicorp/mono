import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
  getOrCreateValueHistogram,
} from '../../observability/metrics.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  readChangeLogMeta,
} from '../replicator/change-log-db.ts';
import {SQLiteChangeLogPurger} from '../replicator/sqlite-change-log-purger.ts';
import type {SQLiteChangeLogCleanupGuard} from './sqlite-change-log-catchup.ts';
import {SQLITE_CHANGE_LOG_PLAN_SQL} from './sqlite-change-log-reader.ts';

/**
 * The duration target for any one purge statement, enforced by the purger's
 * measured-budget feedback. Purge runs on the writer's own
 * connection, in the stream loop's process, so a statement's duration stalls
 * the appending path — and the fan-out behind it — directly. The bench's
 * 100 ms bound was derived for the replicator's write worker; this budget is
 * measured against an event loop that also owes progress to fan-out, catchup,
 * and websocket keepalives, so it is deliberately tighter.
 */
const DEFAULT_STATEMENT_TARGET_MS = 20;

/**
 * Batches per cycle. This bounds one cycle's bookkeeping, not the drain:
 * a cycle that hits the limit with history still eligible schedules its own
 * continuation immediately.
 */
const DEFAULT_MAX_BATCHES_PER_CYCLE = 100;

/**
 * How long to wait before re-checking a log that is drained down to history
 * that is not old enough to purge yet, or whose writer has not opened the
 * file. This is what lets a quiet source still drain eligible history: the
 * retention cutoff advances with the clock even when no new cleanup watermark
 * arrives to dispatch a cycle.
 */
const DEFAULT_RECHECK_INTERVAL_MS = 30_000;

/**
 * The backstop poll while waiting for the writer's open transaction to
 * commit. The commit notification normally arrives first; this covers an
 * interrupted stream, whose rollback does not notify.
 */
const DEFAULT_IN_TRANSACTION_POLL_MS = 1_000;

/** Why a cycle stopped before draining the eligible set. */
export type PurgeStopReason =
  // The scheduler was stopped.
  | 'stopped'
  // A snapshot reservation is open.
  | 'paused'
  // The writer has not opened the change log (the file does not exist yet),
  // or has failed soft and deleted it.
  | 'writer-unavailable'
  // No subscriber is connected to confirm the floor. Matches the PG arm's
  // "No subscribers to confirm cleanup" bail.
  | 'no-subscribers'
  // A connected subscriber's ACK is behind the floor. Matches the PG arm's
  // "behind backup" decline, re-checked before every batch so a registration
  // that lands mid-drain is honored by the very next batch.
  | 'subscriber-behind'
  // The per-cycle batch limit was reached with history still eligible.
  | 'batch-limit'
  // The cycle failed; the error was logged and counted.
  | 'error';

/**
 * The invariant-14 probe's classification of `plan(floor)` after a cycle.
 *
 * `too-old` at the floor is not by itself a violation: a log that never held
 * that history — routine at every fork, restore, and rolling restart — returns
 * `too-old` without having purged anything. Only the seed point distinguishes
 * the two, which is why `seedWatermark` is in the v2 meta row:
 *
 * ```
 * violation  ⇔  plan(floor) is `too-old`  AND  seedWatermark <= floor
 * ```
 */
export type FloorProbeOutcome =
  // plan(floor) is `range`: the transaction at the floor survives as a
  // restoring follower's catchup boundary.
  | 'retained'
  // The floor is above the log's head, i.e. the log is stale/frozen and the
  // head cap is what held retention. Nothing at the floor was ever purged.
  | 'ahead'
  // plan(floor) is `too-old` but the log was seeded above the floor: it never
  // held that history. Charted, and feeds the fork/resumption reach
  // measurement; not an alert.
  | 'cold-log'
  // plan(floor) is `too-old` and the log demonstrably once held the floor.
  // This is an invariant-14 violation: the only copy a restoring follower
  // could catch up from was purged.
  | 'violation';

export type PurgeCycleResult = {
  /** The external floor the cycle last used. */
  readonly floor: string;
  readonly batches: number;
  readonly deletedRows: number;
  /** Why the cycle ended before draining, if it did. */
  readonly stopped: PurgeStopReason | undefined;
  /** The invariant-14 probe's outcome, when the log was available to probe. */
  readonly probe: FloorProbeOutcome | undefined;
};

export type SQLiteChangeLogPurgeSchedulerOptions = {
  /**
   * `sqliteChangeLogRetentionMs`: the minimum retention window, ANDed on top
   * of the external floor. It can only make purge more conservative — the
   * purger never accepts it as a standalone bound.
   */
  readonly retentionMs: number;
  /**
   * `sqliteChangeLogPurgeBatchRows`: the starting chunk size per batch. Not
   * the bound — the statement duration target is.
   */
  readonly batchRows: number;
  readonly statementTargetMs?: number | undefined;
  readonly maxBatchesPerCycle?: number | undefined;
  readonly recheckIntervalMs?: number | undefined;
  readonly inTransactionPollMs?: number | undefined;
  /** The clock the retention cutoff is measured against. Defaults to `Date.now`. */
  readonly now?: (() => number) | undefined;
  readonly setTimeoutFn?: typeof setTimeout | undefined;
  readonly clearTimeoutFn?: typeof clearTimeout | undefined;
  /**
   * Awaited between batches so fan-out and catchup make progress. Defaults to
   * a macrotask (`setImmediate`), so pending I/O runs before the next batch.
   */
  readonly yieldFn?: (() => Promise<void>) | undefined;
};

type ConnectionCache = {
  readonly db: Database;
  readonly purger: SQLiteChangeLogPurger;
  readonly plan: Statement;
  readonly belowFloorRemains: Statement;
};

type PlanRow = {
  readonly headWatermark: string | null;
  readonly minWatermark: string | null;
  readonly boundaryExists: number;
};

/**
 * Whether any row below both the floor and the head remains. Deliberately not
 * the purger's eligibility query: rows younger than the retention cutoff are
 * not eligible *yet*, but they will be, and their presence is what keeps the
 * recheck timer armed on a quiet source.
 */
const BELOW_FLOOR_REMAINS_SQL = /*sql*/ `
  SELECT EXISTS (
    SELECT 1 FROM "${CHANGE_LOG_STREAM_TABLE}"
    WHERE "watermark" < @floor
      AND "watermark" < (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
  ) AS "remains"
`;

/**
 * Schedules SQLite change-log purges from the change-streamer's own process,
 * on the writer's own connection.
 *
 * The change-streamer dispatches a cycle from the same `#purgeOldChanges`
 * pass that purges the PG change log, with the same floor, after the same
 * decline gate — so the two logs cannot diverge in what they retain. Within a
 * cycle the scheduler drains bounded batches, re-checking the gate before
 * each one and releasing the cleanup guard between them, so a catchup
 * registration or a snapshot reservation never waits longer than one batch.
 *
 * Purge windows exist only between the writer's commits: purge shares the
 * writer's connection, so it cannot run inside the writer's open transaction,
 * and a batch that finds one waits for the commit notification instead.
 *
 * The scheduler also re-arms itself: a cycle that hits its batch limit
 * continues immediately, and a drained log that still holds history younger
 * than the retention cutoff is re-checked on a timer — which is what lets a
 * quiet source, whose backup watermark stops advancing, still drain eligible
 * history.
 */
export class SQLiteChangeLogPurgeScheduler {
  readonly #lc: LogContext;
  readonly #connection: () => Database | undefined;
  readonly #subscriberAcks: () => Iterable<string>;
  readonly #opts: SQLiteChangeLogPurgeSchedulerOptions;
  readonly #now: () => number;
  readonly #setTimeoutFn: typeof setTimeout;
  readonly #clearTimeoutFn: typeof clearTimeout;
  readonly #yield: () => Promise<void>;

  /**
   * The mutex that serializes purge batches with catchup registration and
   * reservation pauses. It is only ever held across a synchronous section —
   * one batch, or one registration — never across a yield, which is what
   * bounds any waiter to a single batch's duration.
   */
  readonly #lock = new Lock();

  /**
   * Open snapshot reservations, reference-counted by taskID. Purge pauses
   * while any is open. Counted rather than a set so that a reservation retry
   * -- whose cancel-then-recreate can resolve its old resume after its new
   * suspend -- still nets out.
   */
  readonly #pausedBy = new Map<string, number>();

  /** Waiters for the writer's next commit (or rollback). */
  readonly #writerIdleWaiters = new Set<() => void>();

  readonly #purgedRows = getOrCreateCounter(
    'replica',
    'sqlite_change_log.purged_rows',
    'Rows removed from the SQLite change log by the purge scheduler.',
  );
  readonly #cycleDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.purge_cycle_duration',
    'Duration of one SQLite change-log purge cycle, including the yields ' +
      'between its batches.',
  );
  readonly #cycleBatches = getOrCreateValueHistogram(
    'replica',
    'sqlite_change_log.purge_cycle_batches',
    {
      description: 'Purge batches run per SQLite change-log purge cycle.',
      unit: '{batch}',
      bucketBoundaries: [1, 2, 5, 10, 25, 50, 100, 250],
    },
  );
  readonly #declines = getOrCreateCounter(
    'replica',
    'sqlite_change_log.purge_declined',
    'SQLite change-log purge cycles that ended before draining, by reason.',
  );
  readonly #floorProbes = getOrCreateCounter(
    'replica',
    'sqlite_change_log.purge_floor_probe',
    'Invariant-14 probe outcomes: whether the transaction at the purge ' +
      'floor is still servable after each cycle. "violation" means history ' +
      'at or above the floor was purged and alerts; "cold-log" means the ' +
      'log never held it, which is routine at every fork and resumption.',
  );

  /** The highest floor any cycle has been dispatched with. Floors only rise. */
  #pendingFloor: string | undefined;
  #running: Promise<PurgeCycleResult> | undefined;
  #recheckTimer: ReturnType<typeof setTimeout> | undefined;
  #cache: ConnectionCache | undefined;
  #stopped = false;

  /**
   * The guard that brackets catchup registration: required-head capture and
   * `Forwarder.add()` run under the same mutex as purge batches, so the
   * registered ACK is visible to `Forwarder.getAcks()` before the next batch
   * evaluates its gate.
   */
  readonly cleanupGuard: SQLiteChangeLogCleanupGuard = {
    runWhilePurgeBlocked: register => this.#lock.withLock(register),
  };

  /**
   * @param connection the writer's own connection (§3.3). `undefined` until
   *     the writer's first reconcile creates the file, and again after the
   *     writer fails soft and deletes it — both of which skip the cycle
   *     rather than fail it, so the scheduler picks a late-appearing file up
   *     without a restart.
   * @param subscriberAcks the ACKs `#purgeOldChanges` gates the PG purge on,
   *     re-read before every batch.
   */
  constructor(
    lc: LogContext,
    connection: () => Database | undefined,
    subscriberAcks: () => Iterable<string>,
    opts: SQLiteChangeLogPurgeSchedulerOptions,
  ) {
    assert(
      Number.isSafeInteger(opts.retentionMs) && opts.retentionMs > 0,
      'sqliteChangeLogRetentionMs must be a positive integer',
    );
    assert(
      Number.isSafeInteger(opts.batchRows) && opts.batchRows > 0,
      'sqliteChangeLogPurgeBatchRows must be a positive integer',
    );
    this.#lc = lc.withContext('component', 'sqlite-change-log-purge');
    this.#connection = connection;
    this.#subscriberAcks = subscriberAcks;
    this.#opts = opts;
    this.#now = opts.now ?? Date.now;
    this.#setTimeoutFn = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeoutFn = opts.clearTimeoutFn ?? clearTimeout;
    this.#yield =
      opts.yieldFn ??
      (() => new Promise<void>(resolve => setImmediate(resolve)));
  }

  /**
   * Dispatches a purge cycle for `floor` — the same floor, in the same cycle,
   * behind the same decline gate as the PG purge. A cycle already running
   * coalesces: the floor is raised for its next batch and its completion is
   * returned.
   *
   * Never rejects; a failed cycle is logged and reported as `stopped: 'error'`.
   */
  purge(floor: string): Promise<PurgeCycleResult> {
    this.#pendingFloor =
      this.#pendingFloor === undefined || floor > this.#pendingFloor
        ? floor
        : this.#pendingFloor;
    if (this.#stopped) {
      return Promise.resolve(this.#skipped(floor, 'stopped'));
    }
    this.#clearRecheckTimer();
    return this.#running ?? this.#dispatch();
  }

  /**
   * Pauses purging while `taskID` holds a snapshot reservation. The returned
   * promise resolves once no purge batch is in flight, i.e. once the bounds a
   * backup monitor is about to advertise cannot be invalidated by a batch
   * that had already been dispatched.
   */
  pause(taskID: string): Promise<void> {
    this.#pausedBy.set(taskID, (this.#pausedBy.get(taskID) ?? 0) + 1);
    this.#clearRecheckTimer();
    return this.#lock.withLock(() => {});
  }

  /**
   * Ends one of `taskID`'s pauses. Purging resumes only when every
   * reservation has ended, picking up any drain the pauses interrupted.
   * Unknown `taskID`s are ignored.
   */
  resume(taskID: string): void {
    const count = this.#pausedBy.get(taskID);
    if (count === undefined) {
      return;
    }
    if (count > 1) {
      this.#pausedBy.set(taskID, count - 1);
      return;
    }
    this.#pausedBy.delete(taskID);
    if (
      this.#pausedBy.size === 0 &&
      !this.#stopped &&
      this.#pendingFloor !== undefined
    ) {
      this.#scheduleRecheck(0);
    }
  }

  /**
   * Notes that the writer's open transaction has ended, waking any batch
   * waiting for a purge window. A missed notification costs one poll
   * interval, never correctness.
   */
  onWriterIdle(): void {
    for (const waiter of this.#writerIdleWaiters) {
      waiter();
    }
    this.#writerIdleWaiters.clear();
  }

  stop(): void {
    this.#stopped = true;
    this.#clearRecheckTimer();
    this.onWriterIdle();
  }

  #dispatch(): Promise<PurgeCycleResult> {
    // One promise per cycle, stored and returned, so a dispatch that lands
    // while a cycle is running coalesces into the identical completion.
    const running = (async () => {
      let result: PurgeCycleResult;
      try {
        result = await this.#runCycle();
      } catch (e) {
        const floor = this.#pendingFloor;
        assert(floor !== undefined, 'a cycle cannot run without a floor');
        this.#lc.warn?.('error purging the SQLite change log', e);
        this.#declines.add(1, {reason: 'error'});
        result = this.#skipped(floor, 'error');
      }
      try {
        this.#finishCycle(result);
      } catch (e) {
        this.#lc.warn?.('error re-arming the SQLite change-log purge', e);
      }
      return result;
    })().finally(() => {
      this.#running = undefined;
    });
    this.#running = running;
    return running;
  }

  async #runCycle(): Promise<PurgeCycleResult> {
    const startedAt = performance.now();
    assert(
      this.#pendingFloor !== undefined,
      'a cycle cannot run without a floor',
    );
    let floor: string = this.#pendingFloor;
    let batches = 0;
    let deletedRows = 0;
    let stopped: PurgeStopReason | undefined;
    let db: Database | undefined;

    try {
      for (;;) {
        // The floor is re-read each batch: a coalesced dispatch may have raised
        // it, and the gate below is evaluated against what is actually used.
        floor = this.#pendingFloor ?? floor;
        if (this.#stopped) {
          stopped = 'stopped';
          break;
        }
        if (this.#pausedBy.size > 0) {
          stopped = 'paused';
          break;
        }
        const connection = this.#connection();
        if (connection === undefined) {
          db = undefined;
          this.#lc.debug?.(
            'skipping SQLite change-log purge: the writer has not opened the log',
          );
          stopped = 'writer-unavailable';
          break;
        }
        db = connection;
        if (
          batches >=
          Math.max(
            1,
            this.#opts.maxBatchesPerCycle ?? DEFAULT_MAX_BATCHES_PER_CYCLE,
          )
        ) {
          stopped = 'batch-limit';
          break;
        }
        if (connection.inTransaction) {
          // Purge windows exist only between the writer's commits (§3.3).
          await this.#awaitWriterIdle();
          continue;
        }

        const batchFloor = floor;
        const outcome = await this.#lock.withLock(() => {
          // Everything the wait above established is re-checked under the lock:
          // the writer may have begun another transaction or failed soft while
          // this batch's acquisition was queued behind a registration.
          if (this.#connection() !== connection || connection.inTransaction) {
            return {kind: 'retry'} as const;
          }
          const declined = this.#gate(batchFloor);
          if (declined !== undefined) {
            return {kind: 'declined', reason: declined} as const;
          }
          const result = this.#cacheFor(connection).purger.purgeBatch({
            externalFloor: batchFloor,
            retentionCutoffMs: this.#now() - this.#opts.retentionMs,
            maxRows: this.#opts.batchRows,
            maxDurationMs:
              this.#opts.statementTargetMs ?? DEFAULT_STATEMENT_TARGET_MS,
          });
          return {kind: 'purged', result} as const;
        });

        if (outcome.kind === 'retry') {
          continue;
        }
        if (outcome.kind === 'declined') {
          stopped = outcome.reason;
          break;
        }
        batches++;
        deletedRows += outcome.result.deletedRows;
        if (!outcome.result.moreEligible) {
          break;
        }
        // Release the loop — and the guard, released above — so fan-out,
        // catchup, and registrations make progress between batches.
        await this.#yield();
      }
    } catch (e) {
      // Earlier batches committed independently and must remain visible in the
      // result and metrics. Finalize the partial cycle, including its floor
      // probe, rather than replacing it with an empty skipped result.
      this.#lc.warn?.('error purging the SQLite change log', e);
      stopped = 'error';
    }

    const probe = db === undefined ? undefined : this.#probeFloor(db, floor);
    if (deletedRows > 0) {
      this.#purgedRows.add(deletedRows);
    }
    if (batches > 0) {
      this.#cycleBatches.record(batches);
      this.#cycleDuration.recordMs(performance.now() - startedAt);
    }
    if (stopped !== undefined) {
      this.#declines.add(1, {reason: stopped});
    }
    if (batches > 0 || stopped !== 'writer-unavailable') {
      this.#lc.debug?.('SQLite change-log purge cycle', {
        sqliteChangeLogPurge: {floor, batches, deletedRows, stopped, probe},
      });
    }
    return {floor, batches, deletedRows, stopped, probe};
  }

  /**
   * Re-arms the scheduler after a cycle. A cycle interrupted by its batch
   * limit continues immediately; a decline or an unopened log is re-checked
   * later (a decline lifts when the lagging subscriber ACKs past the floor,
   * which nothing notifies); and a fully drained log is re-checked as long as
   * it retains history below the floor, which the advancing retention cutoff
   * will eventually release.
   */
  #finishCycle(result: PurgeCycleResult): void {
    if (this.#stopped || this.#pausedBy.size > 0) {
      return; // resume() re-arms.
    }
    switch (result.stopped) {
      case 'batch-limit':
        this.#scheduleRecheck(0);
        break;
      case 'stopped':
      case 'paused':
        break;
      case 'no-subscribers':
      case 'subscriber-behind':
      case 'writer-unavailable':
      case 'error':
        this.#scheduleRecheck(
          this.#opts.recheckIntervalMs ?? DEFAULT_RECHECK_INTERVAL_MS,
        );
        break;
      case undefined: {
        // A read is safe even inside the writer's open transaction; it just
        // observes the uncommitted head, which is never below the floor.
        const db = this.#connection();
        if (db !== undefined) {
          const {remains} = this.#cacheFor(db).belowFloorRemains.get<{
            remains: number;
          }>({floor: result.floor});
          if (remains === 0) {
            break;
          }
        }
        this.#scheduleRecheck(
          this.#opts.recheckIntervalMs ?? DEFAULT_RECHECK_INTERVAL_MS,
        );
        break;
      }
    }
  }

  /**
   * The decline gate, identical to the PG arm's: no purge unless at least one
   * subscriber is connected and none is behind the floor. It is a gate, not a
   * lower floor — more conservative than a `min()` over the ACKs.
   */
  #gate(floor: string): 'no-subscribers' | 'subscriber-behind' | undefined {
    let subscribers = 0;
    for (const ack of this.#subscriberAcks()) {
      subscribers++;
      if (ack < floor) {
        return 'subscriber-behind';
      }
    }
    return subscribers === 0 ? 'no-subscribers' : undefined;
  }

  /**
   * The live check on invariant 14 — what makes running this slice dark worth
   * anything: in `write` mode nothing reads the log, so a wrong floor would
   * otherwise stay invisible until the canary rollout two slices later.
   */
  #probeFloor(db: Database, floor: string): FloorProbeOutcome | undefined {
    try {
      const cache = this.#cacheFor(db);
      const row = cache.plan.get<PlanRow>({fromWatermark: floor});
      const {headWatermark, minWatermark} = row;
      if (headWatermark === null || minWatermark === null) {
        return undefined;
      }
      let outcome: FloorProbeOutcome;
      if (floor > headWatermark) {
        outcome = 'ahead';
      } else if (floor < minWatermark || row.boundaryExists === 0) {
        const {seedWatermark} = readChangeLogMeta(db);
        outcome = seedWatermark <= floor ? 'violation' : 'cold-log';
      } else {
        outcome = 'retained';
      }
      this.#floorProbes.add(1, {outcome});
      if (outcome === 'violation') {
        this.#lc.error?.(
          'SQLite change log purged history at or above the purge floor. A ' +
            'follower restoring to that watermark would get `too-old` and ' +
            'take a full restore.',
          {sqliteChangeLogFloorProbe: {floor, ...row}},
        );
      }
      return outcome;
    } catch (e) {
      this.#lc.warn?.('unable to probe the SQLite change-log purge floor', e);
      return undefined;
    }
  }

  /** Resolves once the writer's open transaction has committed or rolled back. */
  #awaitWriterIdle(): Promise<void> {
    return new Promise<void>(resolve => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const waiter = () => {
        if (timer !== undefined) {
          this.#clearTimeoutFn(timer);
        }
        this.#writerIdleWaiters.delete(waiter);
        resolve();
      };
      this.#writerIdleWaiters.add(waiter);
      timer = this.#setTimeoutFn(
        waiter,
        this.#opts.inTransactionPollMs ?? DEFAULT_IN_TRANSACTION_POLL_MS,
      );
    });
  }

  #scheduleRecheck(delayMs: number): void {
    if (this.#stopped || this.#recheckTimer !== undefined) {
      return;
    }
    this.#recheckTimer = this.#setTimeoutFn(() => {
      this.#recheckTimer = undefined;
      if (
        this.#stopped ||
        this.#running !== undefined ||
        this.#pendingFloor === undefined ||
        this.#pausedBy.size > 0
      ) {
        return;
      }
      void this.#dispatch();
    }, delayMs);
  }

  #clearRecheckTimer(): void {
    if (this.#recheckTimer !== undefined) {
      this.#clearTimeoutFn(this.#recheckTimer);
      this.#recheckTimer = undefined;
    }
  }

  /**
   * The per-connection statements, rebuilt if the writer's connection ever
   * changes. The purger's prepared statements survive a reseed — SQLite
   * re-prepares on schema change — but not a different `Database`.
   */
  #cacheFor(db: Database): ConnectionCache {
    if (this.#cache?.db !== db) {
      this.#cache = {
        db,
        purger: new SQLiteChangeLogPurger(db),
        plan: db.prepare(SQLITE_CHANGE_LOG_PLAN_SQL),
        belowFloorRemains: db.prepare(BELOW_FLOOR_REMAINS_SQL),
      };
    }
    return this.#cache;
  }

  #skipped(floor: string, reason: PurgeStopReason): PurgeCycleResult {
    return {
      floor,
      batches: 0,
      deletedRows: 0,
      stopped: reason,
      probe: undefined,
    };
  }
}
