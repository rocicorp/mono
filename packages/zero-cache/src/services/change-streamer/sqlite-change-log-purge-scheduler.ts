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
 * Batches per pass. This bounds one pass's bookkeeping, not the drain: a pass
 * that hits the limit asks its caller for an immediate continuation.
 */
const DEFAULT_MAX_BATCHES_PER_PASS = 100;

/**
 * The backstop poll while waiting for the writer's open transaction to
 * commit. The commit notification normally arrives first; this covers an
 * interrupted stream, whose rollback does not notify.
 */
const DEFAULT_IN_TRANSACTION_POLL_MS = 1_000;

/** Why a pass stopped before draining the eligible set. */
export type PurgeStopReason =
  // The scheduler was stopped.
  | 'stopped'
  // A snapshot reservation is open.
  | 'paused'
  // The writer has not opened the change log (the file does not exist yet),
  // or has failed soft and deleted it.
  | 'writer-unavailable'
  // A connected subscriber's ACK is behind the floor. Re-checked before every
  // batch so a registration that lands mid-drain is honored by the very next
  // batch.
  | 'subscriber-behind'
  // The per-pass batch limit was reached with history still eligible.
  | 'batch-limit'
  // The pass failed; the error was logged and counted.
  | 'error';

/**
 * The invariant-14 probe's classification of `plan(floor)` after a pass.
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

/** When the cleanup coordinator should run another pass. */
export type PurgeContinuation =
  // The pass hit its batch limit while eligible history remained.
  | 'immediate'
  // Progress depends on time or external state: retention, a subscriber ACK,
  // a reservation, the writer becoming available, or a transient error.
  | 'deferred';

export type PurgePassResult = {
  /** The external floor the pass used. */
  readonly floor: string;
  readonly batches: number;
  readonly deletedRows: number;
  /** Why the pass ended before draining, if it did. */
  readonly stopped: PurgeStopReason | undefined;
  /** The invariant-14 probe's outcome, when the log was available to probe. */
  readonly probe: FloorProbeOutcome | undefined;
  /** Whether, and how soon, the cleanup coordinator should run another pass. */
  readonly continuation: PurgeContinuation | undefined;
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
  readonly maxBatchesPerPass?: number | undefined;
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
 * Whether the cleanup coordinator still owes this floor a pass. Deliberately
 * not the purger's eligibility query, in two ways:
 *
 * - `remains`: rows younger than the retention cutoff are not eligible *yet*,
 *   but they will be, and their presence asks the cleanup coordinator for a
 *   deferred pass even on a quiet source.
 * - `headBehindFloor`: a head behind the floor means the writer is expected
 *   to replay commits below it (e.g. after reconnecting while the backup
 *   outran this log). Those rows do not exist to count yet, writer commits
 *   only wake an in-flight waiter, and backup monitors never resend an
 *   unchanged floor — so reporting quiescence here would disarm cleanup for
 *   good. An empty log (no head) defers for the same reason.
 */
const BELOW_FLOOR_REMAINS_SQL = /*sql*/ `
  SELECT
    EXISTS (
      SELECT 1 FROM "${CHANGE_LOG_STREAM_TABLE}"
      WHERE "watermark" < @floor
        AND "watermark" < (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
    ) AS "remains",
    coalesce(
      (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}") < @floor,
      1
    ) AS "headBehindFloor"
`;

/**
 * Runs bounded SQLite change-log purge passes on the writer's own connection.
 *
 * The caller supplies the current durable cleanup floor independently of any
 * other change-log implementation and owns the timing of subsequent passes.
 * Within a pass this class drains bounded batches, re-checking live subscriber
 * ACKs before each one and releasing the cleanup guard between them, so a
 * catchup registration or snapshot reservation never waits longer than one
 * batch.
 *
 * Purge windows exist only between the writer's commits: purge shares the
 * writer's connection, so it cannot run inside the writer's open transaction,
 * and a batch that finds one waits for the commit notification instead.
 *
 * The result tells the cleanup coordinator whether the next pass should be
 * immediate or deferred. This class deliberately owns no cleanup timer: the
 * same level-triggered coordinator continues a large drain, advances the
 * retention clock on a quiet source, and retries changing external state.
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
  readonly #passDuration = getOrCreateLatencyHistogram(
    'replica',
    'sqlite_change_log.purge_cycle_duration',
    'Duration of one SQLite change-log purge pass, including the yields ' +
      'between its batches.',
  );
  readonly #passBatches = getOrCreateValueHistogram(
    'replica',
    'sqlite_change_log.purge_cycle_batches',
    {
      description: 'Purge batches run per SQLite change-log purge pass.',
      unit: '{batch}',
      bucketBoundaries: [1, 2, 5, 10, 25, 50, 100, 250],
    },
  );
  readonly #declines = getOrCreateCounter(
    'replica',
    'sqlite_change_log.purge_declined',
    'SQLite change-log purge passes that ended before draining, by reason.',
  );
  readonly #floorProbes = getOrCreateCounter(
    'replica',
    'sqlite_change_log.purge_floor_probe',
    'Invariant-14 probe outcomes: whether the transaction at the purge ' +
      'floor is still servable after each pass. "violation" means history ' +
      'at or above the floor was purged and alerts; "cold-log" means the ' +
      'log never held it, which is routine at every fork and resumption.',
  );

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
   *     writer fails soft and deletes it — both of which defer the pass rather
   *     than fail it, so a later coordinator pass picks up a late-appearing
   *     file without a restart.
   * @param subscriberAcks live subscriber ACKs, re-read before every batch so
   *     the supplied floor remains safe throughout an asynchronous drain.
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
   * Runs one purge pass for the current durable cleanup `floor`.
   * Never rejects; a failed pass is logged and reported as `stopped: 'error'`.
   */
  async purge(floor: string): Promise<PurgePassResult> {
    if (this.#stopped) {
      return this.#skipped(floor, 'stopped');
    }
    try {
      return await this.#runPass(floor);
    } catch (e) {
      this.#lc.warn?.('error purging the SQLite change log', e);
      this.#declines.add(1, {reason: 'error'});
      return this.#skipped(floor, 'error', 'deferred');
    }
  }

  /**
   * Pauses purging while `taskID` holds a snapshot reservation. The returned
   * promise resolves once no purge batch is in flight, i.e. once the bounds a
   * backup monitor is about to advertise cannot be invalidated by a batch
   * that had already been dispatched.
   */
  pause(taskID: string): Promise<void> {
    this.#pausedBy.set(taskID, (this.#pausedBy.get(taskID) ?? 0) + 1);
    return this.#lock.withLock(() => {});
  }

  /**
   * Ends one of `taskID`'s pauses. A later coordinator pass can purge once
   * every reservation has ended. Unknown `taskID`s are ignored.
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
    this.onWriterIdle();
  }

  async #runPass(floor: string): Promise<PurgePassResult> {
    const startedAt = performance.now();
    let batches = 0;
    let deletedRows = 0;
    let stopped: PurgeStopReason | undefined;
    let db: Database | undefined;

    try {
      for (;;) {
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
            this.#opts.maxBatchesPerPass ?? DEFAULT_MAX_BATCHES_PER_PASS,
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

        const outcome = await this.#lock.withLock(() => {
          // Everything the wait above established is re-checked under the lock:
          // the writer may have begun another transaction or failed soft while
          // this batch's acquisition was queued behind a registration.
          if (this.#connection() !== connection || connection.inTransaction) {
            return {kind: 'retry'} as const;
          }
          const declined = this.#gate(floor);
          if (declined !== undefined) {
            return {kind: 'declined', reason: declined} as const;
          }
          const result = this.#cacheFor(connection).purger.purgeBatch({
            externalFloor: floor,
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
      // result and metrics. Finalize the partial pass, including its floor
      // probe, rather than replacing it with an empty skipped result.
      this.#lc.warn?.('error purging the SQLite change log', e);
      stopped = 'error';
    }

    const probe = db === undefined ? undefined : this.#probeFloor(db, floor);
    let continuation: PurgeContinuation | undefined;
    try {
      continuation = this.#continuation(floor, stopped);
    } catch (e) {
      // A connection can fail soft between the pass and this level check. Keep
      // completed batches visible and ask the coordinator to retry later.
      this.#lc.warn?.(
        'unable to determine whether SQLite change-log purge should continue',
        e,
      );
      continuation = 'deferred';
      stopped ??= 'error';
    }
    if (deletedRows > 0) {
      this.#purgedRows.add(deletedRows);
    }
    if (batches > 0) {
      this.#passBatches.record(batches);
      this.#passDuration.recordMs(performance.now() - startedAt);
    }
    if (stopped !== undefined) {
      this.#declines.add(1, {reason: stopped});
    }
    if (batches > 0 || stopped !== 'writer-unavailable') {
      this.#lc.debug?.('SQLite change-log purge pass', {
        sqliteChangeLogPurge: {
          floor,
          batches,
          deletedRows,
          stopped,
          probe,
          continuation,
        },
      });
    }
    return {floor, batches, deletedRows, stopped, probe, continuation};
  }

  /**
   * Reports whether the level-triggered cleanup coordinator should run again.
   * Rows below the floor but inside the retention window require a deferred
   * pass even when no new backup watermark arrives, and so does a head behind
   * the floor: the rows the writer replays into that gap arrive after this
   * check, with no other event armed to reap them.
   */
  #continuation(
    floor: string,
    stopped: PurgeStopReason | undefined,
  ): PurgeContinuation | undefined {
    switch (stopped) {
      case 'batch-limit':
        return 'immediate';
      case 'stopped':
        return undefined;
      case 'paused':
      case 'subscriber-behind':
      case 'writer-unavailable':
      case 'error':
        return 'deferred';
      case undefined: {
        // A read is safe even inside the writer's open transaction; it just
        // observes the uncommitted head, which is never below the floor.
        const db = this.#connection();
        if (db === undefined) {
          return 'deferred';
        }
        const {remains, headBehindFloor} = this.#cacheFor(
          db,
        ).belowFloorRemains.get<{
          remains: number;
          headBehindFloor: number;
        }>({floor});
        return remains === 0 && headBehindFloor === 0 ? undefined : 'deferred';
      }
    }
  }

  /**
   * No purge while a connected subscriber is behind the supplied floor. The
   * cleanup coordinator has already provided the reconnect grace period; an
   * empty live set places no additional constraint on the durable floor.
   */
  #gate(floor: string): 'subscriber-behind' | undefined {
    for (const ack of this.#subscriberAcks()) {
      if (ack < floor) {
        return 'subscriber-behind';
      }
    }
    return undefined;
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

  #skipped(
    floor: string,
    reason: PurgeStopReason,
    continuation?: PurgeContinuation | undefined,
  ): PurgePassResult {
    return {
      floor,
      batches: 0,
      deletedRows: 0,
      stopped: reason,
      probe: undefined,
      continuation,
    };
  }
}
