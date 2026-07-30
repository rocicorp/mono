/**
 * Deletes aged-out transactions from the change-log database, one bounded write
 * transaction at a time.
 *
 * The purger is a pure function of one database: it takes the floors its caller
 * has already gated on and narrows them, never widens them, and it reads no
 * replication state. That is what makes it testable without a scheduler and
 * what lets slice 9 run it from the change-streamer's stream loop, between the
 * writer's commits, on the writer's own connection.
 */

import {assert} from '../../../../shared/src/asserts.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import {CHANGE_LOG_STREAM_TABLE} from './change-log-db.ts';

/**
 * The eligible set, as three ANDed bounds. Only the `externalFloor` is a safety
 * property; the other two make the log serviceable and make purge conservative.
 *
 * `"watermark" < @externalFloor` is deliberately strict. A follower that lost
 * its replica restores it from S3 at watermark `W` and then asks this log for
 * everything after `W`, which requires the `commit` row *at* `W` to still be
 * here (`SQLITE_CHANGE_LOG_BOUNDARY_SQL`). Deleting it returns `too-old` to the
 * very subscriber the floor exists to protect.
 *
 * The head cap reads `max("watermark")` from the log's own database, which
 * keeps the purger a pure function of one file. The cap is inert in normal
 * operation: the floor is derived from a backup of a replica that consumes this
 * loop's output, and the log commits before that output is forwarded, so
 * `externalFloor <= head` holds by construction. It engages only on a *stale*
 * log — writer stopped, mode flipped to `off`, another task took over — where
 * deleting to a floor that has run past a frozen log would empty the file. Note
 * that head can be a phantom — committed here but not yet re-deliverable by a
 * resumed stream — for as long as it takes the watermark the stream would
 * resume from to advance past it. That is harmless precisely because head is
 * never a purge candidate, and it is why the purger asserts nothing about
 * anything outside its own file.
 *
 * Ordered by `("writeTimeMs", "watermark")` so it rides the partial index,
 * which carries one entry per transaction rather than one per change. The
 * `LIMIT` keeps a drain from re-scanning the remaining history on every batch,
 * which would be quadratic in the number of eligible transactions and would do
 * it inside the `BEGIN IMMEDIATE`, holding the write lock alongside the delete.
 *
 * Exported so the focused query-plan test covers the production query.
 */
export const SQLITE_CHANGE_LOG_ELIGIBLE_COMMITS_SQL = /*sql*/ `
  SELECT "watermark"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "writeTimeMs" IS NOT NULL
    AND "writeTimeMs" < @retentionCutoffMs
    AND "watermark" < @externalFloor
    AND "watermark" < (SELECT max("watermark") FROM "${CHANGE_LOG_STREAM_TABLE}")
  ORDER BY "writeTimeMs", "watermark"
  LIMIT @limit
`;

/**
 * Counts the rows at `@watermark`, capped at `@limit`. The cap is what keeps
 * batch sizing O(budget): this runs per candidate inside the
 * `BEGIN IMMEDIATE`, and the transaction being sized can be arbitrarily
 * large — an uncapped count of a multi-million-row backfill would rescan
 * every remaining row on every tear batch, holding the write lock (and the
 * appending path behind it) for the duration, quadratically over the drain.
 * The caller only needs to know whether the transaction fits its remaining
 * budget, so it passes `remaining + 1` and reads a count above `remaining`
 * as "does not fit".
 *
 * Exported so the focused tests cover the production query.
 */
export const SQLITE_CHANGE_LOG_COUNT_TRANSACTION_SQL = /*sql*/ `
  SELECT count(*) AS "rows" FROM (
    SELECT 1
    FROM "${CHANGE_LOG_STREAM_TABLE}"
    WHERE "watermark" = @watermark
    LIMIT @limit
  )
`;

/**
 * A prefix delete, so no transaction between the minimum and the head is ever
 * split. The `<=` is correct and is not the floor comparison: `deletedThrough`
 * is a watermark this batch already established as wholly eligible.
 */
const DELETE_THROUGH_SQL = /*sql*/ `
  DELETE FROM "${CHANGE_LOG_STREAM_TABLE}" WHERE "watermark" <= ?
`;

const MIN_WATERMARK_SQL = /*sql*/ `
  SELECT min("watermark") AS "minWatermark" FROM "${CHANGE_LOG_STREAM_TABLE}"
`;

/**
 * The `pos` at `@offset` rows into the torn transaction, ascending. `undefined`
 * when fewer than `@offset` rows remain, which is how the tear knows it is on
 * its final chunk.
 */
const TEAR_BOUNDARY_SQL = /*sql*/ `
  SELECT "pos"
  FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" = @watermark
  ORDER BY "pos"
  LIMIT 1 OFFSET @offset
`;

/**
 * A range scan on the primary key. Expressed as `"pos" < @pos` rather than
 * `DELETE ... LIMIT n`, which is both non-portable and the wrong bound: it
 * carries no guarantee about *which* rows it removes, and the commit row must
 * survive until the chunk that finishes the tear.
 */
const DELETE_BELOW_POS_SQL = /*sql*/ `
  DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"
  WHERE "watermark" = @watermark AND "pos" < @pos
`;

export type PurgeBatchResult = {
  readonly deletedRows: number;
  /**
   * The watermark through which whole transactions were removed. `undefined`
   * when the batch only tore rows off the minimum transaction without
   * completing it — in that case `deletedRows > 0` and `moreEligible` is true.
   */
  readonly deletedThrough: string | undefined;
  /** True while a tear is in progress, as well as while whole transactions remain eligible. */
  readonly moreEligible: boolean;
};

export type PurgeBatchOptions = {
  /**
   * The floor the caller has already gated on subscriber ACKs and snapshot
   * reservations — normally the confirmed S3 backup watermark. Transactions at
   * or above it are NOT durable anywhere else: the change log is excluded from
   * the litestream backup, so this is the boundary a follower restores to and
   * then catches up from. Compared strictly (`<`), so the transaction *at* the
   * floor survives as that follower's catchup boundary. The purger narrows this
   * input and never widens it.
   */
  readonly externalFloor: string;
  /**
   * A *minimum* retention, ANDed on top of `externalFloor` rather than an
   * alternative to it. Which of the two binds depends on the backup interval.
   */
  readonly retentionCutoffMs: number;
  /** Starting chunk size. Not the bound — `maxDurationMs` is. */
  readonly maxRows: number;
  /**
   * The statement duration target, enforced by feedback rather than by
   * interruption: SQLite cannot yield mid-statement, so the purger measures
   * each delete and shrinks the next batch's row budget when one overruns
   * (see {@link SQLiteChangeLogPurger.budgetRows}) — which means the first
   * delete after a restart, sized by `maxRows` alone, can still overrun it.
   * The target is this consequential because purge shares the writer's
   * connection: there is no lock to arbitrate and no `busy_timeout` to bound
   * the wait — a long statement blocks the appending path, and the forward
   * behind it, directly and for its full duration. That is why this is a
   * precondition of purging from the stream loop rather than a tuning knob.
   */
  readonly maxDurationMs: number;
};

type Statements = {
  readonly eligibleCommits: Statement;
  readonly countTransaction: Statement;
  readonly deleteThrough: Statement;
  readonly minWatermark: Statement;
  readonly tearBoundary: Statement;
  readonly deleteBelowPos: Statement;
};

export class SQLiteChangeLogPurger {
  readonly #db: Database;
  readonly #now: () => number;
  readonly #stmts: Statements;

  /**
   * The measured row budget, carried across batches. Row cost varies with
   * payload size, so `maxRows` alone cannot bound a statement's duration; this
   * shrinks when a delete overruns `maxDurationMs` and grows back when the
   * budget — rather than the eligible set — was what limited the batch.
   */
  #budgetRows: number | undefined;

  /**
   * @param db a writable connection to a change log the writer has already
   *     created and reconciled — in slice 9, the writer's own connection.
   *     Statements are prepared eagerly, so a caller that may run before the
   *     log exists opens the connection lazily and treats construction failure
   *     as a skipped cycle.
   */
  constructor(
    db: Database,
    now: () => number = performance.now.bind(performance),
  ) {
    this.#db = db;
    this.#now = now;
    this.#stmts = {
      eligibleCommits: db.prepare(SQLITE_CHANGE_LOG_ELIGIBLE_COMMITS_SQL),
      countTransaction: db.prepare(SQLITE_CHANGE_LOG_COUNT_TRANSACTION_SQL),
      deleteThrough: db.prepare(DELETE_THROUGH_SQL),
      minWatermark: db.prepare(MIN_WATERMARK_SQL),
      tearBoundary: db.prepare(TEAR_BOUNDARY_SQL),
      deleteBelowPos: db.prepare(DELETE_BELOW_POS_SQL),
    };
  }

  /** The row budget the next batch would use. Exposed for tests and metrics. */
  get budgetRows(): number | undefined {
    return this.#budgetRows;
  }

  /**
   * Removes one bounded batch of aged-out history, in one `BEGIN IMMEDIATE`
   * transaction. Callers loop while `moreEligible`, releasing whatever guards
   * they hold between batches.
   */
  purgeBatch(opts: PurgeBatchOptions): PurgeBatchResult {
    const {maxRows, maxDurationMs} = opts;
    assert(
      Number.isSafeInteger(maxRows) && maxRows > 0,
      'change-log purge maxRows must be a positive safe integer',
    );
    assert(
      maxDurationMs > 0,
      'change-log purge maxDurationMs must be positive',
    );

    this.#db.exec('BEGIN IMMEDIATE');
    try {
      const result = this.#purge(opts);
      this.#db.exec('COMMIT');
      return result;
    } catch (e) {
      if (this.#db.inTransaction) {
        this.#db.exec('ROLLBACK');
      }
      throw e;
    }
  }

  #purge(opts: PurgeBatchOptions): PurgeBatchResult {
    const {externalFloor, retentionCutoffMs} = opts;
    const budget = this.#budget(opts.maxRows);
    // Each transaction is at least a `begin` and a `commit`, so this is the
    // most candidates that can fit in the budget.
    const limit = Math.max(1, Math.ceil(budget / 2));
    const candidates = this.#stmts.eligibleCommits.all<{watermark: string}>({
      retentionCutoffMs,
      externalFloor,
      limit,
    });
    if (candidates.length === 0) {
      return {deletedRows: 0, deletedThrough: undefined, moreEligible: false};
    }

    // Accounted per candidate rather than per prefix, which is exact as long as
    // the candidates arrive in watermark order — they do, because the single
    // writer stamps `writeTimeMs` at commit time. A clock that went backwards
    // can put a non-candidate below `deletedThrough`, and the prefix delete
    // then removes more rows than were counted. Not a safety problem: both
    // watermark bounds are prefix-closed, only the minimum-retention courtesy
    // is crossed, and `maxRows` is the starting chunk size rather than the
    // bound — `maxDurationMs` is, and the governor measures what actually ran.
    let rows = 0;
    let deletedThrough: string | undefined;
    for (const {watermark} of candidates) {
      // The count is capped at one past the remaining budget: `n > remaining`
      // is exactly `rows + n > budget` for a fitting transaction, and a
      // too-large one stops scanning as soon as it is known not to fit.
      const remaining = budget - rows;
      const {rows: n} = this.#stmts.countTransaction.get<{rows: number}>({
        watermark,
        limit: remaining + 1,
      });
      if (n > remaining) {
        break;
      }
      rows += n;
      deletedThrough = watermark;
    }

    if (deletedThrough === undefined) {
      return this.#tear(opts, budget);
    }
    const deletedRows = this.#timed(
      opts,
      budget,
      () => this.#stmts.deleteThrough.run(deletedThrough).changes,
    );
    return {
      deletedRows,
      deletedThrough,
      moreEligible: this.#anyEligible(opts),
    };
  }

  /**
   * Removes one bounded chunk of the minimum transaction, which is the only
   * transaction that is ever torn.
   *
   * A single upstream transaction can be arbitrarily large — a backfill, a
   * schema migration — and SQLite cannot yield mid-statement, so the only way
   * to bound the lock hold is to delete it across several statements. That is
   * unobservable given three constraints, all of which live here:
   *
   * 1. Only `min("watermark")` is torn, so at most one transaction is ever
   *    partial. A reader never reads *into* a transaction — `read()` starts
   *    strictly after `(fromWatermark, MAX_SAFE_INTEGER)` — so the only
   *    transaction a tear can touch is one a subscriber reads *from*, and
   *    reading from it does not read its rows.
   * 2. Rows go ascending by `pos`, commit row last. `plan()`'s boundary check
   *    reads the *highest* `pos` at the watermark and requires it to be the
   *    `commit`; removing that row first would give a subscriber sitting
   *    exactly there a spurious `too-old`.
   * 3. The commit row is removed by a prefix delete, which advances
   *    `min("watermark")` in the same SQLite transaction, so no state exposes
   *    the boundary as gone while the minimum still points at it. Normally that
   *    is {@link SQLiteChangeLogPurger.purgeBatch}'s ordinary whole-transaction
   *    delete, taken on the batch where what is left of the torn transaction
   *    finally fits the budget; the branch below is the same delete for the
   *    case where the minimum is not itself in the eligible set.
   *
   * A crash mid-tear needs no recovery state: the torn transaction is still the
   * minimum and still eligible, so the next batch continues it. Reconciliation
   * is unaffected — it tests the head, and a tear is at the tail.
   *
   * Tearing the minimum is safe even in the unusual case where it is not itself
   * in the eligible set (a writer whose clock went backwards can put a later
   * watermark at an earlier `writeTimeMs`). Both watermark bounds are
   * prefix-closed — a non-empty eligible set implies `min < externalFloor` and
   * `min < head` — and the prefix delete this batch would otherwise run removes
   * the minimum anyway. Only the time floor, which is a minimum-retention
   * courtesy rather than a safety property, can be crossed that way.
   */
  #tear(opts: PurgeBatchOptions, budget: number): PurgeBatchResult {
    const {minWatermark} = this.#stmts.minWatermark.get<{
      minWatermark: string | null;
    }>();
    assert(
      minWatermark !== null,
      'change-log purge found eligible transactions in an empty log',
    );

    const boundary = this.#stmts.tearBoundary.get<{pos: number} | undefined>({
      watermark: minWatermark,
      offset: budget,
    });
    if (boundary === undefined) {
      // At most `budget` rows are left at the minimum, which is reachable only
      // when the minimum is not the transaction that failed to fit — i.e. when
      // it is not itself eligible. The prefix delete removes it whole and
      // advances `min("watermark")`.
      const deletedRows = this.#timed(
        opts,
        budget,
        () => this.#stmts.deleteThrough.run(minWatermark).changes,
      );
      return {
        deletedRows,
        deletedThrough: minWatermark,
        moreEligible: this.#anyEligible(opts),
      };
    }
    // `boundary.pos` is the `budget`-th row by `pos`, so this removes exactly
    // `budget` rows and always makes progress. The commit row is the highest
    // `pos`, so it can only be reached by the branch above.
    const deletedRows = this.#timed(
      opts,
      budget,
      () =>
        this.#stmts.deleteBelowPos.run({
          watermark: minWatermark,
          pos: boundary.pos,
        }).changes,
    );
    return {deletedRows, deletedThrough: undefined, moreEligible: true};
  }

  #anyEligible({externalFloor, retentionCutoffMs}: PurgeBatchOptions): boolean {
    return (
      this.#stmts.eligibleCommits.get<{watermark: string} | undefined>({
        retentionCutoffMs,
        externalFloor,
        limit: 1,
      }) !== undefined
    );
  }

  #budget(maxRows: number): number {
    return Math.max(1, Math.min(maxRows, this.#budgetRows ?? maxRows));
  }

  /**
   * Runs a delete and feeds its measured duration back into the row budget.
   * Growth is gated on the budget having been the binding constraint: a batch
   * that finished quickly because only four rows were eligible is no evidence
   * that a larger statement would stay under the target.
   */
  #timed(
    {maxRows, maxDurationMs}: PurgeBatchOptions,
    budget: number,
    run: () => number,
  ): number {
    const start = this.#now();
    const deletedRows = run();
    const elapsedMs = this.#now() - start;

    if (elapsedMs > maxDurationMs) {
      this.#budgetRows = Math.max(1, Math.floor(budget / 2));
    } else if (deletedRows >= budget && elapsedMs * 2 <= maxDurationMs) {
      this.#budgetRows = Math.min(maxRows, budget * 2);
    } else {
      this.#budgetRows = budget;
    }
    return deletedRows;
  }
}
