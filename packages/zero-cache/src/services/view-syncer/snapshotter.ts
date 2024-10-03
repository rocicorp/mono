import {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import {assert} from 'shared/src/asserts.js';
import * as v from 'shared/src/valita.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {
  jsonObjectSchema,
  type JSONValue,
} from 'zero-cache/src/types/bigint-json.js';
import {
  normalizedKeyOrder,
  type RowKey,
  type RowValue,
} from 'zero-cache/src/types/row-key.js';
import {Database} from 'zqlite/src/db.js';
import {
  changeLogEntrySchema as schema,
  SET_OP,
  TRUNCATE_OP,
} from '../replicator/schema/change-log.js';
import {
  getReplicationVersions,
  ZERO_VERSION_COLUMN_NAME as ROW_VERSION,
} from '../replicator/schema/replication-state.js';

/**
 * A `Snapshotter` manages the progression of database snapshots for a
 * ViewSyncer.
 *
 * The Replicator and ViewSyncers operate on the same SQLite file, with the
 * Replicator being the sole writer to the database. The IVM logic in
 * ViewSyncers, however, rely on incrementally applying changes to the DB to
 * update the state of its pipelines.
 *
 * To avoid coupling the progress of the Replicator and all IVM pipelines on
 * each other, ViewSyncers operate on ephemeral forks of the database by holding
 * [concurrent](https://sqlite.org/src/doc/begin-concurrent/doc/begin_concurrent.md)
 * snapshots of the database and simulating (but ultimately rolling back)
 * mutations on these snapshots.
 *
 * Example:
 * 1. ViewSyncer takes `snapshot_a` at version `t1` of the database and
 *    hydrates its pipeline(s).
 * 2. Replicator applies a new transaction to the database and notifies
 *    subscribers.
 * 3. ViewSyncer takes `snapshot_b` at `t2`, and queries the `ChangeLog` at
 *    that snapshot for changes since `t1`.
 * 4. ViewSyncer applies those changes to `snapshot_a` for IVM, but does not
 *    commit them. (Recall that the Replicator is the sole writer to the db, so
 *    the ViewSyncer never commits any writes.)
 * 5. Replicator applies the next transaction and advances the database to `t3`.
 * 6. ViewSyncer rolls back `snapshot_a` and opens `snapshot_c` at `t3`, using
 *    `snapshot_b` to simulate changes from `t2` to `t3`.
 *
 * ```
 * Replicator:  t1 --------------> t2 --------------> t3 --------------->
 * ViewSyncer:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
 * ```
 *
 * Note that the Replicator (and ViewSyncers) do not wait on the progress of
 * other ViewSyncers. If a ViewSyncer is busy hydrating at `t1`, the Replicator
 * and other ViewSyncers can progress through `t2`, `t3`, etc. independently,
 * as the busy ViewSyncer simply takes its own snapshot when it is ready.
 *
 * ```
 * Replicator:   t1 --------------> t2 --------------> t3 --------------->
 * ViewSyncer1:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
 * ViewSyncer2:       [.......... snapshot_a ..........] ----> [snapshot_b]
 * ```
 *
 * To minimize Database connections (and statement preparation, etc.), the
 * Snapshotter reuses the connection from the previous (rolled back)
 * snapshot when opening the new one.
 *
 * ```
 * Replicator:  t1 --------------> t2 --------------> t3 --------------->
 * ViewSyncer:       [snapshot_a] ----> [snapshot_b] ----> [snapshot_c]
 *                     (conn_1)           (conn_2)           (conn_1)
 * ```
 *
 * In this manner, each ViewSyncer uses two connections that continually
 * "leapfrog" each other to replay the timeline of changes in isolation from
 * the Replicator and other ViewSyncers.
 */
export class Snapshotter {
  readonly #lc: LogContext;
  readonly #dbFile: string;
  #curr: Snapshot | undefined;
  #prev: Snapshot | undefined;

  constructor(lc: LogContext, dbFile: string) {
    this.#lc = lc;
    this.#dbFile = dbFile;
  }

  /**
   * Initializes the snapshot to the current head of the database. This must be
   * only be called once. The state of whether a Snapshotter has been initialized
   * can be determined by calling {@link initialized()}.
   */
  init(): this {
    assert(this.#curr === undefined, 'Already initialized');
    this.#curr = Snapshot.create(this.#lc, this.#dbFile);
    this.#lc.debug?.(`Initial snapshot at version ${this.#curr.version}`);
    return this;
  }

  initialized(): boolean {
    return this.#curr !== undefined;
  }

  /** Returns the current snapshot. Asserts if {@link initialized()} is false. */
  current(): {db: StatementRunner; version: string} {
    assert(this.#curr?.hasLock, 'Snapshotter uninitialized or released');
    return this.#curr;
  }

  /**
   * Advances to the head of the Database, returning a diff between the
   * previously current Snapshot and a new Snapshot at head. This is called
   * in response to a notification from a Replicator subscription. Subsequent
   * calls to {@link current()} return the new Snapshot. Note that the Snapshotter
   * must be initialized before advancing.
   *
   * The returned {@link SnapshotDiff} contains snapshots at the endpoints
   * of the database timeline. Iterating over the diff generates a sequence
   * of {@link Change}s between the two snapshots.
   *
   * Note that this sequence is not chronological; rather, the sequence is
   * ordered by `<table, row-key>`, such that a row can appear at most once
   * in the common case, or twice if its table is `TRUNCATE`'d and a new value
   * is subsequently `INSERT`'ed. This results in dropping most intermediate
   * changes to a row and bounds the amount of work needed to catch up;
   * however, as a consequence, a consistent database state is only guaranteed
   * when the sequence has been fully consumed.
   *
   * Note that Change generation relies on the state of the underlying
   * database connections, and because the connection for the previous snapshot
   * is reused to produce the next snapshot, the diff object is only valid
   * until the next call to `advance()`.
   *
   * It is okay for the caller to apply `Change`s to the `prev` snapshot
   * during the iteration (e.g. this is necessary for IVM); the remainder
   * of the iteration is not affected because a given row can appear at most
   * once in the sequence (with the exception being TRUNCATE, after which the
   * deleted rows can be re-inserted, but this will also behave correctly if
   * the changes are applied).
   *
   * Once the changes have been applied, however, a _subsequent_ iteration
   * will not produce the correct results. In order to perform multiple
   * change-applying iterations, the caller must (1) create a save point
   * on `prev` before each iteration, and (2) rollback to the save point after
   * the iteration.
   */
  advance(): SnapshotDiff {
    assert(this.#curr !== undefined, 'Snapshotter has not been initialized');
    const next = this.#prev
      ? this.#prev.resetToHead()
      : Snapshot.create(this.#lc, this.#curr.db.db.name);
    this.#prev = this.#curr;
    this.#curr = next;
    return new Diff(this.#prev, this.#curr);
  }

  /**
   * Releases all locks on the database. Call {@link advance()}
   * to reestablish a snapshot.
   *
   * This must only be called if the Snapshotter has been initialized.
   */
  release() {
    assert(this.#curr !== undefined, 'Snapshotter has not been initialized');
    this.#curr.release();
    this.#prev?.release();
  }

  /**
   * Call this to close the database connections when the Snapshotter is
   * no longer needed.
   */
  destroy() {
    this.#curr?.db.db.close();
    this.#prev?.db.db.close();
    this.#lc.debug?.('closed database connections');
  }
}

export type Change = {
  readonly table: string;
  readonly prevValue: Readonly<RowValue> | null;
  readonly nextValue: Readonly<RowValue> | null;
};

/**
 * Represents the difference between two database Snapshots.
 * Iterating over the object will produce a sequence of {@link Change}s
 * between the two snapshots.
 *
 * See {@link Snapshotter.advance()} for semantics and usage.
 */
export interface SnapshotDiff extends Iterable<Change> {
  readonly prev: {
    readonly db: StatementRunner;
    readonly version: string;
  };
  readonly curr: {
    readonly db: StatementRunner;
    readonly version: string;
  };

  /**
   * The number of ChangeLog entries between the snapshots. Note that this
   * may not necessarily equal the number of `Change` objects that the iteration
   * will produce, as `TRUNCATE` entries are counted as a single log entry which
   * may be expanded into many changes (i.e. row deletes).
   *
   * TODO: Determine if it is worth changing the definition to count the
   *       truncated rows. This would make diff computation more expensive
   *       (requiring the count to be aggregated by operation type), which
   *       may not be worth it for a presumable rare operation.
   */
  readonly changes: number;
}

class Snapshot {
  static create(lc: LogContext, dbFile: string) {
    const conn = new Database(lc, dbFile);
    conn.pragma('synchronous = OFF'); // Applied changes are ephemeral; COMMIT is never called.

    const db = new StatementRunner(conn);
    db.beginConcurrent();
    // Note: The subsequent read is necessary to acquire the read lock
    // (which results in the logical creation of the snapshot). Calling
    // `BEGIN CONCURRENT` alone does not result in acquiring the read lock.
    const {stateVersion} = getReplicationVersions(db);
    return new Snapshot(db, stateVersion);
  }

  readonly db: StatementRunner;
  readonly version: string;

  constructor(db: StatementRunner, version: string) {
    this.db = db;
    this.version = version;
  }

  numChangesSince(prevVersion: string) {
    const {count} = this.db.get(
      'SELECT COUNT(*) AS count FROM "_zero.ChangeLog" WHERE stateVersion > ?',
      prevVersion,
    );
    return count;
  }

  changesSince(prevVersion: string) {
    const cached = this.db.statementCache.get(
      'SELECT * FROM "_zero.ChangeLog" WHERE stateVersion > ?',
    );
    return {
      changes: cached.statement.iterate(prevVersion),
      cleanup: () => this.db.statementCache.return(cached),
    };
  }

  getRow(table: string, rowKey: JSONValue) {
    const key = normalizedKeyOrder(rowKey as RowKey);
    const conds = Object.keys(key).map(c => `${ident(c)}=?`);
    const cached = this.db.statementCache.get(
      `SELECT * FROM ${ident(table)} WHERE ${conds.join(' AND ')}`,
    );
    cached.statement.safeIntegers(true);
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return cached.statement.get<any>(Object.values(key));
    } finally {
      this.db.statementCache.return(cached);
    }
  }

  getRows(table: string) {
    const cached = this.db.statementCache.get(`SELECT * FROM ${ident(table)}`);
    cached.statement.safeIntegers(true);
    return {
      rows: cached.statement.iterate(),
      cleanup: () => this.db.statementCache.return(cached),
    };
  }

  get hasLock() {
    return this.db.db.inTransaction;
  }

  release() {
    if (this.hasLock) {
      this.db.rollback();
    }
  }

  resetToHead(): Snapshot {
    this.release();
    this.db.beginConcurrent();
    const {stateVersion} = getReplicationVersions(this.db);
    return new Snapshot(this.db, stateVersion);
  }
}

class Diff implements SnapshotDiff {
  readonly prev: Snapshot;
  readonly curr: Snapshot;
  readonly changes: number;

  constructor(prev: Snapshot, curr: Snapshot) {
    this.prev = prev;
    this.curr = curr;
    this.changes = curr.numChangesSince(prev.version);
  }

  [Symbol.iterator](): Iterator<Change> {
    const {changes, cleanup: done} = this.curr.changesSince(this.prev.version);
    const truncates = new TruncateTracker(this.prev);

    const cleanup = () => {
      done();
      truncates.done();
    };

    return {
      next: () => {
        for (;;) {
          // Exhaust the TRUNCATE iteration before continuing the Change sequence.
          const truncatedRow = truncates.next();
          if (truncatedRow) {
            return truncatedRow;
          }

          const {value, done} = changes.next();
          if (done) {
            cleanup();
            return {value, done: true};
          }

          const {table, rowKey, op, stateVersion} = v.parse(value, schema);
          if (op === TRUNCATE_OP) {
            truncates.startTruncate(table);
            continue; // loop around to pull rows from the TruncateTracker.
          }

          assert(rowKey !== null);
          const prevValue =
            truncates.getRowIfNotTruncated(table, rowKey) ?? null;
          const nextValue =
            op === SET_OP ? this.curr.getRow(table, rowKey) : null;

          // Sanity check detects if the diff is being accessed after the Snapshots have advanced.
          this.checkThatDiffIsValid(stateVersion, op, prevValue, nextValue);

          if (prevValue === null && nextValue === null) {
            // Filter out no-op changes (e.g. a delete of a row that does not exist in prev).
            // TODO: Consider doing this for deep-equal values.
            continue;
          }

          return {value: {table, prevValue, nextValue} satisfies Change};
        }
      },

      return: (value: unknown) => {
        try {
          // Allow open iterators to clean up their state.
          truncates.iterReturn(value);
          changes.return?.(value);
          return {value, done: true};
        } finally {
          cleanup();
        }
      },

      throw: (err: unknown) => {
        try {
          // Allow open iterators to clean up their state.
          truncates.iterThrow(err);
          changes.throw?.(err);
          return {value: undefined, done: true};
        } finally {
          cleanup();
        }
      },
    };
  }

  checkThatDiffIsValid(
    stateVersion: string,
    op: string,
    prevValue: RowValue,
    nextValue: RowValue,
  ) {
    // Sanity checks to detect that the diff is not being accessed after
    // the Snapshots have advanced.
    if (stateVersion > this.curr.version) {
      throw new InvalidDiffError(
        `Diff is no longer valid. curr db has advanced past ${this.curr.version}`,
      );
    }
    if (
      prevValue !== null &&
      (prevValue[ROW_VERSION] ?? '~') > this.prev.version
    ) {
      throw new InvalidDiffError(
        `Diff is no longer valid. prev db has advanced past ${this.prev.version}.`,
      );
    }
    if (op === SET_OP && nextValue[ROW_VERSION] !== stateVersion) {
      throw new InvalidDiffError(
        'Diff is no longer valid. curr db has advanced.',
      );
    }
  }
}

/**
 * `TRUNCATE` changes are handled by:
 * 1. Iterating over all of the rows in the `prev` Snapshot and returning
 *    corresponding `DELETE` row operations for them (i.e. `nextValue: null`).
 * 2. Tracking the fact that a table has been truncated (i.e. all row-deletes
 *    have been returned) so that subsequent lookups of prevValues (e.g. for
 *    inserts after the truncate) correctly return `null`.
 */
class TruncateTracker {
  readonly #prev: Snapshot;
  readonly #truncated = new Set<string>();

  #truncating: {
    table: string;
    rows: Iterator<unknown>;
    cleanup: () => void;
  } | null = null;

  constructor(prev: Snapshot) {
    this.#prev = prev;
  }

  startTruncate(table: string) {
    assert(this.#truncating === null);
    const {rows, cleanup} = this.#prev.getRows(table);
    this.#truncating = {table, rows, cleanup};
  }

  next(): IteratorResult<Change> | null {
    if (this.#truncating === null) {
      return null;
    }
    const {table} = this.#truncating;
    const {value, done} = this.#truncating.rows.next();
    if (done) {
      this.#truncating.cleanup();
      this.#truncating = null;
      this.#truncated.add(table);
      return null;
    }
    const prevValue = v.parse(value, jsonObjectSchema);

    // Sanity check detects if the diff is being accessed after the Snapshots have advanced.
    if ((prevValue[ROW_VERSION] ?? '~') > this.#prev.version) {
      throw new InvalidDiffError(
        `Diff is no longer valid. prev db has advanced past ${
          this.#prev.version
        }.`,
      );
    }

    return {value: {table, prevValue, nextValue: null} satisfies Change};
  }

  getRowIfNotTruncated(table: string, rowKey: RowKey) {
    // If the row has been returned in a TRUNCATE iteration, its prevValue is henceforth null.
    return this.#truncated.has(table) ? null : this.#prev.getRow(table, rowKey);
  }

  iterReturn(value: unknown) {
    this.#truncating?.rows.return?.(value);
  }

  iterThrow(err: unknown) {
    this.#truncating?.rows.throw?.(err);
  }

  done() {
    this.#truncating?.cleanup();
  }
}

export class InvalidDiffError extends Error {
  constructor(msg: string) {
    super(msg);
  }
}
