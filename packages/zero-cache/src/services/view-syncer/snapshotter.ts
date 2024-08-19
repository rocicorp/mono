import {LogContext} from '@rocicorp/logger';
import Database from 'better-sqlite3';
import {ident} from 'pg-format';
import {assert} from 'shared/src/asserts.js';
import * as v from 'shared/src/valita.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {JSONValue} from 'zero-cache/src/types/bigint-json.js';
import {
  normalizedKeyOrder,
  RowKey,
  RowValue,
} from 'zero-cache/src/types/row-key.js';
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
 * 1. ViewSyncer hydrates at `t1` and keeps snapshot_a at that version of the
 *    database.
 * 2. Replicator applies a transaction to the database and notifies subscribers.
 * 3. ViewSyncer takes `snapshot_b` at `t2`, and queries its `ChangeLog` for
 *    changes since `t1`.
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
  #curr: Snapshot;
  #prev: Snapshot | undefined;

  constructor(lc: LogContext, dbFile: string) {
    this.#lc = lc;
    this.#curr = Snapshot.create(dbFile);

    this.#lc.debug?.(`Initial snapshot at version ${this.#curr.version}`);
  }

  /** Returns the current snapshot. */
  current(): {db: StatementRunner; version: string} {
    return this.#curr;
  }

  /**
   * Advances to the head of the Database, returning a diff between the
   * previously current Snapshot and a new Snapshot at head. This is called
   * in response to a notification from a Replicator subscription. Subsequent
   * calls to {@link current()} return the new Snapshot.
   *
   * The returned {@link SnapshotDiff} contains snapshots at the endpoints
   * of the database timeline. Iterating over the diff generates a sequence
   * of {@link Change}s between the two snapshots.
   *
   * Note that this sequence is not chronological; rather, the sequence is
   * ordered by `<table, row-key>`, such that a row can appear at most once
   * in the sequence. This results in coalescing multiple changes to a row
   * and bounds the amount of work needed to catch up; however, as a corollary
   * a consistent database state is only guaranteed when the sequence has been
   * fully consumed.
   *
   * Note that the Change generation relies on the state of the underlying
   * snapshots, and because the database connection for the previous snapshot
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
   * Once the changes have been applied, however, a _subsequent_ iteration will
   * not produce the correct results. In order to preform multiple
   * change-applying iterations, the caller must create a save point
   * before each iteration, and rollback to the save point after the iteration.
   */
  advance(): SnapshotDiff {
    const next = this.#prev
      ? this.#prev.resetToHead()
      : Snapshot.create(this.#curr.db.db.name);
    this.#prev = this.#curr;
    this.#curr = next;
    return new Diff(this.#prev, this.#curr);
  }
}

export type Change = {
  readonly table: string;
  readonly rowKey: Readonly<RowKey>;
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
  readonly changes: number;
}

class Snapshot {
  static create(dbFile: string) {
    const conn = new Database(dbFile);
    conn.pragma('journal_mode = WAL');
    conn.pragma('synchronous = OFF'); // Applied changes are ephemeral; COMMIT is never called.

    const db = new StatementRunner(conn);
    db.beginConcurrent();
    // Note: The read is necessary to acquire the read lock (which results in the logical creation
    // of the snapshot). Calling `BEGIN CONCURRENT` on its own does not acquire the read lock.
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
      iter: cached.statement.iterate(prevVersion),
      cleanup: () => this.db.statementCache.return(cached),
    };
  }

  getRow(table: string, rowKey: JSONValue) {
    const key = normalizedKeyOrder(rowKey as RowKey);
    const conds = Object.keys(key).map(c => `${ident(c)}=?`);
    return this.db.get(
      `SELECT * FROM ${ident(table)} WHERE ${conds.join(' AND ')}`,
      Object.values(key),
    );
  }

  resetToHead(): Snapshot {
    this.db.rollback();
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
    const {iter, cleanup} = this.curr.changesSince(this.prev.version);
    return {
      next: () => {
        const {value, done} = iter.next();
        if (done) {
          cleanup();
          return {value, done: true};
        }

        const {table, rowKey, op, stateVersion} = v.parse(value, schema);
        if (op === TRUNCATE_OP) {
          // TODO: Implement TRUNCATE
          throw new Error('implement me');
        }
        assert(rowKey !== null); // Only null for TRUNCATE.
        const prevValue = this.prev.getRow(table, rowKey) ?? null;
        const nextValue =
          op === SET_OP ? this.curr.getRow(table, rowKey) : null;

        // Sanity check detects if the diff is being accessed after the Snapshots have advanced.
        this.checkThatDiffIsValid(stateVersion, op, prevValue, nextValue);

        return {value: {table, rowKey, prevValue, nextValue} satisfies Change};
      },

      return: (value: unknown) => {
        try {
          return iter.return?.(value) ?? {value, done: true};
        } finally {
          cleanup();
        }
      },

      throw: (err: unknown) => {
        try {
          return iter.throw?.(err) ?? {value: undefined, done: true};
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

export class InvalidDiffError extends Error {
  constructor(msg: string) {
    super(msg);
  }
}
