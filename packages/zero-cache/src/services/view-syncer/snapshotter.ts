import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-types/src/schema.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../../../zqlite/src/table-source.ts';
import type {LiteAndZqlSpec, LiteTableSpecWithKeys} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {
  normalizedKeyOrder,
  type RowKey,
  type RowValue,
} from '../../types/row-key.ts';
import {type SchemaVersions} from '../../types/schema-versions.ts';
import type {AppID} from '../../types/shards.ts';
import {id} from '../../types/sql.ts';
import {
  RESET_OP,
  changeLogEntrySchema as schema,
  SET_OP,
  TRUNCATE_OP,
} from '../replicator/schema/change-log.ts';
import {
  getReplicationState,
  ZERO_VERSION_COLUMN_NAME as ROW_VERSION,
} from '../replicator/schema/replication-state.ts';

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
  readonly #appID: string;
  readonly #pageCacheSizeKib: number | undefined;
  #curr: Snapshot | undefined;
  #prev: Snapshot | undefined;

  constructor(
    lc: LogContext,
    dbFile: string,
    {appID}: AppID,
    pageCacheSizeKib?: number | undefined,
  ) {
    this.#lc = lc;
    this.#dbFile = dbFile;
    this.#appID = appID;
    this.#pageCacheSizeKib = pageCacheSizeKib;
  }

  /**
   * Initializes the snapshot to the current head of the database. This must be
   * only be called once. The state of whether a Snapshotter has been initialized
   * can be determined by calling {@link initialized()}.
   */
  init(): this {
    assert(this.#curr === undefined, 'Already initialized');
    this.#curr = Snapshot.create(
      this.#lc,
      this.#dbFile,
      this.#appID,
      this.#pageCacheSizeKib,
    );
    this.#lc.debug?.(`Initial snapshot at version ${this.#curr.version}`);
    return this;
  }

  initialized(): boolean {
    return this.#curr !== undefined;
  }

  /** Returns the current snapshot. Asserts if {@link initialized()} is false. */
  current(): Snapshot {
    assert(this.#curr !== undefined, 'Snapshotter has not been initialized');
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
  advance(tables: Map<string, LiteAndZqlSpec>): SnapshotDiff {
    const {prev, curr} = this.advanceWithoutDiff();
    return new Diff(this.#appID, tables, prev, curr);
  }

  advanceWithoutDiff() {
    assert(this.#curr !== undefined, 'Snapshotter has not been initialized');
    const next = this.#prev
      ? this.#prev.resetToHead()
      : Snapshot.create(
          this.#lc,
          this.#curr.db.db.name,
          this.#appID,
          this.#pageCacheSizeKib,
        );
    this.#prev = this.#curr;
    this.#curr = next;
    return {prev: this.#prev, curr: this.#curr};
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
  /**
   * If this change represents a remove the row to remove,
   * if nextValue is not null then all rows that have a unique constraint
   * violation with nextValue.
   * In both cases these rows should be removed.
   */
  readonly prevValues: Readonly<Row>[];
  readonly nextValue: Readonly<Row> | null;
  readonly rowKey: RowKey;
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

/**
 * Thrown during an iteration of a {@link SnapshotDiff} when a schema
 * change or truncate is encountered, which result in aborting the
 * advancement and resetting / rehydrating the pipelines.
 */
export class ResetPipelinesSignal extends Error {
  readonly name = 'ResetPipelinesSignal';

  constructor(msg: string) {
    super(msg);
  }
}

function getSchemaVersions(db: StatementRunner, appID: string): SchemaVersions {
  return db.get(
    `SELECT minSupportedVersion, maxSupportedVersion FROM "${appID}.schemaVersions"`,
  );
}

class Snapshot {
  static create(
    lc: LogContext,
    dbFile: string,
    appID: string,
    pageCacheSizeKib: number | undefined,
  ) {
    const conn = new Database(lc, dbFile);
    conn.pragma('synchronous = OFF'); // Applied changes are ephemeral; COMMIT is never called.
    if (pageCacheSizeKib !== undefined) {
      conn.pragma(`cache_size = -${pageCacheSizeKib}`); // Negative = size in KiB
    }
    const [{journal_mode: mode}] = conn.pragma('journal_mode') as [
      {journal_mode: string},
    ];
    // The Snapshotter operates on the replica file with BEGIN CONCURRENT,
    // which must be used in concert with the replicator using BEGIN CONCURRENT
    // on a db in the wal2 journal_mode.
    assert(
      mode === 'wal2',
      `replica db must be in wal2 mode (current: ${mode})`,
    );

    const db = new StatementRunner(conn);
    return new Snapshot(db, appID);
  }

  readonly db: StatementRunner;
  readonly #appID: string;
  readonly version: string;
  readonly schemaVersions: SchemaVersions;

  constructor(db: StatementRunner, appID: string) {
    db.beginConcurrent();
    // Note: The subsequent read is necessary to acquire the read lock
    // (which results in the logical creation of the snapshot). Calling
    // `BEGIN CONCURRENT` alone does not result in acquiring the read lock.
    const {stateVersion} = getReplicationState(db);

    this.db = db;
    this.#appID = appID;
    this.version = stateVersion;
    this.schemaVersions = getSchemaVersions(db, appID);
  }

  numChangesSince(prevVersion: string) {
    const {count} = this.db.get(
      'SELECT COUNT(*) AS count FROM "_zero.changeLog" WHERE stateVersion > ?',
      prevVersion,
    );
    return count;
  }

  changesSince(prevVersion: string) {
    const cached = this.db.statementCache.get(
      'SELECT * FROM "_zero.changeLog" WHERE stateVersion > ?',
    );
    return {
      changes: cached.statement.iterate(prevVersion),
      cleanup: () => this.db.statementCache.return(cached),
    };
  }

  getRow(table: LiteTableSpecWithKeys, rowKey: JSONValue) {
    const key = normalizedKeyOrder(rowKey as RowKey);
    const conds = Object.keys(key).map(c => `${id(c)}=?`);
    const cols = Object.keys(table.columns);
    const cached = this.db.statementCache.get(
      `SELECT ${cols.map(c => id(c)).join(',')} FROM ${id(
        table.name,
      )} WHERE ${conds.join(' AND ')}`,
    );
    cached.statement.safeIntegers(true);
    try {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      return cached.statement.get<any>(Object.values(key));
    } finally {
      this.db.statementCache.return(cached);
    }
  }

  getRows(table: LiteTableSpecWithKeys, keys: PrimaryKey[], row: RowValue) {
    const conds = keys.map(key => key.map(c => `${id(c)}=?`));
    const cols = Object.keys(table.columns);
    const cached = this.db.statementCache.get(
      `SELECT ${cols.map(c => id(c)).join(',')} FROM ${id(
        table.name,
      )} WHERE ${conds.map(cond => cond.join(' AND ')).join(' OR ')}`,
    );
    cached.statement.safeIntegers(true);
    try {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      return cached.statement.all<any>(
        keys.flatMap(key => key.map(column => row[column])),
      );
    } finally {
      this.db.statementCache.return(cached);
    }
  }

  resetToHead(): Snapshot {
    this.db.rollback();
    return new Snapshot(this.db, this.#appID);
  }
}

class Diff implements SnapshotDiff {
  readonly #permissionsTable: string;
  readonly tables: Map<string, LiteAndZqlSpec>;
  readonly prev: Snapshot;
  readonly curr: Snapshot;
  readonly changes: number;

  constructor(
    appID: string,
    tables: Map<string, LiteAndZqlSpec>,
    prev: Snapshot,
    curr: Snapshot,
  ) {
    this.#permissionsTable = `${appID}.permissions`;
    this.tables = tables;
    this.prev = prev;
    this.curr = curr;
    this.changes = curr.numChangesSince(prev.version);
  }

  [Symbol.iterator](): Iterator<Change> {
    const {changes, cleanup: done} = this.curr.changesSince(this.prev.version);

    const cleanup = () => {
      try {
        // Allow open iterators to clean up their state.
        changes.return?.(undefined);
      } finally {
        done();
      }
    };

    return {
      next: () => {
        try {
          for (;;) {
            const {value, done} = changes.next();
            if (done) {
              cleanup();
              return {value, done: true};
            }

            const {table, rowKey, op, stateVersion} = v.parse(value, schema);
            if (op === RESET_OP) {
              // The current map of `TableSpec`s may not have the correct or complete information.
              throw new ResetPipelinesSignal(
                `schema for table ${table} has changed`,
              );
            }
            if (op === TRUNCATE_OP) {
              // Truncates are also processed by rehydrating pipelines at current.
              throw new ResetPipelinesSignal(
                `table ${table} has been truncated`,
              );
            }
            const {tableSpec, zqlSpec} = must(this.tables.get(table));

            assert(rowKey !== null);
            const nextValue =
              op === SET_OP ? this.curr.getRow(tableSpec, rowKey) : null;
            let prevValues;
            if (nextValue) {
              prevValues = this.prev.getRows(
                tableSpec,
                tableSpec.uniqueKeys,
                nextValue,
              );
            } else {
              const prevValue = this.prev.getRow(tableSpec, rowKey);
              prevValues = prevValue ? [prevValue] : [];
            }
            if (nextValue === undefined) {
              throw new Error(
                `Missing value for ${table} ${stringify(rowKey)}`,
              );
            }
            // Sanity check detects if the diff is being accessed after the Snapshots have advanced.
            this.checkThatDiffIsValid(stateVersion, op, prevValues, nextValue);

            if (prevValues.length === 0 && nextValue === null) {
              // Filter out no-op changes (e.g. a delete of a row that does not exist in prev).
              // TODO: Consider doing this for deep-equal values.
              continue;
            }

            if (
              table === this.#permissionsTable &&
              prevValues.find(
                prevValue => prevValue.permissions !== nextValue.permissions,
              )
            ) {
              throw new ResetPipelinesSignal(
                `Permissions have changed ${
                  prevValues.find(
                    prevValue =>
                      prevValue.permissions !== nextValue.permissions,
                  ).hash
                } => ${nextValue.hash}`,
              );
            }

            // Modify the values in place when converting to ZQL rows
            // This is safe since we're the first node in the iterator chain.
            // TODO Can we get rid of these RowValue casts?
            return {
              value: {
                table,
                prevValues: prevValues.map(prevValue =>
                  fromSQLiteTypes(zqlSpec, prevValue, table),
                ),
                nextValue: nextValue
                  ? fromSQLiteTypes(zqlSpec, nextValue, table)
                  : null,
                rowKey,
              } satisfies Change,
            };
          }
        } catch (e) {
          // This control flow path is not covered by the return() method (i.e. `break`).
          cleanup();
          throw e;
        }
      },

      return: (value: unknown) => {
        cleanup();
        return {value, done: true};
      },
    };
  }

  checkThatDiffIsValid(
    stateVersion: string,
    op: string,
    prevValues: RowValue[],
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
      prevValues.findIndex(
        prevValue => (prevValue[ROW_VERSION] ?? '~') > this.prev.version,
      ) !== -1
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
