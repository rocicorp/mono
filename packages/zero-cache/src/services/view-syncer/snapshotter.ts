import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {stringify, type JSONValue} from '../../../../shared/src/bigint-json.ts';
import {getOrInsertComputed} from '../../../../shared/src/map.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-types/src/schema.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../../../zqlite/src/table-source.ts';
import type {
  LiteAndZqlSpec,
  LiteTableSpecWithKeysAndVersion,
} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {
  normalizedKeyOrder,
  type RowKey,
  type RowValue,
} from '../../types/row-key.ts';
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
import type {SnapshotRowCache} from './snapshot-row-cache.ts';

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
  readonly #rowCache: SnapshotRowCache | undefined;
  #curr: Snapshot | undefined;
  #prev: Snapshot | undefined;

  /**
   * @param rowCache An optional worker-wide {@link SnapshotRowCache} through
   *        which the row reads performed when iterating over a
   *        {@link SnapshotDiff} are shared with the other Snapshotters
   *        (i.e. client groups) on the worker.
   */
  constructor(
    lc: LogContext,
    dbFile: string,
    {appID}: AppID,
    pageCacheSizeKib?: number,
    rowCache?: SnapshotRowCache,
  ) {
    this.#lc = lc;
    this.#dbFile = dbFile;
    this.#appID = appID;
    this.#pageCacheSizeKib = pageCacheSizeKib;
    this.#rowCache = rowCache;
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
   *
   * @param prevWrites How the caller writes to `prev` during the iteration,
   *        which determines which `prev` reads can be shared through the
   *        {@link SnapshotRowCache}. See {@link PrevWrites}.
   */
  advance(
    syncableTables: Map<string, LiteAndZqlSpec>,
    allTableNames: Set<string>,
    observedTables?: TableFilter | undefined,
    prevWrites: PrevWrites = 'uniform',
  ): SnapshotDiff {
    const {prev, curr} = this.advanceWithoutDiff();
    return new Diff(
      this.#appID,
      syncableTables,
      allTableNames,
      prev,
      curr,
      observedTables,
      this.#rowCache,
      prevWrites,
    );
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

export interface TableFilter {
  has(table: string): boolean;
}

/**
 * How the caller of {@link Snapshotter.advance()} writes the changes of a
 * {@link SnapshotDiff} to its `prev` snapshot while iterating over it.
 *
 * A `prev` read for a table with a single unique key (its primary key)
 * cannot observe earlier changes of the iteration, since a row key appears
 * once in the diff. A read for a table with other unique keys can, because
 * the rows it returns may have been displaced or edited by earlier changes.
 *
 * * `uniform`: every caller applies every change, so such a read depends on
 *   the `<prev, curr>` versions of the diff, and is shared on that basis.
 * * `divergent`: callers apply different subsets of the changes (e.g. the
 *   pipeline driver skips changes that no pipeline can observe), so such a
 *   read depends on the caller and is not shared.
 */
export type PrevWrites = 'uniform' | 'divergent';

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
export type ResetPipelinesReason =
  | 'advancement-timeout'
  | 'scalar-subquery'
  | 'schema-change'
  | 'truncation'
  | 'permissions-change';

export class ResetPipelinesSignal extends Error {
  readonly name = 'ResetPipelinesSignal';
  readonly reason: ResetPipelinesReason;

  constructor(msg: string, reason: ResetPipelinesReason) {
    super(msg);
    this.reason = reason;
  }
}

// Memoizes, per table spec, the SQL of the row lookups performed for each
// change, which would otherwise be rebuilt from the table spec for every
// read. Handing out the same string for every read of a given shape also
// lets the SnapshotRowCache and the statement cache look it up without
// rehashing it. Specs are recreated whenever they are recomputed, so stale
// SQL is never served.
type TableReads = {
  /** `SELECT <columns> FROM <table> WHERE ` */
  readonly prefix: string;
  /** {@link Snapshot.getRow} SQL, by the NUL-joined key columns. */
  readonly byKeyColumns: Map<string, string>;
  /**
   * {@link Snapshot.getRows} SQL, by the bitmask of the table's `uniqueKeys`
   * that are queried.
   */
  readonly byUniqueKeys: Map<number, string>;
};

const tableReads = new WeakMap<LiteTableSpecWithKeysAndVersion, TableReads>();

function readsFor(table: LiteTableSpecWithKeysAndVersion): TableReads {
  return getOrInsertComputed(tableReads, table, () => ({
    prefix: `SELECT ${Object.keys(table.columns)
      .map(c => id(c))
      .join(',')} FROM ${id(table.name)} WHERE `,
    byKeyColumns: new Map(),
    byUniqueKeys: new Map(),
  }));
}

// The byUniqueKeys bitmask must fit in a (positive) 31-bit integer.
const MAX_MEMOIZED_UNIQUE_KEYS = 30;

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
    // BEGIN CONCURRENT lets the Snapshotter simulate changes on a historic
    // snapshot without taking the WAL writer lock. These private changes are
    // always rolled back; the replicator is the only committer and can use
    // BEGIN IMMEDIATE on the same WAL2 database.
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
  #reset = false;

  constructor(db: StatementRunner, appID: string) {
    db.beginConcurrent();
    // Note: The subsequent read is necessary to acquire the read lock
    // (which results in the logical creation of the snapshot). Calling
    // `BEGIN CONCURRENT` alone does not result in acquiring the read lock.
    const {stateVersion} = getReplicationState(db);

    this.db = db;
    this.#appID = appID;
    this.version = stateVersion;
  }

  numChangesSince(prevVersion: string) {
    const {count} = this.db.get(
      'SELECT COUNT(*) AS count FROM "_zero.changeLog2" WHERE stateVersion > ?',
      prevVersion,
    );
    return count;
  }

  /**
   * Whether a table-wide RESET op (i.e. a schema change) was logged after
   * `prevVersion`. Table specs computed at or before `prevVersion` are
   * stale with respect to this snapshot if so.
   */
  schemaChangedSince(prevVersion: string): boolean {
    const row = this.db.get(
      'SELECT 1 FROM "_zero.changeLog2" WHERE stateVersion > ? AND op = ? LIMIT 1',
      prevVersion,
      RESET_OP,
    );
    return row !== undefined;
  }

  /**
   * Whether any table-wide op (i.e. a RESET or TRUNCATE) was logged after
   * `prevVersion`. Iterating over a diff ending at this snapshot will
   * abort with a {@link ResetPipelinesSignal} if so.
   */
  hasTableWideOpSince(prevVersion: string): boolean {
    const row = this.db.get(
      'SELECT 1 FROM "_zero.changeLog2" WHERE stateVersion > ? AND op IN (?, ?) LIMIT 1',
      prevVersion,
      RESET_OP,
      TRUNCATE_OP,
    );
    return row !== undefined;
  }

  changesSince(prevVersion: string) {
    // Note: The queried fields are constrained to only those that are relevant
    // to the snapshot diff, i.e. those defined in the changeLogEntrySchema.
    const cached = this.db.statementCache.get(
      `SELECT "stateVersion", "table", "rowKey", "op" FROM "_zero.changeLog2"
         WHERE "stateVersion" > ? ORDER BY "stateVersion" ASC, "pos" ASC`,
    );
    return {
      changes: cached.statement.iterate(prevVersion),
      cleanup: () => this.db.statementCache.return(cached),
    };
  }

  /**
   * Reads the row with the given `rowKey`, or `undefined` if it does not
   * exist.
   *
   * @param cache An optional {@link SnapshotRowCache} through which to
   *        perform the read, along with the `tag` that identifies the
   *        version(s) of the replica that the result depends on.
   */
  getRow(
    table: LiteTableSpecWithKeysAndVersion,
    rowKey: JSONValue,
    cache?: SnapshotRowCache,
    tag?: string,
  ) {
    const key = normalizedKeyOrder(rowKey as RowKey);
    const cols = Object.keys(key);
    const reads = readsFor(table);
    const sql = getOrInsertComputed(
      reads.byKeyColumns,
      cols.join('\0'),
      () => reads.prefix + cols.map(c => `${id(c)}=?`).join(' AND '),
    );
    const args = Object.values(key);
    const read = () =>
      this.db.statementCache.use(sql, cached => {
        cached.statement.safeIntegers(true);
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        return cached.statement.get<any>(args);
      });
    return cache
      ? cache.getOrRead(tag ?? '', sql, 'get', args, () => {
          const row = read();
          if (row !== undefined) {
            this.#assertNotAdvanced(row);
          }
          return row;
        })
      : read();
  }

  /**
   * Reads the rows that conflict with `row` on any of the given unique
   * `keys` (which includes the row with the same primary key, if any).
   *
   * @param cache An optional {@link SnapshotRowCache} through which to
   *        perform the read, along with the `tag` that identifies the
   *        version(s) of the replica that the result depends on.
   */
  getRows(
    table: LiteTableSpecWithKeysAndVersion,
    keys: PrimaryKey[],
    row: RowValue,
    cache?: SnapshotRowCache,
    tag?: string,
  ) {
    // Filter out keys where any column is NULL. This is both correct and
    // critical for performance:
    // 1. Correctness: NULL values can't violate uniqueness (NULL != NULL in SQL)
    // 2. Performance: SQLite's MULTI-INDEX OR optimization completely fails when
    //    any branch involves NULL, falling back to a full table scan. This was
    //    causing slowdowns of hundreds of times on tables with nullable unique columns.
    const validKeys: PrimaryKey[] = [];
    let mask = 0;
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      if (
        key.every(column => row[column] !== null && row[column] !== undefined)
      ) {
        validKeys.push(key);
        mask |= 1 << i;
      }
    }
    if (validKeys.length === 0) {
      return [];
    }
    const reads = readsFor(table);
    const buildSQL = () =>
      reads.prefix +
      validKeys
        .map(key => key.map(c => `${id(c)}=?`).join(' AND '))
        .join(' OR ');
    const sql =
      keys === table.uniqueKeys && keys.length <= MAX_MEMOIZED_UNIQUE_KEYS
        ? getOrInsertComputed(reads.byUniqueKeys, mask, buildSQL)
        : buildSQL();
    const args = validKeys.flatMap(key => key.map(column => row[column]));
    const read = () =>
      this.db.statementCache.use(sql, cached => {
        cached.statement.safeIntegers(true);
        // oxlint-disable-next-line @typescript-eslint/no-explicit-any
        return cached.statement.all<any>(args);
      });
    return cache
      ? cache.getOrRead(tag ?? '', sql, 'all', args, () => {
          const rows = read();
          for (const row of rows) {
            this.#assertNotAdvanced(row);
          }
          return rows;
        })
      : read();
  }

  /**
   * Rows are shared with other client groups via the SnapshotRowCache, so
   * one read from a connection that has advanced past this snapshot (i.e. a
   * {@link SnapshotDiff} used after its Snapshotter advanced) must fail
   * before it is cached, rather than be served to the valid Diffs of other
   * client groups (whose own validity checks would then fail).
   */
  #assertNotAdvanced(row: RowValue) {
    if ((row[ROW_VERSION] ?? '~') > this.version) {
      throw new InvalidDiffError(
        `Diff is no longer valid. db has advanced past ${this.version}.`,
      );
    }
  }

  /**
   * Whether the connection has been reset to a newer snapshot (i.e. via
   * {@link resetToHead()}), after which reads no longer reflect
   * {@link version}.
   */
  get reset(): boolean {
    return this.#reset;
  }

  resetToHead(): Snapshot {
    this.#reset = true;
    this.db.rollback();
    return new Snapshot(this.db, this.#appID);
  }
}

class Diff implements SnapshotDiff {
  readonly #permissionsTable: string;
  readonly #syncableTables: Map<string, LiteAndZqlSpec>;
  readonly #allTableNames: Set<string>;
  readonly #observedTables: TableFilter | undefined;
  readonly #rowCache: SnapshotRowCache | undefined;
  readonly #prevWrites: PrevWrites;
  readonly prev: Snapshot;
  readonly curr: Snapshot;
  readonly changes: number;

  constructor(
    appID: string,
    syncableTables: Map<string, LiteAndZqlSpec>,
    allTableNames: Set<string>,
    prev: Snapshot,
    curr: Snapshot,
    observedTables?: TableFilter | undefined,
    rowCache?: SnapshotRowCache | undefined,
    prevWrites: PrevWrites = 'uniform',
  ) {
    this.#prevWrites = prevWrites;
    this.#permissionsTable = `${appID}.permissions`;
    this.#syncableTables = syncableTables;
    this.#allTableNames = allTableNames;
    this.#observedTables = observedTables;
    this.prev = prev;
    this.curr = curr;
    this.changes = curr.numChangesSince(prev.version);
    // Row values are only shared (via the cache) between Diffs that do not
    // straddle a table-wide op. This guarantees that any two Diffs that
    // encounter the same change log entry read the row from tables with
    // the same schema (and contents), since a RESET or TRUNCATE between
    // their `curr` versions would necessarily fall within the range of the
    // later Diff. (Such a Diff aborts with a ResetPipelinesSignal anyway.)
    this.#rowCache =
      rowCache && !curr.hasTableWideOpSince(prev.version)
        ? rowCache
        : undefined;
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
                'schema-change',
              );
            }
            if (op === TRUNCATE_OP) {
              // Truncates are also processed by rehydrating pipelines at current.
              throw new ResetPipelinesSignal(
                `table ${table} has been truncated`,
                'truncation',
              );
            }
            if (
              this.#observedTables &&
              !this.#observedTables.has(table) &&
              table !== this.#permissionsTable
            ) {
              if (this.#allTableNames.has(table)) {
                continue; // skip change log entries for unobserved tables.
              }
              throw new Error(`change for unknown table ${table}`);
            }
            const specs = this.#syncableTables.get(table);
            if (!specs) {
              if (this.#allTableNames.has(table)) {
                continue; // skip change log entries for non-syncable tables.
              }
              throw new Error(`change for unknown table ${table}`);
            }
            const {tableSpec, zqlSpec} = specs;

            // Sanity check: All change log ops should have a stateVersion
            // greater than minRowVersion in the table metadata. This is a
            // mini-proof that the overlay does not need to be applied to
            // rows produced in incremental catchup, based on the invariant in
            // change-processor's #bumpVersions(), whereby the setting of the
            // minRowVersion is always followed by a RESET OP, meaning that
            // subsequent change-log traversal happens at a later version.
            assert(
              (tableSpec.minRowVersion ?? '') < stateVersion,
              () =>
                `unexpected change @${stateVersion} for table ${table} with ` +
                `minRowVersion ${tableSpec.minRowVersion}: ${op}(${rowKey})`,
            );

            assert(rowKey !== null, 'rowKey must be present for row changes');

            // Row reads are shared with the other client groups on the
            // worker via the SnapshotRowCache (if configured):
            //
            // * The new value of the row is read from `curr`, which is
            //   never written to during the iteration, and is the version
            //   of the row at `stateVersion` (as verified by
            //   checkThatDiffIsValid()). It is thus identical for every
            //   Diff that encounters the change log entry.
            //
            // * Previous values are read from `prev`, at `prev.version`,
            //   which the caller (i.e. IVM) writes to as the iteration
            //   progresses. For a table whose only unique key is its
            //   primary key, no change applied before this one can affect
            //   this row, so the result depends solely on `prev.version`.
            //   Otherwise, the result can include rows that conflict on
            //   other unique keys, which may have been added or removed by
            //   preceding changes; see PrevWrites for when that can be
            //   shared.
            // The first advance() after this Diff was created resets the
            // `prev` connection to head. Reads would then no longer reflect
            // `prev.version`, and cache hits would not reveal it (the cached
            // rows were read by valid Diffs), so this must be checked
            // explicitly rather than inferred from the rows' versions.
            if (this.prev.reset) {
              throw new InvalidDiffError(
                `Diff is no longer valid. prev db has advanced past ${this.prev.version}.`,
              );
            }

            const cache = this.#rowCache;
            const prevDependsOnWrites = tableSpec.uniqueKeys.length > 1;
            const prevCache =
              prevDependsOnWrites && this.#prevWrites === 'divergent'
                ? undefined
                : cache;
            const prevTag = prevDependsOnWrites
              ? `p:${this.prev.version}:${this.curr.version}`
              : `p:${this.prev.version}`;
            const nextValue =
              op === SET_OP
                ? this.curr.getRow(
                    tableSpec,
                    rowKey,
                    cache,
                    `n:${stateVersion}`,
                  )
                : null;
            let prevValues;
            if (nextValue) {
              prevValues = this.prev.getRows(
                tableSpec,
                tableSpec.uniqueKeys,
                nextValue,
                prevCache,
                prevTag,
              );
            } else {
              const prevValue = this.prev.getRow(
                tableSpec,
                rowKey,
                prevCache,
                prevTag,
              );
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
              prevValues.some(
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
                'permissions-change',
              );
            }

            // Note: The raw SQLite rows may be shared with other client
            // groups via the SnapshotRowCache, so they must not be modified.
            // fromSQLiteTypes() produces new row objects.
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
