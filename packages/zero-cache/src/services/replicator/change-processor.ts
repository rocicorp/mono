import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {mapEntries} from '../../../../shared/src/objects.ts';
import type {DownloadStatus} from '../../../../zero-events/src/status.ts';
import type {Statement} from '../../../../zqlite/src/db.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
  liteColumnDef,
} from '../../db/create.ts';
import {
  computeZqlSpecs,
  listIndexes,
  listTables,
  type LiteTableSpecWithReplicationStatus,
} from '../../db/lite-tables.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.ts';
import type {StatementRunner} from '../../db/statements.ts';
import type {LexiVersion} from '../../types/lexi-version.ts';
import {
  JSON_PARSED,
  liteRow,
  type JSONFormat,
  type LiteRow,
  type LiteRowKey,
  type LiteValueType,
} from '../../types/lite.ts';
import {liteTableName} from '../../types/names.ts';
import {normalizedKeyOrder} from '../../types/row-key.ts';
import {id} from '../../types/sql.ts';
import type {
  BackfillCompleted,
  Change,
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
  Identifier,
  IndexCreate,
  IndexDrop,
  MessageBackfill,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageRelation,
  MessageTruncate,
  MessageUpdate,
  TableCreate,
  TableDrop,
  TableRename,
  TableUpdateMetadata,
} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ReplicatorMode} from './replicator.ts';
import {
  type BatchRowOp,
  ChangeLog,
  DEL_OP,
  SET_OP,
} from './schema/change-log.ts';
import {ColumnMetadataStore} from './schema/column-metadata.ts';
import {
  ZERO_VERSION_COLUMN_NAME,
  updateReplicationWatermark,
} from './schema/replication-state.ts';
import {TableMetadataTracker} from './schema/table-metadata.ts';

export type ChangeProcessorMode = ReplicatorMode | 'initial-sync';

export type CommitResult = {
  watermark: string;
  completedBackfill: DownloadStatus | undefined;
  schemaUpdated: boolean;
  changeLogUpdated: boolean;
};

type ChangeProcessorOptions = {
  readonly cacheDmlSqlPlans?: boolean;
};

type UpsertPlan = {
  // The row's present columns define the prepared INSERT shape; partial
  // backfill/update rows must not reuse a statement for a different shape.
  readonly rowColumns: readonly string[];
  readonly sql: string;
  statement: Statement | undefined;
  // Multi-row INSERT SQL has one placeholder group per row, so the row count is
  // part of the prepared-statement shape.
  readonly batchStatements: Map<number, Statement>;
};

type UpdatePlan = {
  readonly rowColumns: readonly string[];
  readonly keyColumns: readonly string[];
  readonly sql: string;
};

type DeletePlan = {
  readonly keyColumns: readonly string[];
  readonly sql: string;
};

type PendingInsertBatch = {
  readonly table: string;
  // All rows in a batch share a table and upsert plan so SQLite sees one
  // multi-row INSERT with a fixed column list and placeholder layout.
  readonly plan: UpsertPlan;
  // Bound by SQLite's parameter limit; the version column is included in the
  // per-row binding count.
  readonly maxRows: number;
  readonly rows: LiteRow[];
  readonly logEntries: BatchRowOp[];
  // A duplicate row key in the same multi-row INSERT would make "last write
  // wins" depend on SQLite conflict behavior instead of stream order, so it
  // forces a batch boundary.
  readonly rowKeys: Set<string>;
};

// Consecutive same-shape inserts are the common catch-up/load-test burst:
// one upstream transaction can carry many rows, and each row used to cross
// JS/SQLite separately. Batching only this narrow shape keeps ordering and
// rollback behavior easy to reason about while reducing native calls.
const MAX_BATCH_BINDINGS = 900;
const CHANGE_LOG_SET_OP_BINDINGS = 4;

class DmlSqlPlanCache {
  readonly #enabled: boolean;
  // Full maps retain prepared SQL by table/column/key shape across transactions.
  readonly #upsertPlans = new Map<string, UpsertPlan>();
  readonly #updatePlans = new Map<string, UpdatePlan>();
  readonly #deletePlans = new Map<string, DeletePlan>();
  // Logical replication usually emits long runs for the same table and shape.
  // The one-entry per-table fast path avoids rebuilding a shape key for every
  // row when the previous plan is still valid.
  readonly #lastUpsertPlan = new Map<string, UpsertPlan>();
  readonly #lastUpdatePlan = new Map<string, UpdatePlan>();
  readonly #lastDeletePlan = new Map<string, DeletePlan>();

  constructor(enabled: boolean) {
    this.#enabled = enabled;
  }

  clear() {
    this.#upsertPlans.clear();
    this.#updatePlans.clear();
    this.#deletePlans.clear();
    this.#lastUpsertPlan.clear();
    this.#lastUpdatePlan.clear();
    this.#lastDeletePlan.clear();
  }

  getUpsertPlan(table: string, row: LiteRow, numCols: number): UpsertPlan {
    if (!this.#enabled) {
      return createUpsertPlan(table, columnsForRow(row, numCols));
    }

    const last = this.#lastUpsertPlan.get(table);
    if (last && rowHasColumns(row, numCols, last.rowColumns)) {
      // Reuse is safe only when the row still has every column the prepared
      // INSERT binds. Otherwise values would be bound to the wrong column list.
      return last;
    }

    const rowColumns = columnsForRow(row, numCols);
    const cacheKey = shapeKey(table, rowColumns);
    let plan = this.#upsertPlans.get(cacheKey);
    if (!plan) {
      plan = createUpsertPlan(table, rowColumns);
      this.#upsertPlans.set(cacheKey, plan);
    }
    this.#lastUpsertPlan.set(table, plan);
    return plan;
  }

  getUpdatePlan(
    table: string,
    row: LiteRow,
    numCols: number,
    keyColumns: readonly string[],
  ): UpdatePlan {
    if (!this.#enabled) {
      return createUpdatePlan(table, columnsForRow(row, numCols), keyColumns);
    }

    const last = this.#lastUpdatePlan.get(table);
    if (
      last &&
      rowHasColumns(row, numCols, last.rowColumns) &&
      sameColumns(keyColumns, last.keyColumns)
    ) {
      // UPDATE reuse also depends on the key column order because those
      // placeholders are bound after the SET values.
      return last;
    }

    const rowColumns = columnsForRow(row, numCols);
    const cacheKey = shapeKey(table, rowColumns, keyColumns);
    let plan = this.#updatePlans.get(cacheKey);
    if (!plan) {
      plan = createUpdatePlan(table, rowColumns, keyColumns);
      this.#updatePlans.set(cacheKey, plan);
    }
    this.#lastUpdatePlan.set(table, plan);
    return plan;
  }

  getDeletePlan(table: string, keyColumns: readonly string[]): DeletePlan {
    if (!this.#enabled) {
      return createDeletePlan(table, keyColumns);
    }

    const last = this.#lastDeletePlan.get(table);
    if (last && sameColumns(keyColumns, last.keyColumns)) {
      // DELETE statements bind only the key columns, so key order is the whole
      // statement shape.
      return last;
    }

    const cacheKey = shapeKey(table, keyColumns);
    let plan = this.#deletePlans.get(cacheKey);
    if (!plan) {
      plan = createDeletePlan(table, keyColumns);
      this.#deletePlans.set(cacheKey, plan);
    }
    this.#lastDeletePlan.set(table, plan);
    return plan;
  }
}

function createUpsertPlan(
  table: string,
  rowColumns: readonly string[],
): UpsertPlan {
  const insertColumns = [...rowColumns, ZERO_VERSION_COLUMN_NAME];
  const columnsSQL = insertColumns.map(c => id(c)).join(',');
  return {
    rowColumns,
    sql: /*sql*/ `
      INSERT OR REPLACE INTO ${id(table)} (${columnsSQL})
        VALUES (${placeholders(insertColumns.length)})
      `,
    statement: undefined,
    batchStatements: new Map(),
  };
}

function createBatchUpsertSql(
  table: string,
  rowColumns: readonly string[],
  rows: number,
): string {
  const insertColumns = [...rowColumns, ZERO_VERSION_COLUMN_NAME];
  const columnsSQL = insertColumns.map(c => id(c)).join(',');
  return /*sql*/ `
    INSERT OR REPLACE INTO ${id(table)} (${columnsSQL})
      VALUES ${Array.from({length: rows})
        .map(() => `(${placeholders(insertColumns.length)})`)
        .join(',')}
    `;
}

function upsertStatement(
  db: StatementRunner,
  table: string,
  plan: UpsertPlan,
  rows: number,
): Statement {
  if (rows === 1) {
    return (plan.statement ??= db.db.prepare(plan.sql));
  }

  let stmt = plan.batchStatements.get(rows);
  if (!stmt) {
    // Multi-row SQL changes with the row count because the placeholder list
    // changes, so cache by rows inside the shared plan.
    stmt = db.db.prepare(createBatchUpsertSql(table, plan.rowColumns, rows));
    plan.batchStatements.set(rows, stmt);
  }
  return stmt;
}

function createUpdatePlan(
  table: string,
  rowColumns: readonly string[],
  keyColumns: readonly string[],
): UpdatePlan {
  const setExprs = [...rowColumns, ZERO_VERSION_COLUMN_NAME].map(
    col => `${id(col)}=?`,
  );
  const conds = keyColumns.map(col => `${id(col)}=?`);
  return {
    rowColumns,
    keyColumns,
    sql: /*sql*/ `
      UPDATE ${id(table)}
        SET ${setExprs.join(',')}
        WHERE ${conds.join(' AND ')}
      `,
  };
}

function createDeletePlan(
  table: string,
  keyColumns: readonly string[],
): DeletePlan {
  const conds = keyColumns.map(col => `${id(col)}=?`);
  return {
    keyColumns,
    sql: `DELETE FROM ${id(table)} WHERE ${conds.join(' AND ')}`,
  };
}

function columnsForRow(row: LiteRow, numCols: number): string[] {
  const columns: string[] = [];
  columns.length = numCols;
  let i = 0;
  for (const col in row) {
    columns[i++] = col;
  }
  assert(i === numCols, `Expected ${numCols} columns, got ${i}`);
  return columns;
}

function rowHasColumns(
  row: LiteRow,
  numCols: number,
  columns: readonly string[],
) {
  // The row object can be reused from parsed change-source payloads; this check
  // proves the cached statement's column list still matches without allocating a
  // new column array for the common same-shape row.
  if (numCols !== columns.length) {
    return false;
  }
  for (const col of columns) {
    if (!Object.hasOwn(row, col)) {
      return false;
    }
  }
  return true;
}

function sameColumns(left: readonly string[], right: readonly string[]) {
  if (left.length !== right.length) {
    return false;
  }
  for (let i = 0; i < left.length; i++) {
    if (left[i] !== right[i]) {
      return false;
    }
  }
  return true;
}

function shapeKey(
  table: string,
  columns: readonly string[],
  extraColumns?: readonly string[],
) {
  let key = columnKeyPart(table) + columnListKeyPart(columns);
  if (extraColumns) {
    key += columnListKeyPart(extraColumns);
  }
  return key;
}

function columnListKeyPart(columns: readonly string[]) {
  // Length prefixes keep shapes unambiguous without allocating nested arrays;
  // ["ab", "c"] and ["a", "bc"] must not collide.
  let key = `#${columns.length}`;
  for (const col of columns) {
    key += columnKeyPart(col);
  }
  return key;
}

function columnKeyPart(col: string) {
  return `${col.length}:${col}`;
}

function placeholders(length: number) {
  return Array.from({length}).fill('?').join(',');
}

function upsertValues(
  row: LiteRow,
  columns: readonly string[],
  version: LexiVersion,
) {
  const values: (LiteValueType | LexiVersion)[] = [];
  values.length = columns.length + 1;
  for (let i = 0; i < columns.length; i++) {
    values[i] = row[columns[i]];
  }
  values[columns.length] = version;
  return values;
}

function updateValues(
  row: LiteRow,
  rowColumns: readonly string[],
  key: LiteRowKey,
  keyColumns: readonly string[],
  version: LexiVersion,
) {
  const values: (LiteValueType | LexiVersion)[] = [];
  values.length = rowColumns.length + keyColumns.length + 1;
  let pos = 0;
  for (const col of rowColumns) {
    values[pos++] = row[col];
  }
  values[pos++] = version;
  for (const col of keyColumns) {
    values[pos++] = key[col];
  }
  return values;
}

function keyValues(key: LiteRowKey, keyColumns: readonly string[]) {
  const values: LiteValueType[] = [];
  values.length = keyColumns.length;
  for (let i = 0; i < keyColumns.length; i++) {
    values[i] = key[keyColumns[i]];
  }
  return values;
}

function rowKeyString(key: LiteRowKey) {
  // Most replicated tables use a single-column key. Build that canonical JSON
  // directly to avoid allocating a normalized copy on every row; composite keys
  // still use normalizedKeyOrder() so column order remains stable.
  let singleColumn: string | undefined;
  let singleValue: LiteValueType = null;
  for (const col in key) {
    if (singleColumn !== undefined) {
      return stringify(normalizedKeyOrder(key));
    }
    singleColumn = col;
    singleValue = key[col];
  }
  if (singleColumn !== undefined) {
    return `{${JSON.stringify(singleColumn)}:${valueKeyString(singleValue)}}`;
  }
  return stringify(normalizedKeyOrder(key));
}

function valueKeyString(value: LiteValueType) {
  if (typeof value === 'string') {
    return JSON.stringify(value);
  }
  return stringify(value);
}

/**
 * The ChangeProcessor partitions the stream of messages into transactions
 * by creating a {@link TransactionProcessor} when a transaction begins, and dispatching
 * messages to it until the commit is received.
 *
 * From https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW :
 *
 * "The logical replication protocol sends individual transactions one by one.
 *  This means that all messages between a pair of Begin and Commit messages
 *  belong to the same transaction."
 */
export class ChangeProcessor {
  readonly #db: StatementRunner;
  readonly #changeLog: ChangeLog;
  readonly #tableMetadata: TableMetadataTracker;
  readonly #mode: ChangeProcessorMode;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  // The TransactionProcessor lazily loads table specs into this Map,
  // and reloads them after a schema change. It is cached here to avoid
  // reading them from the DB on every transaction.
  readonly #tableSpecs = new Map<string, LiteTableSpecWithReplicationStatus>();
  readonly #dmlSqlPlans: DmlSqlPlanCache;

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    mode: ChangeProcessorMode,
    failService: (lc: LogContext, err: unknown) => void,
    {cacheDmlSqlPlans = true}: ChangeProcessorOptions = {},
  ) {
    this.#db = db;
    this.#changeLog = new ChangeLog(db.db);
    this.#tableMetadata = new TableMetadataTracker(db.db);
    this.#mode = mode;
    this.#failService = failService;
    this.#dmlSqlPlans = new DmlSqlPlanCache(cacheDmlSqlPlans);
  }

  #fail(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      let failureError = err;
      try {
        this.#currentTx?.abort(lc); // roll back any pending transaction.
      } catch (rollbackError) {
        const combinedError = new Error(
          `Message processing failed and rollback also failed: operation error = ${String(err)}; rollback error = ${String(rollbackError)}`,
        );
        combinedError.cause = err;
        failureError = combinedError;
      }

      this.#failure = ensureError(failureError);

      if (!(this.#failure instanceof AbortError)) {
        // Propagate the failure up to the service.
        lc.error?.('Message Processing failed:', this.#failure);
        this.#failService(lc, this.#failure);
      }
    }
  }

  abort(lc: LogContext) {
    this.#fail(lc, new AbortError());
  }

  /** @return If a transaction was committed. */
  processMessage(
    lc: LogContext,
    downstream: ChangeStreamData,
  ): CommitResult | null {
    const [type, message] = downstream;
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return null;
    }
    try {
      const watermark =
        type === 'begin'
          ? downstream[2].commitWatermark
          : type === 'commit'
            ? downstream[2].watermark
            : undefined;
      // Begin carries the future commit watermark used as this transaction's
      // row version. Commit carries the durable replication watermark to persist.
      // Data messages intentionally carry neither; they are ordered by the open tx.
      return this.#processMessage(lc, message, watermark);
    } catch (e) {
      this.#fail(lc, e);
    }
    return null;
  }

  processMessages(
    lc: LogContext,
    downstreams: readonly ChangeStreamData[],
  ): CommitResult | readonly CommitResult[] | null {
    if (this.#failure) {
      return null;
    }

    // RM -> VS streams are row-heavy, so the common path should pay the public
    // failure/try wrapper once per stream batch rather than once per row.
    const results: CommitResult[] = [];
    try {
      for (const downstream of downstreams) {
        const [type, message] = downstream;
        const watermark =
          type === 'begin'
            ? downstream[2].commitWatermark
            : type === 'commit'
              ? downstream[2].watermark
              : undefined;
        const result = this.#processMessage(lc, message, watermark);
        if (result) {
          results.push(result);
        }
      }
    } catch (e) {
      this.#fail(lc, e);
    }

    if (results.length === 0) {
      return null;
    }
    return results.length === 1 ? results[0] : results;
  }

  #beginTransaction(
    lc: LogContext,
    commitVersion: string,
    jsonFormat: JSONFormat,
  ): TransactionProcessor {
    const start = Date.now();

    // litestream can technically hold the lock for an arbitrary amount of time
    // when checkpointing a large commit. Crashing on the busy-timeout in this
    // scenario will either produce a corrupt backup or otherwise prevent
    // replication from proceeding.
    //
    // Instead, retry the lock acquisition indefinitely. If this masks
    // an unknown deadlock situation, manual intervention will be necessary.
    for (let i = 0; ; i++) {
      try {
        return new TransactionProcessor(
          lc,
          this.#db,
          this.#mode,
          this.#changeLog,
          this.#tableMetadata,
          this.#tableSpecs,
          this.#dmlSqlPlans,
          commitVersion,
          jsonFormat,
        );
      } catch (e) {
        if (e instanceof SqliteError && e.code === 'SQLITE_BUSY') {
          lc.warn?.(
            `SQLITE_BUSY for ${Date.now() - start} ms (attempt ${i + 1}). ` +
              `This is only expected if litestream is performing a large ` +
              `checkpoint.`,
            e,
          );
          continue;
        }
        throw e;
      }
    }
  }

  /** @return If a transaction was committed. */
  #processMessage(
    lc: LogContext,
    msg: Change,
    watermark: string | undefined,
  ): CommitResult | null {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = this.#beginTransaction(
        lc,
        must(watermark),
        msg.json ?? JSON_PARSED,
      );
      return null;
    }

    // For non-begin messages, there should be a #currentTx set.
    const tx = this.#currentTx;
    if (!tx) {
      throw new Error(
        `Received message outside of transaction: ${stringify(msg)}`,
      );
    }

    if (msg.tag === 'commit') {
      assert(watermark, 'watermark is required for commit messages');
      const result = tx.processCommit(msg, watermark);
      // Undef this.#currentTx to allow the assembly of the next transaction.
      this.#currentTx = null;
      return result;
    }

    if (msg.tag === 'rollback') {
      this.#currentTx?.abort(lc);
      this.#currentTx = null;
      return null;
    }

    // Insert batches are deliberately narrow: consecutive INSERTs with the same
    // table/shape. Every other mutation flushes first so change-log positions,
    // metadata side effects, and rollback behavior remain in stream order.
    switch (msg.tag) {
      case 'insert':
        tx.processInsert(msg);
        break;
      case 'update':
        tx.flushPendingInserts();
        tx.processUpdate(msg);
        break;
      case 'delete':
        tx.flushPendingInserts();
        tx.processDelete(msg);
        break;
      case 'truncate':
        tx.flushPendingInserts();
        tx.processTruncate(msg);
        break;
      case 'create-table':
        tx.flushPendingInserts();
        tx.processCreateTable(msg);
        break;
      case 'rename-table':
        tx.flushPendingInserts();
        tx.processRenameTable(msg);
        break;
      case 'update-table-metadata':
        tx.flushPendingInserts();
        tx.processTableMetadata(msg);
        break;
      case 'add-column':
        tx.flushPendingInserts();
        tx.processAddColumn(msg);
        break;
      case 'update-column':
        tx.flushPendingInserts();
        tx.processUpdateColumn(msg);
        break;
      case 'drop-column':
        tx.flushPendingInserts();
        tx.processDropColumn(msg);
        break;
      case 'drop-table':
        tx.flushPendingInserts();
        tx.processDropTable(msg);
        break;
      case 'create-index':
        tx.flushPendingInserts();
        tx.processCreateIndex(msg);
        break;
      case 'drop-index':
        tx.flushPendingInserts();
        tx.processDropIndex(msg);
        break;
      case 'backfill':
        tx.flushPendingInserts();
        tx.processBackfill(msg);
        break;
      case 'backfill-completed':
        tx.flushPendingInserts();
        tx.processBackfillCompleted(msg);
        break;
      default:
        unreachable(msg);
    }

    return null;
  }
}

/**
 * The {@link TransactionProcessor} handles the sequence of messages from
 * upstream, from `BEGIN` to `COMMIT` and executes the corresponding mutations
 * on the {@link postgres.TransactionSql} on the replica.
 *
 * When applying row contents to the replica, the `_0_version` column is added / updated,
 * and a corresponding entry in the `ChangeLog` is added. The version value is derived
 * from the watermark of the preceding transaction (stored as the `nextStateVersion` in the
 * `ReplicationState` table).
 *
 *   Side note: For non-streaming Postgres transactions, the commitEndLsn (and thus
 *   commit watermark) is available in the `begin` message, so it could theoretically
 *   be used for the row version of changes within the transaction. However, the
 *   commitEndLsn is not available in the streaming (in-progress) transaction
 *   protocol, and may not be available for CDC streams of other upstream types.
 *   Therefore, the zero replication protocol is designed to not require the commit
 *   watermark when a transaction begins.
 *
 * Also of interest is the fact that all INSERT Messages are logically applied as
 * UPSERTs. See {@link processInsert} for the underlying motivation.
 */
class TransactionProcessor {
  readonly #lc: LogContext;
  readonly #startMs: number;
  readonly #db: StatementRunner;
  readonly #mode: ChangeProcessorMode;
  readonly #version: LexiVersion;
  readonly #changeLog: ChangeLog;
  readonly #tableMetadata: TableMetadataTracker;
  readonly #tableSpecs: Map<string, LiteTableSpecWithReplicationStatus>;
  readonly #dmlSqlPlans: DmlSqlPlanCache;
  readonly #jsonFormat: JSONFormat;
  readonly #columnMetadata: ColumnMetadataStore;

  #pos = 0;
  #schemaChanged = false;
  #numChangeLogEntries = 0;
  #pendingInsertBatch: PendingInsertBatch | undefined;

  constructor(
    lc: LogContext,
    db: StatementRunner,
    mode: ChangeProcessorMode,
    changeLog: ChangeLog,
    tableMetadata: TableMetadataTracker,
    tableSpecs: Map<string, LiteTableSpecWithReplicationStatus>,
    dmlSqlPlans: DmlSqlPlanCache,
    commitVersion: LexiVersion,
    jsonFormat: JSONFormat,
  ) {
    this.#startMs = Date.now();
    this.#mode = mode;
    this.#jsonFormat = jsonFormat;

    switch (mode) {
      case 'serving':
        // Although the Replicator / Incremental Syncer is the only writer of the replica,
        // a `BEGIN CONCURRENT` transaction is used to allow View Syncers to simulate
        // (i.e. and `ROLLBACK`) changes on historic snapshots of the database for the
        // purpose of IVM).
        //
        // This TransactionProcessor is the only logic that will actually
        // `COMMIT` any transactions to the replica.
        db.beginConcurrent();
        break;
      case 'backup':
        // For the backup-replicator (i.e. replication-manager), there are no View Syncers
        // and thus BEGIN CONCURRENT is not necessary. In fact, BEGIN CONCURRENT can cause
        // deadlocks with forced wal-checkpoints (which `litestream replicate` performs),
        // so it is important to use vanilla transactions in this configuration.
        db.beginImmediate();
        break;
      case 'initial-sync':
        // When the ChangeProcessor is used for initial-sync, the calling code
        // handles the transaction boundaries.
        break;
      default:
        unreachable();
    }
    this.#db = db;
    this.#version = commitVersion;
    this.#lc = lc.withContext('version', commitVersion);
    this.#changeLog = changeLog;
    this.#tableMetadata = tableMetadata;
    this.#tableSpecs = tableSpecs;
    this.#dmlSqlPlans = dmlSqlPlans;
    // The column_metadata table is guaranteed to exist since the
    // replica-schema.ts migration to v8.
    this.#columnMetadata = must(ColumnMetadataStore.getInstance(db.db));

    if (this.#tableSpecs.size === 0) {
      this.#reloadTableSpecs();
    }
  }

  #reloadTableSpecs() {
    this.#tableSpecs.clear();
    this.#dmlSqlPlans.clear();
    // zqlSpecs include the primary key derived from unique indexes
    const zqlSpecs = computeZqlSpecs(this.#lc, this.#db.db, {
      includeBackfillingColumns: true,
    });
    for (let spec of listTables(this.#db.db)) {
      if (!spec.primaryKey) {
        spec = {
          ...spec,
          primaryKey: [
            ...(zqlSpecs.get(spec.name)?.tableSpec.primaryKey ?? []),
          ],
        };
      }
      this.#tableSpecs.set(spec.name, spec);
    }
  }

  #tableSpec(name: string) {
    return must(this.#tableSpecs.get(name), `Unknown table ${name}`);
  }

  #getKeyColumns({relation}: {relation: MessageRelation}) {
    const keyColumns =
      relation.rowKey.type !== 'full'
        ? relation.rowKey.columns // already a suitable key
        : this.#tableSpec(liteTableName(relation)).primaryKey;
    if (!keyColumns?.length) {
      throw new Error(
        `Cannot replicate table "${relation.name}" without a PRIMARY KEY or UNIQUE INDEX`,
      );
    }
    return keyColumns;
  }

  #getKey(
    {row, numCols}: {row: LiteRow; numCols: number},
    {relation}: {relation: MessageRelation},
    keyColumns = this.#getKeyColumns({relation}),
  ): LiteRowKey {
    // For the common case (replica identity default), the row is already the
    // key for deletes and updates, in which case a new object can be avoided.
    if (numCols === keyColumns.length) {
      return row;
    }
    const key: Record<string, LiteValueType> = {};
    for (const col of keyColumns) {
      key[col] = row[col];
    }
    return key;
  }

  processInsert(insert: MessageInsert) {
    const table = liteTableName(insert.relation);
    const tableSpec = this.#tableSpec(table);
    const newRow = liteRow(insert.new, tableSpec, this.#jsonFormat);

    if (insert.relation.rowKey.columns.length === 0) {
      // INSERTs can be replicated for rows without a PRIMARY KEY or a
      // UNIQUE INDEX. These are written to the replica but not recorded
      // in the changeLog, because these rows cannot participate in IVM.
      //
      // (Once the table schema has been corrected to include a key, the
      //  associated schema change will reset pipelines and data can be
      //  loaded via hydration.)
      this.#queueInsert(table, newRow, undefined, undefined);
      return;
    }
    const key = this.#getKey(newRow, insert);
    const backfilledColumns = getBackfilledColumns(newRow.row, tableSpec);
    if (backfilledColumns !== undefined) {
      // Backfill markers merge with column metadata in the change log, so this
      // row takes the single-row path instead of the plain SET-op batch.
      this.#flushPendingInserts();
      this.#upsert(table, newRow);
      this.#logSetOp(table, key, backfilledColumns);
      return;
    }
    this.#queueInsert(table, newRow, key, rowKeyString(key));
  }

  #upsert(table: string, {row, numCols}: {row: LiteRow; numCols: number}) {
    const plan = this.#dmlSqlPlans.getUpsertPlan(table, row, numCols);
    this.#db.run(plan.sql, upsertValues(row, plan.rowColumns, this.#version));
  }

  #queueInsert(
    table: string,
    newRow: {row: LiteRow; numCols: number},
    key: LiteRowKey | undefined,
    rowKey: string | undefined,
  ) {
    const plan = this.#dmlSqlPlans.getUpsertPlan(
      table,
      newRow.row,
      newRow.numCols,
    );
    const upsertMaxRows = Math.floor(
      MAX_BATCH_BINDINGS / (plan.rowColumns.length + 1),
    );
    const logMaxRows =
      this.#mode === 'serving' && key !== undefined && rowKey !== undefined
        ? Math.floor(MAX_BATCH_BINDINGS / CHANGE_LOG_SET_OP_BINDINGS)
        : Number.POSITIVE_INFINITY;
    const maxRows = Math.max(1, Math.min(upsertMaxRows, logMaxRows));
    const batch = this.#pendingInsertBatch;
    if (batch) {
      const tableChanged = batch.table !== table;
      const shapeChanged = batch.plan.sql !== plan.sql;
      const batchFull = batch.rows.length >= batch.maxRows;
      const duplicateRowInBatch =
        rowKey !== undefined && batch.rowKeys.has(rowKey);
      if (tableChanged || shapeChanged || batchFull || duplicateRowInBatch) {
        // A multi-row INSERT has one target table and one column shape. It also
        // must not contain the same row key twice, because stream order should
        // decide repeated writes to a row, not SQLite conflict resolution inside
        // one statement.
        this.#flushPendingInserts();
      }
    }

    const current =
      this.#pendingInsertBatch ??
      (this.#pendingInsertBatch = {
        table,
        plan,
        maxRows,
        rows: [],
        logEntries: [],
        rowKeys: new Set(),
      });

    if (rowKey !== undefined) {
      current.rowKeys.add(rowKey);
    }
    const logEntry =
      this.#mode === 'serving' && key !== undefined && rowKey !== undefined
        ? {
            pos: this.#pos++,
            table,
            rowKey,
          }
        : undefined;
    if (logEntry) {
      this.#numChangeLogEntries++;
      current.logEntries.push(logEntry);
    }
    current.rows.push(newRow.row);
  }

  #flushPendingInserts() {
    const batch = this.#pendingInsertBatch;
    if (!batch) {
      return;
    }
    this.#pendingInsertBatch = undefined;

    const values = [];
    const rowValueCount = batch.plan.rowColumns.length + 1;
    values.length = batch.rows.length * rowValueCount;
    let i = 0;
    for (const row of batch.rows) {
      for (const col of batch.plan.rowColumns) {
        values[i++] = row[col];
      }
      values[i++] = this.#version;
    }

    upsertStatement(this.#db, batch.table, batch.plan, batch.rows.length).run(
      values,
    );
    this.#changeLog.logSetOps(this.#version, batch.logEntries);
  }

  // Updates by default are applied as UPDATE commands to support partial
  // row specifications from the change source. In particular, this is needed
  // to handle updates for which unchanged TOASTed values are not sent:
  //
  // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-TUPLEDATA
  //
  // However, in certain cases an UPDATE may be received for a row that
  // was not initially synced, such as when, an existing table is added
  // to the app's publication.
  //
  // In order to facilitate "resumptive" replication, the logic falls back to
  // an INSERT if the update did not change any rows.
  processUpdate(update: MessageUpdate) {
    const table = liteTableName(update.relation);
    const tableSpec = this.#tableSpec(table);
    const newRow = liteRow(update.new, tableSpec, this.#jsonFormat);
    const keyColumns = this.#getKeyColumns(update);

    // update.key is set with the old values if the key has changed.
    const oldKey = update.key
      ? this.#getKey(
          liteRow(update.key, this.#tableSpec(table), this.#jsonFormat),
          update,
          keyColumns,
        )
      : null;
    const newKey = this.#getKey(newRow, update, keyColumns);

    if (oldKey) {
      this.#logDeleteOp(table, oldKey, tableSpec.backfilling);
    }
    this.#logSetOp(table, newKey, getBackfilledColumns(newRow.row, tableSpec));

    const currKey = oldKey ?? newKey;
    const plan = this.#dmlSqlPlans.getUpdatePlan(
      table,
      newRow.row,
      newRow.numCols,
      keyColumns,
    );

    const {changes} = this.#db.run(
      plan.sql,
      updateValues(
        newRow.row,
        plan.rowColumns,
        currKey,
        plan.keyColumns,
        this.#version,
      ),
    );

    // If the UPDATE did not affect any rows, perform an UPSERT of the
    // new row for resumptive replication.
    if (changes === 0) {
      this.#upsert(table, newRow);
    }
  }

  processDelete(del: MessageDelete) {
    const table = liteTableName(del.relation);
    const tableSpec = this.#tableSpec(table);
    const keyColumns = this.#getKeyColumns(del);
    const rowKey = this.#getKey(
      liteRow(del.key, tableSpec, this.#jsonFormat),
      del,
      keyColumns,
    );

    this.#delete(table, rowKey, keyColumns);
    this.#logDeleteOp(table, rowKey, tableSpec.backfilling);
  }

  #delete(table: string, rowKey: LiteRowKey, keyColumns: readonly string[]) {
    const plan = this.#dmlSqlPlans.getDeletePlan(table, keyColumns);
    this.#db.run(plan.sql, keyValues(rowKey, plan.keyColumns));
  }

  flushPendingInserts() {
    this.#flushPendingInserts();
  }

  processTruncate(truncate: MessageTruncate) {
    for (const relation of truncate.relations) {
      const table = liteTableName(relation);
      // Update replica data.
      this.#db.run(`DELETE FROM ${id(table)}`);

      // Update change log.
      this.#logTruncateOp(table);
    }
  }

  processCreateTable(create: TableCreate) {
    if (create.metadata) {
      this.#tableMetadata.setUpstreamMetadata(create.spec, create.metadata);
    }
    const table = mapPostgresToLite(create.spec);
    this.#db.db.exec(createLiteTableStatement(table));

    // Write to metadata table
    for (const [colName, colSpec] of Object.entries(create.spec.columns)) {
      this.#columnMetadata.insert(
        table.name,
        colName,
        colSpec,
        create.backfill?.[colName],
      );
    }

    if (
      Object.keys(create.backfill ?? {}).length ===
      Object.keys(create.spec.columns).length
    ) {
      this.#reloadTableSpecs();
    } else {
      // Make the table visible immediately unless all of the columns are
      // being backfilled. In the backfill case, the version bump will happen
      // with the backfill is complete.
      this.#logResetOp(table.name);
    }
    this.#lc.info?.(create.tag, table.name);
  }

  processTableMetadata(msg: TableUpdateMetadata) {
    this.#tableMetadata.setUpstreamMetadata(msg.table, msg.new);
  }

  processRenameTable(rename: TableRename) {
    this.#tableMetadata.rename(rename.old, rename.new);

    const oldName = liteTableName(rename.old);
    const newName = liteTableName(rename.new);
    this.#db.db.exec(`ALTER TABLE ${id(oldName)} RENAME TO ${id(newName)}`);

    // Rename in metadata table
    this.#columnMetadata.renameTable(oldName, newName);

    this.#bumpVersions(rename.new);
    this.#logResetOp(oldName);
    this.#lc.info?.(rename.tag, oldName, newName);
  }

  processAddColumn(msg: ColumnAdd) {
    if (msg.tableMetadata) {
      this.#tableMetadata.setUpstreamMetadata(msg.table, msg.tableMetadata);
    }
    const table = liteTableName(msg.table);
    const {name} = msg.column;
    const spec = mapPostgresToLiteColumn(table, msg.column);
    this.#db.db.exec(
      `ALTER TABLE ${id(table)} ADD ${id(name)} ${liteColumnDef(spec)}`,
    );

    // Write to metadata table
    this.#columnMetadata.insert(table, name, msg.column.spec, msg.backfill);

    if (msg.backfill) {
      this.#reloadTableSpecs();
    } else {
      // Make the new column visible immediately if it's not being backfilled.
      // Otherwise, the version bump will happen with the backfill is complete.
      this.#bumpVersions(msg.table);
    }
    this.#lc.info?.(msg.tag, table, msg.column);
  }

  processUpdateColumn(msg: ColumnUpdate) {
    const table = liteTableName(msg.table);
    let oldName = msg.old.name;
    const newName = msg.new.name;

    // update-column can ignore defaults because it does not change the values
    // in existing rows.
    //
    // https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-DESC-SET-DROP-DEFAULT
    //
    // "The new default value will only apply in subsequent INSERT or UPDATE
    //  commands; it does not cause rows already in the table to change."
    //
    // This allows support for _changing_ column defaults to any expression,
    // since it does not affect what the replica needs to do.
    const oldSpec = mapPostgresToLiteColumn(table, msg.old, 'ignore-default');
    const newSpec = mapPostgresToLiteColumn(table, msg.new, 'ignore-default');

    // If neither the column name nor the SQLite data type changes, only the
    // upstream metadata needs to be updated. This includes changes such as a
    // varchar character limit, which SQLite does not enforce but a freshly
    // built replica still records.
    if (oldName === newName && oldSpec.dataType === newSpec.dataType) {
      this.#columnMetadata.update(
        table,
        msg.old.name,
        msg.new.name,
        msg.new.spec,
      );
      this.#lc.info?.(msg.tag, 'updated metadata only', oldSpec, newSpec);
      return;
    }
    // If the data type changes, we have to make a new column with the new data type
    // and copy the values over.
    if (oldSpec.dataType !== newSpec.dataType) {
      const tableSpec = must(
        listTables(this.#db.db, false, false).find(
          tableSpec => tableSpec.name === table,
        ),
      );
      const indexes = listIndexes(this.#db.db).filter(
        idx => idx.tableName === table,
      );
      const tmpTable = `tmp.${table}`;
      const newColumns = mapEntries(tableSpec.columns, (column, spec) => [
        column === oldName ? newName : column,
        column === oldName ? {...newSpec, pos: spec.pos} : spec,
      ]);
      const sourceColumns = Object.keys(tableSpec.columns);
      const destinationColumns = Object.keys(newColumns);
      const stmts = [
        createLiteTableStatement({
          ...tableSpec,
          name: tmpTable,
          columns: newColumns,
        }),
        `INSERT INTO ${id(tmpTable)} (${destinationColumns.map(id).join(',')})
         SELECT ${sourceColumns.map(id).join(',')} FROM ${id(table)};`,
        `DROP TABLE ${id(table)};`,
        `ALTER TABLE ${id(tmpTable)} RENAME TO ${id(table)};`,
        ...indexes.map(idx =>
          createLiteIndexStatement({
            ...idx,
            columns: mapEntries(idx.columns, (column, direction) => [
              column === oldName ? newName : column,
              direction,
            ]),
          }),
        ),
      ];
      this.#db.db.exec(stmts.join(''));
      oldName = newName;
    }
    if (oldName !== newName) {
      this.#db.db.exec(
        `ALTER TABLE ${id(table)} RENAME ${id(oldName)} TO ${id(newName)}`,
      );
    }

    // Update metadata table
    this.#columnMetadata.update(
      table,
      msg.old.name,
      msg.new.name,
      msg.new.spec,
    );

    this.#bumpVersions(msg.table);
    this.#lc.info?.(msg.tag, table, msg.new);
  }

  processDropColumn(msg: ColumnDrop) {
    const table = liteTableName(msg.table);
    const {column} = msg;
    this.#db.db.exec(`ALTER TABLE ${id(table)} DROP ${id(column)}`);

    // Delete from metadata table
    this.#columnMetadata.deleteColumn(table, column);

    this.#bumpVersions(msg.table);
    this.#lc.info?.(msg.tag, table, column);
  }

  processDropTable(drop: TableDrop) {
    this.#tableMetadata.drop(drop.id);

    const name = liteTableName(drop.id);
    this.#db.db.exec(`DROP TABLE IF EXISTS ${id(name)}`);

    // Delete from metadata table
    this.#columnMetadata.deleteTable(name);

    this.#logResetOp(name);
    this.#lc.info?.(drop.tag, name);
  }

  processCreateIndex(create: IndexCreate) {
    const index = mapPostgresToLiteIndex(create.spec);
    this.#db.db.exec(createLiteIndexStatement(index));

    // indexes affect tables visibility (e.g. sync-ability is gated on
    // having a unique index), so reset pipelines to refresh table schemas.
    // However, the reset is not necessary if the index is for a table
    // that is not yet visible due to backfilling.
    const tableSpec = must(this.#tableSpecs.get(index.tableName));
    if (
      (tableSpec.backfilling ?? []).length ===
      Object.entries(tableSpec.columns).length - 1 // don't count _0_version
    ) {
      this.#reloadTableSpecs();
    } else {
      this.#logResetOp(index.tableName);
    }
    this.#lc.info?.(create.tag, index.name);
  }

  processDropIndex(drop: IndexDrop) {
    const name = liteTableName(drop.id);
    this.#db.db.exec(`DROP INDEX IF EXISTS ${id(name)}`);
    this.#lc.info?.(drop.tag, name);
  }

  #bumpVersions(table: Identifier) {
    this.#tableMetadata.setMinRowVersion(table, this.#version);
    this.#logResetOp(liteTableName(table));
  }

  /**
   * @param backfilledColumns `backfilling` columns for which values were set
   */
  #logSetOp(
    table: string,
    key: LiteRowKey,
    backfilledColumns: string[] | undefined,
  ) {
    // The "serving" replicator always writes to the change-log (for IVM).
    // The "backup" replicator only needs to write to the change log
    // when writing columns that are being backfilled.
    if (this.#mode === 'serving' || backfilledColumns !== undefined) {
      this.#changeLog.logSetOp(
        this.#version,
        this.#pos++,
        table,
        key,
        backfilledColumns,
      );
      this.#numChangeLogEntries++;
    }
  }

  #logDeleteOp(table: string, key: LiteRowKey, backfilling?: string[]) {
    // The "serving" replicator always writes to the change-log (for IVM).
    // The "backup" replicator only needs to write to the change log
    // when writing columns that are being backfilled.
    if (this.#mode === 'serving' || backfilling?.length) {
      this.#changeLog.logDeleteOp(this.#version, this.#pos++, table, key);
      this.#numChangeLogEntries++;
    }
  }

  #logTruncateOp(table: string) {
    if (this.#mode === 'serving') {
      this.#changeLog.logTruncateOp(this.#version, table);
      this.#numChangeLogEntries++;
    }
  }

  #logResetOp(table: string) {
    this.#schemaChanged = true;
    if (this.#mode === 'serving') {
      this.#changeLog.logResetOp(this.#version, table);
      this.#numChangeLogEntries++;
    }
    this.#reloadTableSpecs();
  }

  processBackfill({relation, watermark, columns, rowValues}: MessageBackfill) {
    const tableName = liteTableName(relation);
    const tableSpec = must(this.#tableSpecs.get(tableName));
    const rowKeyCols = relation.rowKey.columns;
    const cols = [...rowKeyCols, ...columns];

    // Common parts of the INSERT sql statement.
    const insertColsStr = [...cols, ZERO_VERSION_COLUMN_NAME].map(id).join(',');
    const qMarks = Array.from({length: cols.length + 1})
      .fill('?')
      .join(',');
    const rowKeyColsStr = rowKeyCols.map(id).join(',');

    let backfilled = 0;
    let skipped = 0;
    for (const v of rowValues) {
      const row = liteRow(
        Object.fromEntries(cols.map((c, i) => [c, v[i]])),
        tableSpec,
        this.#jsonFormat,
      );
      const rowKey = this.#getKey(row, {relation});
      const rowOp = this.#changeLog.getLatestRowOp(tableName, rowKey);
      if (rowOp?.op === DEL_OP && rowOp.stateVersion > watermark) {
        skipped++;
        continue; // the row was deleted after the backfill snapshot
      }
      const updates =
        rowOp?.op === SET_OP
          ? cols.filter(
              c => (rowOp.backfillingColumnVersions[c] ?? '') <= watermark,
            )
          : cols;
      if (updates.length === 0) {
        // row already has newer values for all backfilling columns.
        skipped++;
        continue;
      }
      const updateStmts = updates.map(col => `${id(col)}=excluded.${id(col)}`);
      this.#db.run(
        /*sql*/ `
        INSERT INTO ${id(tableName)} (${insertColsStr}) VALUES (${qMarks})
          ON CONFLICT (${rowKeyColsStr})
          DO UPDATE SET ${updateStmts.join(',')};
      `,
        ...Object.values(row.row),
        watermark, // the _0_version for new rows (i.e. table backfill)
      );
      backfilled++;
    }

    this.#lc.debug?.(
      `backfilled ${backfilled} rows (skipped ${skipped}) into ${tableName}`,
    );
  }

  #completedBackfill: DownloadStatus | undefined;

  processBackfillCompleted({relation, columns, status}: BackfillCompleted) {
    const tableName = liteTableName(relation);
    const rowKeyCols = relation.rowKey.columns;
    const cols = [...rowKeyCols, ...columns];

    const columnMetadata = must(ColumnMetadataStore.getInstance(this.#db.db));
    for (const col of cols) {
      columnMetadata.clearBackfilling(tableName, col);
    }
    // Given that new columns are being exposed for every row in the table, bump the
    // row version for all rows.
    this.#bumpVersions(relation);
    if (status) {
      this.#completedBackfill = {table: tableName, columns: cols, ...status};
    }
    this.#lc.info?.(`finished backfilling ${tableName}`);

    // Note that there is no need to clear the backfillingColumnVersions values
    // in the changeLog. It could theoretically be done for clarity but:
    // (1) it could be non-trivial in terms of latency introduced and
    // (2) the data must be preserved if _other_ columns are in the process
    //     of being backfilled
    //
    // Thus, for speed and simplicity, the values are left as is. (Note that
    // subsequent replicated changes to those rows will clear the values if
    // no backfills are in progress).
  }

  processCommit(commit: MessageCommit, watermark: string): CommitResult {
    if (watermark !== this.#version) {
      throw new Error(
        `'commit' version ${watermark} does not match 'begin' version ${
          this.#version
        }: ${stringify(commit)}`,
      );
    }
    this.#flushPendingInserts();
    updateReplicationWatermark(this.#db, watermark);

    if (this.#schemaChanged) {
      const start = Date.now();
      this.#db.db.pragma('optimize');
      this.#lc.info?.(
        `PRAGMA optimized after schema change (${Date.now() - start} ms)`,
      );
    }

    if (this.#mode !== 'initial-sync') {
      this.#db.commit();
    }

    const elapsedMs = Date.now() - this.#startMs;
    this.#lc.debug?.(`Committed tx@${this.#version} (${elapsedMs} ms)`);

    return {
      watermark,
      completedBackfill: this.#completedBackfill,
      schemaUpdated: this.#schemaChanged,
      changeLogUpdated: this.#numChangeLogEntries > 0,
    };
  }

  abort(lc: LogContext) {
    lc.info?.(`aborting transaction ${this.#version}`);
    this.#db.rollback();
  }
}

function getBackfilledColumns(
  row: LiteRow,
  {backfilling}: LiteTableSpecWithReplicationStatus,
): string[] | undefined {
  if (!backfilling?.length) {
    return undefined; // common case
  }
  return backfilling.filter(col => col in row);
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const error = new Error();
  error.cause = err;
  return error;
}
