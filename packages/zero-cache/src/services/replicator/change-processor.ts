import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
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
import type {LiteTableSpec} from '../../db/specs.ts';
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
import {id} from '../../types/sql.ts';
import type {
  Change,
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
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
import {ChangeLog, DEL_OP, SET_OP} from './schema/change-log.ts';
import {ColumnMetadataStore} from './schema/column-metadata.ts';
import {
  ZERO_VERSION_COLUMN_NAME,
  updateReplicationWatermark,
} from './schema/replication-state.ts';
import {TableMetadataTracker} from './schema/table-metadata.ts';

export type ChangeProcessorMode = ReplicatorMode | 'initial-sync';

export type CommitResult = {
  watermark: string;
  schemaUpdated: boolean;
  changeLogUpdated: boolean;
};

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
  readonly #tableSpecs = new Map<string, LiteTableSpec>();

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    mode: ChangeProcessorMode,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#changeLog = new ChangeLog(db.db);
    this.#tableMetadata = new TableMetadataTracker(db.db);
    this.#mode = mode;
    this.#failService = failService;
  }

  #fail(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      this.#currentTx?.abort(lc); // roll back any pending transaction.

      this.#failure = ensureError(err);

      if (!(err instanceof AbortError)) {
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
      return this.#processMessage(lc, message, watermark);
    } catch (e) {
      this.#fail(lc, e);
    }
    return null;
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
      // Undef this.#currentTx to allow the assembly of the next transaction.
      this.#currentTx = null;

      assert(watermark, 'watermark is required for commit messages');
      const {schemaUpdated, changeLogUpdated} = tx.processCommit(
        msg,
        watermark,
      );
      return {watermark, schemaUpdated, changeLogUpdated};
    }

    if (msg.tag === 'rollback') {
      this.#currentTx?.abort(lc);
      this.#currentTx = null;
      return null;
    }

    switch (msg.tag) {
      case 'insert':
        tx.processInsert(msg);
        break;
      case 'update':
        tx.processUpdate(msg);
        break;
      case 'delete':
        tx.processDelete(msg);
        break;
      case 'truncate':
        tx.processTruncate(msg);
        break;
      case 'create-table':
        tx.processCreateTable(msg);
        break;
      case 'rename-table':
        tx.processRenameTable(msg);
        break;
      case 'update-table-metadata':
        tx.processTableMetadata(msg);
        break;
      case 'add-column':
        tx.processAddColumn(msg);
        break;
      case 'update-column':
        tx.processUpdateColumn(msg);
        break;
      case 'drop-column':
        tx.processDropColumn(msg);
        break;
      case 'drop-table':
        tx.processDropTable(msg);
        break;
      case 'create-index':
        tx.processCreateIndex(msg);
        break;
      case 'drop-index':
        tx.processDropIndex(msg);
        break;
      case 'backfill':
        tx.processBackfill(msg);
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
  readonly #jsonFormat: JSONFormat;
  readonly #columnMetadata: ColumnMetadataStore;

  #pos = 0;
  #schemaChanged = false;
  #numChangeLogEntries = 0;

  constructor(
    lc: LogContext,
    db: StatementRunner,
    mode: ChangeProcessorMode,
    changeLog: ChangeLog,
    tableMetadata: TableMetadataTracker,
    tableSpecs: Map<string, LiteTableSpecWithReplicationStatus>,
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
    // The column_metadata table is guaranteed to exist since the
    // replica-schema.ts migration to v8.
    this.#columnMetadata = must(ColumnMetadataStore.getInstance(db.db));

    if (this.#tableSpecs.size === 0) {
      this.#reloadTableSpecs();
    }
  }

  #reloadTableSpecs() {
    this.#tableSpecs.clear();
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

  #getKey(
    {row, numCols}: {row: LiteRow; numCols: number},
    {relation}: {relation: MessageRelation},
  ): LiteRowKey {
    const keyColumns =
      relation.rowKey.type !== 'full'
        ? relation.rowKey.columns // already a suitable key
        : this.#tableSpec(liteTableName(relation)).primaryKey;
    if (!keyColumns?.length) {
      throw new Error(
        `Cannot replicate table "${relation.name}" without a PRIMARY KEY or UNIQUE INDEX`,
      );
    }
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

    this.#upsert(table, {
      ...newRow.row,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    });

    if (insert.relation.rowKey.columns.length === 0) {
      // INSERTs can be replicated for rows without a PRIMARY KEY or a
      // UNIQUE INDEX. These are written to the replica but not recorded
      // in the changeLog, because these rows cannot participate in IVM.
      //
      // (Once the table schema has been corrected to include a key, the
      //  associated schema change will reset pipelines and data can be
      //  loaded via hydration.)
      return;
    }
    const key = this.#getKey(newRow, insert);
    this.#logSetOp(table, key, getBackfilledColumns(newRow.row, tableSpec));
  }

  #upsert(table: string, row: LiteRow) {
    const columns = Object.keys(row).map(c => id(c));
    this.#db.run(
      `
      INSERT OR REPLACE INTO ${id(table)} (${columns.join(',')})
        VALUES (${Array.from({length: columns.length}).fill('?').join(',')})
      `,
      Object.values(row),
    );
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
    const row = {...newRow.row, [ZERO_VERSION_COLUMN_NAME]: this.#version};

    // update.key is set with the old values if the key has changed.
    const oldKey = update.key
      ? this.#getKey(
          liteRow(update.key, this.#tableSpec(table), this.#jsonFormat),
          update,
        )
      : null;
    const newKey = this.#getKey(newRow, update);

    if (oldKey) {
      this.#logDeleteOp(table, oldKey, tableSpec.backfilling);
    }
    this.#logSetOp(table, newKey, getBackfilledColumns(newRow.row, tableSpec));

    const currKey = oldKey ?? newKey;
    const conds = Object.keys(currKey).map(col => `${id(col)}=?`);
    const setExprs = Object.keys(row).map(col => `${id(col)}=?`);

    const {changes} = this.#db.run(
      `
      UPDATE ${id(table)}
        SET ${setExprs.join(',')}
        WHERE ${conds.join(' AND ')}
      `,
      [...Object.values(row), ...Object.values(currKey)],
    );

    // If the UPDATE did not affect any rows, perform an UPSERT of the
    // new row for resumptive replication.
    if (changes === 0) {
      this.#upsert(table, row);
    }
  }

  processDelete(del: MessageDelete) {
    const table = liteTableName(del.relation);
    const tableSpec = this.#tableSpec(table);
    const rowKey = this.#getKey(
      liteRow(del.key, tableSpec, this.#jsonFormat),
      del,
    );

    this.#delete(table, rowKey);
    this.#logDeleteOp(table, rowKey, tableSpec.backfilling);
  }

  #delete(table: string, rowKey: LiteRowKey) {
    const conds = Object.keys(rowKey).map(col => `${id(col)}=?`);
    this.#db.run(
      `DELETE FROM ${id(table)} WHERE ${conds.join(' AND ')}`,
      Object.values(rowKey),
    );
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
      this.#tableMetadata.set(create.spec, create.metadata);
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
    this.#tableMetadata.set(msg.table, msg.new);
  }

  processRenameTable(rename: TableRename) {
    this.#tableMetadata.rename(rename.old, rename.new);

    const oldName = liteTableName(rename.old);
    const newName = liteTableName(rename.new);
    this.#db.db.exec(`ALTER TABLE ${id(oldName)} RENAME TO ${id(newName)}`);

    // Rename in metadata table
    this.#columnMetadata.renameTable(oldName, newName);

    this.#bumpVersions(newName);
    this.#logResetOp(oldName);
    this.#lc.info?.(rename.tag, oldName, newName);
  }

  processAddColumn(msg: ColumnAdd) {
    if (msg.tableMetadata) {
      this.#tableMetadata.set(msg.table, msg.tableMetadata);
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
      this.#bumpVersions(table);
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

    // The only updates that are relevant are the column name and the data type.
    if (oldName === newName && oldSpec.dataType === newSpec.dataType) {
      this.#lc.info?.(msg.tag, 'no thing to update', oldSpec, newSpec);
      return;
    }
    // If the data type changes, we have to make a new column with the new data type
    // and copy the values over.
    if (oldSpec.dataType !== newSpec.dataType) {
      // Remember (and drop) the indexes that reference the column.
      const indexes = listIndexes(this.#db.db).filter(
        idx => idx.tableName === table && oldName in idx.columns,
      );
      const stmts = indexes.map(idx => `DROP INDEX IF EXISTS ${id(idx.name)};`);
      const tmpName = `tmp.${newName}`;
      stmts.push(`
        ALTER TABLE ${id(table)} ADD ${id(tmpName)} ${liteColumnDef(newSpec)};
        UPDATE ${id(table)} SET ${id(tmpName)} = ${id(oldName)};
        ALTER TABLE ${id(table)} DROP ${id(oldName)};
        `);
      for (const idx of indexes) {
        // Re-create the indexes to reference the new column.
        idx.columns[tmpName] = idx.columns[oldName];
        delete idx.columns[oldName];
        stmts.push(createLiteIndexStatement(idx));
      }
      this.#db.db.exec(stmts.join(''));
      oldName = tmpName;
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

    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, msg.new);
  }

  processDropColumn(msg: ColumnDrop) {
    const table = liteTableName(msg.table);
    const {column} = msg;
    this.#db.db.exec(`ALTER TABLE ${id(table)} DROP ${id(column)}`);

    // Delete from metadata table
    this.#columnMetadata.deleteColumn(table, column);

    this.#bumpVersions(table);
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

  #bumpVersions(table: string) {
    this.#db.run(
      `UPDATE ${id(table)} SET ${id(ZERO_VERSION_COLUMN_NAME)} = ?`,
      this.#version,
    );
    this.#logResetOp(table);
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

  processBackfill({
    relation,
    watermark,
    columns,
    rowValues,
    completed,
  }: MessageBackfill) {
    const tableName = liteTableName(relation);
    const tableSpec = must(this.#tableSpecs.get(tableName));
    const rowKeyCols = relation.rowKey.columns;
    const cols = [...rowKeyCols, ...columns];

    // Common parts of the INSERT sql statement.
    const insertColsStr = [...cols, ZERO_VERSION_COLUMN_NAME].map(id).join(',');
    const qMarks = Array.from({length: cols.length + 1}, () => '?').join(',');
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
          ? columns.filter(
              c => (rowOp.backfillingColumnVersions[c] ?? '') <= watermark,
            )
          : columns;
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

    if (completed) {
      const columnMetadata = must(ColumnMetadataStore.getInstance(this.#db.db));
      for (const col of cols) {
        columnMetadata.clearBackfilling(tableName, col);
      }
      // Given that new columns are being exposed for every row in the table, bump the
      // row version for all rows.
      this.#bumpVersions(tableName);
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
  }

  processCommit(
    commit: MessageCommit,
    watermark: string,
  ): {schemaUpdated: boolean; changeLogUpdated: boolean} {
    if (watermark !== this.#version) {
      throw new Error(
        `'commit' version ${watermark} does not match 'begin' version ${
          this.#version
        }: ${stringify(commit)}`,
      );
    }
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
