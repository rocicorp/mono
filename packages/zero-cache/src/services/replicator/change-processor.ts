import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {must} from '../../../../shared/src/must.ts';
import {
  columnDef,
  createIndexStatement,
  createTableStatement,
} from '../../db/create.ts';
import {
  computeZqlSpecs,
  listIndexes,
  listTables,
} from '../../db/lite-tables.ts';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.ts';
import type {LiteTableSpec} from '../../db/specs.ts';
import type {StatementRunner} from '../../db/statements.ts';
import {stringify} from '../../types/bigint-json.ts';
import type {LexiVersion} from '../../types/lexi-version.ts';
import {liteRow, type LiteRow, type LiteRowKey} from '../../types/lite.ts';
import {liteTableName} from '../../types/names.ts';
import {id} from '../../types/sql.ts';
import type {
  Change,
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
  IndexCreate,
  IndexDrop,
  MessageCommit,
  MessageDelete,
  MessageInsert,
  MessageRelation,
  MessageTruncate,
  MessageUpdate,
  TableCreate,
  TableDrop,
  TableRename,
} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './schema/change-log.ts';
import {
  ZERO_VERSION_COLUMN_NAME,
  updateReplicationWatermark,
} from './schema/replication-state.ts';

// 'INITIAL-SYNC' means the caller is handling the Transaction, and no change
// log entries need be written.
export type TransactionMode = 'IMMEDIATE' | 'CONCURRENT' | 'INITIAL-SYNC';

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
  readonly #txMode: TransactionMode;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  // The TransactionProcessor lazily loads table specs into this Map,
  // and reloads them after a schema change. It is cached here to avoid
  // reading them from the DB on every transaction.
  readonly #tableSpecs = new Map<string, LiteTableSpec>();

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    txMode: TransactionMode,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#txMode = txMode;
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
  processMessage(lc: LogContext, downstream: ChangeStreamData): boolean {
    const [type, message] = downstream;
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return false;
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
    return false;
  }

  #beginTransaction(
    lc: LogContext,
    commitVersion: string,
  ): TransactionProcessor {
    let start = Date.now();
    for (let i = 0; ; i++) {
      try {
        return new TransactionProcessor(
          lc,
          this.#db,
          this.#txMode,
          this.#tableSpecs,
          commitVersion,
        );
      } catch (e) {
        // The db occasionally errors with a 'database is locked' error when
        // being concurrently processed by `litestream replicate`, even with
        // a long busy_timeout. Retry once to see if any deadlock situation
        // was resolved when aborting the first attempt.
        if (e instanceof SqliteError) {
          lc.error?.(
            `${e.code} after ${Date.now() - start} ms (attempt ${i + 1})`,
            e,
          );

          if (i === 0) {
            // retry once
            start = Date.now();
            continue;
          }
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
  ): boolean {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = this.#beginTransaction(lc, must(watermark));
      return false;
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

      assert(watermark);
      tx.processCommit(msg, watermark);
      return true;
    }

    if (msg.tag === 'rollback') {
      this.#currentTx?.abort(lc);
      this.#currentTx = null;
      return false;
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
      default:
        unreachable(msg);
    }

    return false;
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
  readonly #txMode: TransactionMode;
  readonly #version: LexiVersion;
  readonly #tableSpecs: Map<string, LiteTableSpec>;

  #schemaChanged = false;

  constructor(
    lc: LogContext,
    db: StatementRunner,
    txMode: TransactionMode,
    tableSpecs: Map<string, LiteTableSpec>,
    commitVersion: LexiVersion,
  ) {
    this.#startMs = Date.now();
    this.#txMode = txMode;

    if (txMode === 'CONCURRENT') {
      // Although the Replicator / Incremental Syncer is the only writer of the replica,
      // a `BEGIN CONCURRENT` transaction is used to allow View Syncers to simulate
      // (i.e. and `ROLLBACK`) changes on historic snapshots of the database for the
      // purpose of IVM).
      //
      // This TransactionProcessor is the only logic that will actually
      // `COMMIT` any transactions to the replica.
      db.beginConcurrent();
    } else if (txMode === 'IMMEDIATE') {
      // For the backup-replicator (i.e. replication-manager), there are no View Syncers
      // and thus BEGIN CONCURRENT is not necessary. In fact, BEGIN CONCURRENT can cause
      // deadlocks with forced wal-checkpoints (which `litestream replicate` performs),
      // so it is important to use vanilla transactions in this configuration.
      db.beginImmediate();
    }
    this.#db = db;
    this.#version = commitVersion;
    this.#lc = lc.withContext('version', commitVersion);
    this.#tableSpecs = tableSpecs;

    if (this.#tableSpecs.size === 0) {
      this.#reloadTableSpecs();
    }
  }

  #reloadTableSpecs() {
    this.#tableSpecs.clear();
    // zqlSpecs include the primary key derived from unique indexes
    const zqlSpecs = computeZqlSpecs(this.#lc, this.#db.db);
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
      relation.replicaIdentity !== 'full'
        ? relation.keyColumns // already a suitable key
        : this.#tableSpec(liteTableName(relation)).primaryKey;
    if (!keyColumns?.length) {
      throw new Error(
        `Cannot replicate table "${relation.name}" without a PRIMARY KEY or UNIQUE INDEX`,
      );
    }
    // For the common case (replica identity default), the row is already the
    // key for deletes and updates, in which case a new object can be avoided.
    return numCols === keyColumns.length
      ? row
      : Object.fromEntries(keyColumns.map(col => [col, row[col]]));
  }

  processInsert(insert: MessageInsert) {
    const table = liteTableName(insert.relation);
    const newRow = liteRow(insert.new, this.#tableSpec(table));
    const row = {
      ...newRow.row,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    const columns = Object.keys(row).map(c => id(c));
    this.#db.run(
      `
      INSERT INTO ${id(table)} (${columns.join(',')})
        VALUES (${new Array(columns.length).fill('?').join(',')})
      `,
      Object.values(row),
    );

    if (insert.relation.keyColumns.length === 0) {
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
    this.#logSetOp(table, key);
  }

  processUpdate(update: MessageUpdate) {
    const table = liteTableName(update.relation);
    const newRow = liteRow(update.new, this.#tableSpec(table));
    const row = {
      ...newRow.row,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    // update.key is set with the old values if the key has changed.
    const oldKey = update.key
      ? this.#getKey(liteRow(update.key, this.#tableSpec(table)), update)
      : null;
    const newKey = this.#getKey(newRow, update);
    const currKey = oldKey ?? newKey;
    const setExprs = Object.keys(row).map(col => `${id(col)}=?`);
    const conds = Object.keys(currKey).map(col => `${id(col)}=?`);

    this.#db.run(
      `
      UPDATE ${id(table)}
        SET ${setExprs.join(',')}
        WHERE ${conds.join(' AND ')}
      `,
      [...Object.values(row), ...Object.values(currKey)],
    );

    if (oldKey) {
      this.#logDeleteOp(table, oldKey);
    }
    this.#logSetOp(table, newKey);
  }

  processDelete(del: MessageDelete) {
    const table = liteTableName(del.relation);
    const rowKey = this.#getKey(liteRow(del.key, this.#tableSpec(table)), del);

    const conds = Object.keys(rowKey).map(col => `${id(col)}=?`);
    this.#db.run(
      `DELETE FROM ${id(table)} WHERE ${conds.join(' AND ')}`,
      Object.values(rowKey),
    );

    if (this.#txMode !== 'INITIAL-SYNC') {
      this.#logDeleteOp(table, rowKey);
    }
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
    const table = mapPostgresToLite(create.spec);
    this.#db.db.exec(createTableStatement(table));

    this.#logResetOp(table.name);
    this.#lc.info?.(create.tag, table.name);
  }

  processRenameTable(rename: TableRename) {
    const oldName = liteTableName(rename.old);
    const newName = liteTableName(rename.new);
    this.#db.db.exec(`ALTER TABLE ${id(oldName)} RENAME TO ${id(newName)}`);

    this.#bumpVersions(newName);
    this.#logResetOp(oldName);
    this.#lc.info?.(rename.tag, oldName, newName);
  }

  processAddColumn(msg: ColumnAdd) {
    const table = liteTableName(msg.table);
    const {name} = msg.column;
    const spec = mapPostgresToLiteColumn(table, msg.column);
    this.#db.db.exec(
      `ALTER TABLE ${id(table)} ADD ${id(name)} ${columnDef(spec)}`,
    );

    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, msg.column);
  }

  processUpdateColumn(msg: ColumnUpdate) {
    const table = liteTableName(msg.table);
    let oldName = msg.old.name;
    const newName = msg.new.name;

    const oldSpec = mapPostgresToLiteColumn(table, msg.old);
    const newSpec = mapPostgresToLiteColumn(table, msg.new);

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
        ALTER TABLE ${id(table)} ADD ${id(tmpName)} ${columnDef(newSpec)};
        UPDATE ${id(table)} SET ${id(tmpName)} = ${id(oldName)};
        ALTER TABLE ${id(table)} DROP ${id(oldName)};
        `);
      for (const idx of indexes) {
        // Re-create the indexes to reference the new column.
        idx.columns[tmpName] = idx.columns[oldName];
        delete idx.columns[oldName];
        stmts.push(createIndexStatement(idx));
      }
      this.#db.db.exec(stmts.join(''));
      oldName = tmpName;
    }
    if (oldName !== newName) {
      this.#db.db.exec(
        `ALTER TABLE ${id(table)} RENAME ${id(oldName)} TO ${id(newName)}`,
      );
    }
    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, msg.new);
  }

  processDropColumn(msg: ColumnDrop) {
    const table = liteTableName(msg.table);
    const {column} = msg;
    this.#db.db.exec(`ALTER TABLE ${id(table)} DROP ${id(column)}`);

    this.#bumpVersions(table);
    this.#lc.info?.(msg.tag, table, column);
  }

  processDropTable(drop: TableDrop) {
    const name = liteTableName(drop.id);
    this.#db.db.exec(`DROP TABLE IF EXISTS ${id(name)}`);

    this.#logResetOp(name);
    this.#lc.info?.(drop.tag, name);
  }

  processCreateIndex(create: IndexCreate) {
    const index = mapPostgresToLiteIndex(create.spec);
    this.#db.db.exec(createIndexStatement(index));

    // indexes affect tables visibility (e.g. sync-ability is gated on
    // having a unique index), so reset pipelines to refresh table schemas.
    this.#logResetOp(index.tableName);
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

  #logSetOp(table: string, key: LiteRowKey) {
    if (this.#txMode !== 'INITIAL-SYNC') {
      logSetOp(this.#db, this.#version, table, key);
    }
  }

  #logDeleteOp(table: string, key: LiteRowKey) {
    if (this.#txMode !== 'INITIAL-SYNC') {
      logDeleteOp(this.#db, this.#version, table, key);
    }
  }

  #logTruncateOp(table: string) {
    if (this.#txMode !== 'INITIAL-SYNC') {
      logTruncateOp(this.#db, this.#version, table);
    }
  }

  #logResetOp(table: string) {
    this.#schemaChanged = true;
    if (this.#txMode !== 'INITIAL-SYNC') {
      logResetOp(this.#db, this.#version, table);
    }
    this.#reloadTableSpecs();
  }

  processCommit(commit: MessageCommit, watermark: string) {
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

    if (this.#txMode !== 'INITIAL-SYNC') {
      this.#db.commit();
    }

    const elapsedMs = Date.now() - this.#startMs;
    this.#lc.debug?.(`Committed tx@${this.#version} (${elapsedMs} ms)`);
  }

  abort(lc: LogContext) {
    lc.info?.(`aborting transaction ${this.#version}`);
    this.#db.rollback();
  }
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  const error = new Error();
  error.cause = err;
  return error;
}
