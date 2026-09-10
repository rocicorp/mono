import {constants as bufferConstants} from 'node:buffer';
import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {assert, unreachable} from '../../../../shared/src/asserts.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {must} from '../../../../shared/src/must.ts';
import {mapEntries} from '../../../../shared/src/objects.ts';
import type {DownloadStatus} from '../../../../zero-events/src/status.ts';
import {
  createLiteIndexStatement,
  createLiteTableStatement,
  liteColumnDef,
} from '../../db/create.ts';
import {
  computeZqlSpecs,
  listTables,
  type LiteTableSpecWithReplicationStatus,
} from '../../db/lite-tables.ts';
import {
  isArrayColumn,
  isEnumColumn,
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.ts';
import type {ColumnSpec} from '../../db/specs.ts';
import type {StatementRunner} from '../../db/statements.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
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
import {
  stateVersionFromString,
  stateVersionToString,
} from '../../types/state-version.ts';
import type {
  BackfillCompleted,
  BackfillStarted,
  Change,
  ColumnAdd,
  ColumnDrop,
  ColumnUpdate,
  Identifier,
  IndexCreate,
  IndexDrop,
  MessageBackfill,
  MessageBegin,
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
import {BackfillingTracker} from './schema/backfilling.ts';
import {ChangeLog, DEL_OP, SET_OP} from './schema/change-log.ts';
import {ColumnMetadataStore} from './schema/column-metadata.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  ZERO_VERSION_COLUMN_NAME,
  updateReplicationWatermark,
} from './schema/replication-state.ts';
import {TableMetadataTracker} from './schema/table-metadata.ts';

export type ChangeProcessorMode = ReplicatorMode | 'initial-sync';

const BIND_VALUE_TOO_BIG_ERROR =
  'The bound string, buffer, or bigint is too big';
const MAX_SQLITE_BIND_BYTES = Math.min(
  bufferConstants.MAX_LENGTH,
  bufferConstants.MAX_STRING_LENGTH,
);

export type CommitResult = {
  watermark: string;
  completedBackfill: DownloadStatus | undefined;
  schemaUpdated: boolean;
  changeLogUpdated: boolean;
  /**
   * Millisecond epoch at which the transaction committed upstream, if the
   * ChangeSource reported one. Propagated to ViewSyncers as the origin
   * timestamp of the end-to-end serving lag measurement.
   */
  upstreamCommitTimeMs?: number | undefined;
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
  readonly #backfilling: BackfillingTracker;
  readonly #mode: ChangeProcessorMode;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  // The TransactionProcessor lazily loads table specs into this Map,
  // and reloads them after a schema change. It is cached here to avoid
  // reading them from the DB on every transaction.
  readonly #tableSpecs = new Map<string, LiteTableSpecWithReplicationStatus>();

  #currentTx: TransactionProcessor | null = null;

  /**
   * The `commitWatermark` the change-streamer sent for the open transaction,
   * which the `commit` must match. It differs from the version the
   * transaction is committed at whenever that version is replica-local
   * (see {@link #commitVersionFor}).
   */
  #currentTxWatermark: string | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    mode: ChangeProcessorMode,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#changeLog = new ChangeLog(db.db);
    this.#tableMetadata = new TableMetadataTracker(db.db);
    this.#backfilling = new BackfillingTracker(db.db);
    this.#mode = mode;
    this.#failService = failService;
  }

  #fail(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      let failureError = err;
      const rollbackErrors = [attempt(() => this.#currentTx?.abort(lc))].filter(
        e => e !== undefined,
      );

      if (rollbackErrors.length) {
        const combinedError = new Error(
          `Message processing failed and rollback also failed: operation error = ${String(err)}; rollback error = ${rollbackErrors.map(String).join('; ')}`,
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
      return this.#processMessage(lc, message, watermark);
    } catch (e) {
      this.#fail(lc, e);
    }
    return null;
  }

  /**
   * The version to commit the transaction at.
   *
   * For an ordinary transaction this is the change-streamer's commit
   * watermark. For a **backfill** transaction it is a version local to this
   * replica, because the watermark a replication-manager mints for a backfill
   * transaction — its last upstream watermark plus a minor — orders that
   * manager's stream and means nothing to another one. Two managers under the
   * same upstream watermark `M` both mint `M.1, M.2, ...` for different rows,
   * so a subscriber that carried one manager's `M.3` to another manager would
   * either land on that manager's own `M.3`, silently skipping its `M.1..M.3`,
   * or find no `M.3` at all and be told `WatermarkTooOld`, which
   * `IncrementalSyncer` answers with a full replica restore. During a backfill
   * on a quiet upstream most commits are backfill commits, so the second is the
   * common outcome.
   *
   * Committing at a local version instead means the replica's major is always
   * a real upstream commit, which is what the replicator subscribes at. The
   * local version is never below the incoming one, which preserves the
   * guarantee that a completion transaction's version is at least its
   * snapshot watermark. The `_0_version` of the backfilled rows is the
   * snapshot watermark either way, and is unaffected.
   */
  #commitVersionFor(msg: MessageBegin, watermark: string): string {
    if (!msg.backfill) {
      return watermark;
    }
    const current = getSubscriptionState(this.#db).watermark;
    if (watermark > current) {
      return watermark;
    }
    const {major, minor = 0n} = stateVersionFromString(current);
    return stateVersionToString({major, minor: BigInt(minor) + 1n});
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
          this.#backfilling,
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
      this.#currentTxWatermark = must(watermark);
      this.#currentTx = this.#beginTransaction(
        lc,
        this.#commitVersionFor(msg, this.#currentTxWatermark),
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
      if (watermark !== this.#currentTxWatermark) {
        throw new Error(
          `'commit' watermark ${watermark} does not match 'begin' watermark ` +
            `${this.#currentTxWatermark}: ${stringify(msg)}`,
        );
      }
      const result = tx.processCommit(msg, tx.version);
      // Clear only after a successful commit so #fail can roll back a commit
      // path that throws before SQLite has committed.
      this.#currentTx = null;
      this.#currentTxWatermark = null;
      return result;
    }

    if (msg.tag === 'rollback') {
      tx.abort(lc);
      this.#currentTx = null;
      this.#currentTxWatermark = null;
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
      case 'backfill-started':
        tx.processBackfillStarted(msg);
        break;
      case 'backfill-completed':
        tx.processBackfillCompleted(msg);
        break;
      default:
        unreachable(msg);
    }

    return null;
  }
}

// Counted where the guard and the following rule actually fire. A nonzero
// count is expected only while a subscriber is moving between
// replication-managers mid-backfill; a steadily climbing one means runs are
// being sent to subscribers that cannot use them.
const rowsSkippedByGuard = getOrCreateCounter(
  'replication',
  'backfill_rows_skipped_by_guard',
  "Backfilled rows dropped because none of the message's columns is in the " +
    "replica's in-flight backfill set.",
);
const completionsIgnored = getOrCreateCounter(
  'replication',
  'backfill_completions_ignored',
  'Backfill completions ignored because the replica was not following the ' +
    'run that sent them.',
);

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

  /** The version this transaction commits at; see `#commitVersionFor`. */
  get version(): LexiVersion {
    return this.#version;
  }
  readonly #changeLog: ChangeLog;
  readonly #tableMetadata: TableMetadataTracker;
  readonly #backfilling: BackfillingTracker;
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
    backfilling: BackfillingTracker,
    tableSpecs: Map<string, LiteTableSpecWithReplicationStatus>,
    commitVersion: LexiVersion,
    jsonFormat: JSONFormat,
  ) {
    this.#startMs = Date.now();
    this.#mode = mode;
    this.#jsonFormat = jsonFormat;

    switch (mode) {
      case 'serving':
        // This is the only transaction that commits to the serving replica.
        // Snapshotters use BEGIN CONCURRENT for private changes that are
        // always rolled back, while BEGIN IMMEDIATE lets this writer spill
        // dirty pages during large transactions.
        db.beginImmediate();
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
    this.#backfilling = backfilling;
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

    try {
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
    } catch (e) {
      if (
        !(e instanceof RangeError) ||
        e.message !== BIND_VALUE_TOO_BIG_ERROR
      ) {
        throw e;
      }

      const binding = [...Object.entries(row), ...Object.entries(currKey)]
        .map(
          ([column, value]) =>
            [column, oversizedValueDescription(value)] as const,
        )
        .find(([, description]) => description);
      const relationOid =
        'relationOid' in update.relation &&
        typeof update.relation.relationOid === 'number'
          ? ` relationOid=${update.relation.relationOid}`
          : '';
      const error = new Error(
        `Oversized SQLite update binding: tx=${this.#version}${relationOid} table=${update.relation.schema}.${update.relation.name} column=${binding?.[0] ?? 'unknown'}${binding?.[1] ? ` ${binding[1]}` : ''}`,
        {cause: e},
      );
      error.name = 'OversizedUpdateBindingError';
      throw error;
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
      this.#tableMetadata.setUpstreamMetadata(create.spec, create.metadata);
    }
    this.#backfilling.apply(create);
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

    this.#logResetOp(table.name);
    this.#lc.info?.(create.tag, table.name);
  }

  processTableMetadata(msg: TableUpdateMetadata) {
    this.#tableMetadata.setUpstreamMetadata(msg.table, msg.new);
    // Inert today: the fold's only op for this tag is `upsert-metadata`, which
    // the line above is the replica's interpreter of. Called anyway so that
    // every site that hands a schema change to one cookie tracker hands it to
    // both -- an exception here would be a schema change the shared fold has an
    // opinion about and this store never sees, which is the drift the fold
    // exists to make impossible.
    this.#backfilling.apply(msg);
  }

  processRenameTable(rename: TableRename) {
    this.#tableMetadata.rename(rename.old, rename.new);
    this.#backfilling.apply(rename);

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
    this.#backfilling.apply(msg);
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
    // A no-op unless this is a rename, which is the only part of a column
    // update that moves the backfill cookie.
    this.#backfilling.apply(msg);

    const table = liteTableName(msg.table);
    let oldName = msg.old.name;
    const newName = msg.new.name;

    const storageTypesDiffer = differentStorageTypes(
      msg.old.spec,
      msg.new.spec,
    );

    // If neither the column name nor the SQLite data type changes, only the
    // upstream metadata needs to be updated. This includes changes such as a
    // varchar character limit and nullability, which SQLite does not enforce
    // but a freshly built replica still records.
    if (oldName === newName && !storageTypesDiffer) {
      this.#columnMetadata.update(
        table,
        msg.old.name,
        msg.new.name,
        msg.new.spec,
      );
      if (Boolean(msg.old.spec.notNull) !== Boolean(msg.new.spec.notNull)) {
        this.#bumpVersions(msg.table);
      }
      this.#lc.info?.(msg.tag, 'updated metadata only', msg.old, msg.new);
      return;
    }
    // Storage type changes require a new column so that SQLite uses the new
    // representation for existing and future values.
    if (storageTypesDiffer) {
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
      const newLiteSpec = mapPostgresToLiteColumn(
        table,
        msg.new,
        'ignore-default',
      );
      if (oldName !== newName) {
        // Rename the column in place first so that SQLite rewrites every
        // reference to it in the table's index definitions, including the
        // WHERE clauses of partial indexes.
        this.#db.db.exec(
          `ALTER TABLE ${id(table)} RENAME ${id(oldName)} TO ${id(newName)}`,
        );
        oldName = newName;
      }
      const tableSpec = must(
        listTables(this.#db.db, false, false).find(
          tableSpec => tableSpec.name === table,
        ),
      );
      // Recreating the table drops its indexes, so capture their DDL to
      // replay afterwards. Auto-indexes (PRIMARY KEY) have no DDL and are
      // recreated by the CREATE TABLE statement itself.
      const indexDDL = this.#db.db
        .prepare(
          `SELECT sql FROM sqlite_master
             WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL
             ORDER BY rowid`,
        )
        .all<{sql: string}>(table)
        .map(({sql}) => sql);
      const tmpTable = `tmp.${table}`;
      const columns = Object.keys(tableSpec.columns);
      const stmts = [
        createLiteTableStatement({
          ...tableSpec,
          name: tmpTable,
          columns: mapEntries(tableSpec.columns, (column, spec) => [
            column,
            column === newName ? {...newLiteSpec, pos: spec.pos} : spec,
          ]),
        }),
        `INSERT INTO ${id(tmpTable)} (${columns.map(id).join(',')})
         SELECT ${columns.map(id).join(',')} FROM ${id(table)};`,
        `DROP TABLE ${id(table)};`,
        `ALTER TABLE ${id(tmpTable)} RENAME TO ${id(table)};`,
        ...indexDDL.map(sql => `${sql};`),
      ];
      this.#db.db.exec(stmts.join(''));
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
    this.#backfilling.apply(msg);

    this.#bumpVersions(msg.table);
    this.#lc.info?.(msg.tag, table, column);
  }

  processDropTable(drop: TableDrop) {
    this.#tableMetadata.drop(drop.id);
    this.#backfilling.apply(drop);

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
    this.#logResetOp(index.tableName);
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

  processBackfill(msg: MessageBackfill) {
    const {relation, watermark, columns, rowValues, runID, lastKey} = msg;
    const tableName = liteTableName(relation);
    const tableSpec = must(this.#tableSpecs.get(tableName));
    const rowKeyCols = relation.rowKey.columns;
    const table = {schema: relation.schema, name: relation.name};

    // The column guard. A run can outlive the completion of the columns it is
    // backfilling -- a subscriber that moved between replication-managers can
    // be sent rows from a snapshot older than values it already has -- so the
    // replica's own in-flight set, not the message, decides what may be
    // written. Without this, a stale `backfill` overwrites newer replicated
    // values, because no per-column version is recorded once the column's
    // backfill has completed.
    const backfilling = this.#backfilling.backfillingColumns(table);
    const writable = [...rowKeyCols, ...columns].filter(c =>
      backfilling.has(c),
    );
    if (writable.length === 0) {
      this.#lc.debug?.(
        `skipping backfill of ${tableName}: none of ` +
          `[${columns.join(',')}] is being backfilled`,
      );
      rowsSkippedByGuard.add(rowValues.length, {table: tableName});
      return;
    }

    // Every value the message carries, in message order, so that a row's
    // values can be read off `rowValues` positionally...
    const cols = [...rowKeyCols, ...columns];
    // ...but only the key and the writable columns are ever written.
    const inserted = [
      ...rowKeyCols,
      ...writable.filter(c => !rowKeyCols.includes(c)),
    ];

    // Common parts of the INSERT sql statement.
    const insertColsStr = [...inserted, ZERO_VERSION_COLUMN_NAME]
      .map(id)
      .join(',');
    const qMarks = Array.from({length: inserted.length + 1})
      .fill('?')
      .join(',');
    const rowKeyColsStr = rowKeyCols.map(id).join(',');

    let backfilled = 0;
    let skipped = 0;
    for (const v of rowValues) {
      const values = Object.fromEntries(cols.map((c, i) => [c, v[i]]));
      const row = liteRow(
        Object.fromEntries(inserted.map(c => [c, values[c]])),
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
          ? writable.filter(
              c => (rowOp.backfillingColumnVersions[c] ?? '') <= watermark,
            )
          : writable;
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

    // Advance the mark, but only for columns following this run: only then
    // does the replica know it holds every row the run sent before this batch.
    if (runID !== undefined && lastKey !== undefined) {
      for (const col of writable) {
        if (backfilling.get(col)?.runID === runID) {
          this.#backfilling.advanceMark(table, col, runID, lastKey, watermark);
        }
      }
    }

    this.#lc.debug?.(
      `backfilled ${backfilled} rows (skipped ${skipped}) into ${tableName}`,
    );
  }

  #completedBackfill: DownloadStatus | undefined;

  /**
   * Records whether this replica is *following* the announced run.
   *
   * A subscriber that has processed this announcement and every message since
   * holds every row of the run whose key sorts after `resumeFrom`. So it may
   * follow when the announcement covers everyone (`resumeFrom` null), when the
   * announcement covers exactly where it has got to (`resumeFrom` equals its
   * mark), or when it is already following this run (a re-delivery after a
   * reconnect). Otherwise it stops following, which is what keeps it from
   * honoring a completion for rows it does not have.
   *
   * The mark is never touched here: a subscriber that stops following a run
   * keeps how far it got, so that a later run can be resumed from it.
   */
  processBackfillStarted(msg: BackfillStarted) {
    const {relation, columns, runID, resumeFrom} = msg;
    const table = {schema: relation.schema, name: relation.name};
    const backfilling = this.#backfilling.backfillingColumns(table);

    let following = 0;
    for (const col of [...relation.rowKey.columns, ...columns]) {
      const state = backfilling.get(col);
      if (state === undefined) {
        continue; // not in flight here; the column guard covers it
      }
      const follows =
        resumeFrom === null ||
        state.runID === runID ||
        JSON.stringify(state.mark) === JSON.stringify(resumeFrom);
      this.#backfilling.setFollowing(table, col, follows ? runID : null);
      following += follows ? 1 : 0;
    }
    this.#lc.debug?.(
      `backfill run ${runID} of ${liteTableName(relation)} announced from ` +
        `${JSON.stringify(resumeFrom)}: following ${following} of ` +
        `${backfilling.size} in-flight column(s)`,
    );
  }

  processBackfillCompleted(msg: BackfillCompleted) {
    const {relation, columns, status, runID} = msg;
    const tableName = liteTableName(relation);
    const rowKeyCols = relation.rowKey.columns;
    const table = {schema: relation.schema, name: relation.name};

    // A completion may only complete columns this replica has in flight, and
    // (for a change source that identifies its runs) only those it is
    // following. A completion it is not following is a completion of rows it
    // does not have: honoring it would publish half a column, and its
    // unconditional version bump would reset the whole table for IVM.
    const backfilling = this.#backfilling.backfillingColumns(table);
    const cols = [...rowKeyCols, ...columns].filter(col => {
      const state = backfilling.get(col);
      return (
        state !== undefined && (runID === undefined || state.runID === runID)
      );
    });
    if (cols.length === 0) {
      this.#lc.info?.(
        `ignoring completion of backfill run ${runID} of ${tableName}: ` +
          `not following it`,
      );
      completionsIgnored.add(1, {table: tableName});
      return;
    }

    const columnMetadata = must(ColumnMetadataStore.getInstance(this.#db.db));
    for (const col of cols) {
      columnMetadata.clearBackfilling(tableName, col);
    }
    this.#backfilling.completeColumns(table, cols);
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
      upstreamCommitTimeMs: commit.commitTimeMs,
    };
  }

  abort(lc: LogContext) {
    lc.info?.(`aborting transaction ${this.#version}`);
    this.#db.rollback();
  }
}

function differentStorageTypes(
  oldSpec: ColumnSpec,
  newSpec: ColumnSpec,
): boolean {
  return (
    oldSpec.dataType !== newSpec.dataType ||
    isEnumColumn(oldSpec) !== isEnumColumn(newSpec) ||
    isArrayColumn(oldSpec) !== isArrayColumn(newSpec)
  );
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

function oversizedValueDescription(value: LiteValueType): string | undefined {
  if (typeof value === 'bigint') {
    return BigInt.asIntN(64, value) !== value
      ? 'valueType=bigint fitsInt64=false'
      : undefined;
  }

  const sizeBytes =
    typeof value === 'string'
      ? Buffer.byteLength(value)
      : value instanceof Uint8Array
        ? value.byteLength
        : undefined;
  if (sizeBytes !== undefined && sizeBytes > MAX_SQLITE_BIND_BYTES) {
    const valueType = typeof value === 'string' ? 'string' : 'buffer';
    return `valueType=${valueType} sizeBytes=${sizeBytes} limitBytes=${MAX_SQLITE_BIND_BYTES}`;
  }

  return undefined;
}

/** Runs `fn`, returning its error rather than throwing it. */
function attempt(fn: () => void): unknown {
  try {
    fn();
    return undefined;
  } catch (e) {
    return e ?? new Error('rollback failed');
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
