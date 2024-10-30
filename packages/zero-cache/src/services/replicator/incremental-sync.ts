import type {LogContext} from '@rocicorp/logger';
import {SqliteError} from '@rocicorp/zero-sqlite3';
import {LogicalReplicationService} from 'pg-logical-replication';
import {AbortError} from '../../../../shared/src/abort-error.js';
import {assert, unreachable} from '../../../../shared/src/asserts.js';
import {Database} from '../../../../zqlite/src/db.js';
import {
  columnDef,
  createIndexStatement,
  createTableStatement,
} from '../../db/create.js';
import {listIndexes} from '../../db/lite-tables.js';
import {
  mapPostgresToLite,
  mapPostgresToLiteColumn,
  mapPostgresToLiteIndex,
} from '../../db/pg-to-lite.js';
import {StatementRunner} from '../../db/statements.js';
import {stringify} from '../../types/bigint-json.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {liteRow} from '../../types/lite.js';
import {liteTableName} from '../../types/names.js';
import {id} from '../../types/sql.js';
import type {Source} from '../../types/streams.js';
import type {
  ChangeStreamer,
  Downstream,
  DownstreamChange,
} from '../change-streamer/change-streamer.js';
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
  MessageTruncate,
  MessageUpdate,
  TableCreate,
  TableDrop,
  TableRename,
} from '../change-streamer/schema/change.js';
import {RunningState} from '../running-state.js';
import {Notifier} from './notifier.js';
import type {ReplicaState} from './replicator.js';
import {
  logDeleteOp,
  logResetOp,
  logSetOp,
  logTruncateOp,
} from './schema/change-log.js';
import {
  ZERO_VERSION_COLUMN_NAME,
  getReplicationVersions,
  getSubscriptionState,
  updateReplicationWatermark,
} from './schema/replication-state.js';

type TransactionMode = 'IMMEDIATE' | 'CONCURRENT';

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link MessageProcessor}.
 */
export class IncrementalSyncer {
  readonly #id: string;
  readonly #changeStreamer: ChangeStreamer;
  readonly #replica: StatementRunner;
  readonly #txMode: TransactionMode;
  readonly #notifier: Notifier;

  readonly #state = new RunningState('IncrementalSyncer');
  #service: LogicalReplicationService | undefined;

  constructor(
    id: string,
    changeStreamer: ChangeStreamer,
    replica: Database,
    txMode: TransactionMode,
  ) {
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#replica = new StatementRunner(replica);
    this.#txMode = txMode;
    this.#notifier = new Notifier();
  }

  async run(lc: LogContext) {
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} = getSubscriptionState(this.#replica);

    // Notify any waiting subscribers that the replica is ready to be read.
    this.#notifier.notifySubscribers();

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} = getSubscriptionState(this.#replica);
      const processor = new MessageProcessor(
        this.#replica,
        this.#txMode,
        (_watermark: string) => {}, // TODO: Add ACKs to ChangeStreamer API
        (lc: LogContext, err: unknown) => this.stop(lc, err),
      );

      let downstream: Source<Downstream> | undefined;
      let unregister = () => {};
      let err: unknown | undefined;

      try {
        downstream = await this.#changeStreamer.subscribe({
          id: this.#id,
          watermark,
          replicaVersion,
          initial: watermark === initialWatermark,
        });
        this.#state.resetBackoff();
        unregister = this.#state.cancelOnStop(downstream);

        for await (const message of downstream) {
          if (message[0] === 'error') {
            // Unrecoverable error. Stop the service.
            await this.stop(lc, message[1]);
            break;
          }
          if (processor.processMessage(lc, message)) {
            this.#notifier.notifySubscribers({state: 'version-ready'});
          }
        }
        processor.abort(lc);
      } catch (e) {
        err = e;
        processor.abort(lc);
      } finally {
        downstream?.cancel();
        unregister();
      }
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  async stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
    await this.#service?.stop();
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

class ReplayedTransactionError extends Error {
  readonly watermark: string;

  constructor(watermark: string, commit: MessageCommit) {
    super(`${watermark} has already been processed: ${stringify(commit)}`);
    this.watermark = watermark;
  }
}

/**
 * The {@link MessageProcessor} partitions the stream of messages into transactions
 * by creating a {@link TransactionProcessor} when a transaction begins, and dispatching
 * messages to it until the commit is received.
 *
 * From https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW :
 *
 * "The logical replication protocol sends individual transactions one by one.
 *  This means that all messages between a pair of Begin and Commit messages
 *  belong to the same transaction."
 */
// Exported for testing.
export class MessageProcessor {
  readonly #db: StatementRunner;
  readonly #txMode: TransactionMode;
  readonly #acknowledge: (watermark: string) => unknown;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    txMode: TransactionMode,
    acknowledge: (watermark: string) => unknown,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#txMode = txMode;
    this.#acknowledge = acknowledge;
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
  processMessage(lc: LogContext, downstream: DownstreamChange): boolean {
    const [type, message] = downstream;
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return false;
    }
    try {
      const watermark = type === 'commit' ? downstream[2].watermark : undefined;
      return this.#processMessage(lc, message, watermark);
    } catch (e) {
      if (e instanceof ReplayedTransactionError) {
        lc.info?.(e);
        this.#acknowledge(e.watermark);
      } else {
        this.#fail(lc, e);
      }
    }
    return false;
  }

  #beginTransaction(lc: LogContext): TransactionProcessor {
    let start = Date.now();
    for (let i = 0; ; i++) {
      try {
        return new TransactionProcessor(lc, this.#db, this.#txMode);
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

  /** @return The number of changes committed. */
  #processMessage(
    lc: LogContext,
    msg: Change,
    watermark: string | undefined,
  ): boolean {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = this.#beginTransaction(lc);
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
      const elapsedMs = tx.processCommit(msg, watermark);
      lc.debug?.(`Committed tx (${elapsedMs} ms)`);

      this.#acknowledge(watermark);
      return true;
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
  readonly #version: LexiVersion;

  constructor(lc: LogContext, db: StatementRunner, txMode: TransactionMode) {
    this.#startMs = Date.now();

    if (txMode === 'CONCURRENT') {
      // Although the Replicator / Incremental Syncer is the only writer of the replica,
      // a `BEGIN CONCURRENT` transaction is used to allow View Syncers to simulate
      // (i.e. and `ROLLBACK`) changes on historic snapshots of the database for the
      // purpose of IVM).
      //
      // This TransactionProcessor is the only logic that will actually
      // `COMMIT` any transactions to the replica.
      db.beginConcurrent();
    } else {
      // For the backup-replicator (i.e. replication-manager), there are no View Syncers
      // and thus BEGIN CONCURRENT is not necessary. In fact, BEGIN CONCURRENT can cause
      // deadlocks with forced wal-checkpoints (which `litestream replicate` performs),
      // so it is important to use vanilla transactions in this configuration.
      db.beginImmediate();
    }
    const {nextStateVersion} = getReplicationVersions(db);
    this.#db = db;
    this.#version = nextStateVersion;
    this.#lc = lc.withContext('version', nextStateVersion);
  }

  /**
   * Note: All INSERTs are processed a UPSERTs in order to properly handle
   * replayed transactions (e.g. if an acknowledgement was lost). In the case
   * of a replayed transaction, the final commit results in an rollback if the
   * watermark is earlier than what has already been processed.
   * See {@link processCommit}.
   *
   * Note that a transaction replay could theoretically be detected at the BEGIN message
   * since it contains the commitEndLsn (from which a watermark can be derived), but
   * that would not generalize to streaming transactions for which the commitEndLsn
   * is not known until STREAM COMMIT.
   *
   * This UPSERT strategy instead handles both protocols by accepting all messages and
   * making the COMMIT/ROLLBACK decision when the commit watermark is guaranteed to be
   * available.
   */
  processInsert(insert: MessageInsert) {
    const table = liteTableName(insert.relation);
    const newRow = liteRow(insert.new);
    const row = {
      ...newRow,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    const key = Object.fromEntries(
      insert.relation.keyColumns.map(col => [col, newRow[col]]),
    );
    const rawColumns = Object.keys(row);
    const keyColumns = insert.relation.keyColumns.map(c => id(c));
    const columns = rawColumns.map(c => id(c));
    const upsert = rawColumns.map(c => `${id(c)}=EXCLUDED.${id(c)}`);
    this.#db.run(
      `
      INSERT INTO ${id(table)} (${columns.join(',')})
        VALUES (${new Array(columns.length).fill('?').join(',')})
        ON CONFLICT (${keyColumns.join(',')})
        DO UPDATE SET ${upsert.join(',')}
      `,
      Object.values(row),
    );

    logSetOp(this.#db, this.#version, table, key);
  }

  processUpdate(update: MessageUpdate) {
    const table = liteTableName(update.relation);
    const newRow = liteRow(update.new);
    const row = {
      ...newRow,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    // update.key is set with the old values if the key has changed.
    const oldKey = update.key ? liteRow(update.key) : null;
    const newKey = Object.fromEntries(
      update.relation.keyColumns.map(col => [col, newRow[col]]),
    );
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
      logDeleteOp(this.#db, this.#version, table, oldKey);
    }
    logSetOp(this.#db, this.#version, table, newKey);
  }

  processDelete(del: MessageDelete) {
    // REPLICA IDENTITY DEFAULT means the `key` must be set.
    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
    assert(del.relation.replicaIdentity === 'default');
    const rowKey = liteRow(del.key);
    const table = liteTableName(del.relation);

    const conds = Object.keys(rowKey).map(col => `${id(col)}=?`);
    this.#db.run(
      `DELETE FROM ${id(table)} WHERE ${conds.join(' AND ')}`,
      Object.values(rowKey),
    );

    logDeleteOp(this.#db, this.#version, table, rowKey);
  }

  processTruncate(truncate: MessageTruncate) {
    for (const relation of truncate.relations) {
      const table = liteTableName(relation);
      // Update replica data.
      this.#db.run(`DELETE FROM ${id(table)}`);

      // Update change log.
      logTruncateOp(this.#db, this.#version, table);
    }
  }
  processCreateTable(create: TableCreate) {
    const table = mapPostgresToLite(create.spec);
    this.#db.db.exec(createTableStatement(table));

    logResetOp(this.#db, this.#version, table.name);
    this.#lc.info?.(create.tag, table.name);
  }

  processRenameTable(rename: TableRename) {
    const oldName = liteTableName(rename.old);
    const newName = liteTableName(rename.new);
    this.#db.db.exec(`ALTER TABLE ${id(oldName)} RENAME TO ${id(newName)}`);

    this.#bumpVersions(newName);
    logResetOp(this.#db, this.#version, oldName);
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

    logResetOp(this.#db, this.#version, name);
    this.#lc.info?.(drop.tag, name);
  }

  processCreateIndex(create: IndexCreate) {
    const index = mapPostgresToLiteIndex(create.spec);
    this.#db.db.exec(createIndexStatement(index));
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
    logResetOp(this.#db, this.#version, table);
  }

  processCommit(commit: MessageCommit, watermark: string) {
    const nextVersion = watermark;
    if (nextVersion <= this.#version) {
      this.#db.rollback();
      throw new ReplayedTransactionError(watermark, commit);
    }
    updateReplicationWatermark(this.#db, nextVersion);
    this.#db.commit();

    const elapsedMs = Date.now() - this.#startMs;
    return elapsedMs;
  }

  abort(lc: LogContext) {
    lc.info?.(`aborting transaction ${this.#version}`);
    this.#db.rollback();
  }
}
