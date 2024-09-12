import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import {LogicalReplicationService, Pgoutput} from 'pg-logical-replication';
import {AbortError} from 'shared/src/abort-error.js';
import {assert, unreachable} from 'shared/src/asserts.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {liteValues} from 'zero-cache/src/types/lite.js';
import {Database} from 'zqlite/src/db.js';
import {stringify} from '../../types/bigint-json.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {liteTableName} from '../../types/names.js';
import type {Source} from '../../types/streams.js';
import {ChangeStreamer} from '../change-streamer/change-streamer.js';
import {Change, MessageCommit} from '../change-streamer/schema/change.js';
import {RunningState} from '../running-state.js';
import {Notifier} from './notifier.js';
import {ReplicaVersionReady} from './replicator.js';
import {logDeleteOp, logSetOp, logTruncateOp} from './schema/change-log.js';
import {
  ZERO_VERSION_COLUMN_NAME,
  getReplicationVersions,
  getSubscriptionState,
  updateReplicationWatermark,
} from './schema/replication-state.js';

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
  readonly #notifier: Notifier;

  readonly #state = new RunningState('IncrementalSyncer');
  #service: LogicalReplicationService | undefined;

  constructor(id: string, changeStreamer: ChangeStreamer, replica: Database) {
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#replica = new StatementRunner(replica);
    this.#notifier = new Notifier();
  }

  async run(lc: LogContext) {
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} = getSubscriptionState(this.#replica);

    // Notify any waiting subscribers that the replica is ready to be read.
    this.#notifier.notifySubscribers();

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} = getSubscriptionState(this.#replica);
      const downstream = this.#changeStreamer.subscribe({
        id: this.#id,
        watermark,
        replicaVersion,
        initial: watermark === initialWatermark,
      });

      const processor = new MessageProcessor(
        this.#replica,
        (_watermark: string) => {}, // TODO: Add ACKs to ChangeStreamer API
        () => this.#notifier.notifySubscribers(),
        (lc: LogContext, err: unknown) => this.stop(lc, err),
      );

      const unregister = this.#state.cancelOnStop(downstream);
      try {
        for await (const message of downstream) {
          if (message[0] === 'error') {
            // Unrecoverable error. Stop the service.
            await this.stop(lc, message[1]);
            break;
          }
          this.#state.resetBackoff();

          const {watermark, change} = message[1];
          processor.processMessage(lc, watermark, change);
        }
        processor.abort(lc);
      } catch (e) {
        lc.error?.('Received error from ChangeStreamer', e);
        processor.abort(lc, e);
      } finally {
        downstream.cancel();
        unregister();
      }
      await this.#state.backoff(lc);
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  subscribe(): Source<ReplicaVersionReady> {
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
  readonly #acknowledge: (watermark: string) => unknown;
  readonly #notifyVersionChange: () => void;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    acknowledge: (watermark: string) => unknown,
    notifyVersionChange: () => void,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#db = db;
    this.#acknowledge = acknowledge;
    this.#notifyVersionChange = notifyVersionChange;
    this.#failService = failService;
  }

  #fail(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      this.#currentTx?.abort(lc); // roll back any pending transaction.

      this.#failure = ensureError(err);

      if (err instanceof AbortError) {
        // Aborted by the service.
        lc.info?.('stopping MessageProcessor');
      } else {
        // Propagate the failure up to the service.
        lc.error?.('Message Processing failed:', this.#failure);
        this.#failService(lc, this.#failure);
      }
    }
  }

  abort(lc: LogContext, err?: unknown) {
    this.#fail(lc, err ?? new AbortError());
  }

  processMessage(lc: LogContext, watermark: string, message: Change) {
    lc = lc.withContext('watermark', watermark);
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return;
    }
    try {
      this.#processMessage(lc, watermark, message);
    } catch (e) {
      if (e instanceof ReplayedTransactionError) {
        lc.info?.(e);
        this.#acknowledge(e.watermark);
      } else {
        this.#fail(lc, e);
      }
    }
  }

  #processMessage(lc: LogContext, watermark: string, msg: Change) {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = new TransactionProcessor(this.#db);
      return;
    }
    // For non-begin messages, there should be a #currentTx set.
    if (!this.#currentTx) {
      throw new Error(
        `Received message outside of transaction: ${stringify(msg)}`,
      );
    }
    const tx = this.#currentTx;
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
      case 'commit': {
        // Undef this.#currentTx to allow the assembly of the next transaction.
        this.#currentTx = null;

        const elapsedMs = tx.processCommit(msg, watermark);
        lc.debug?.(`Committed tx (${elapsedMs} ms)`);

        this.#acknowledge(watermark);
        this.#notifyVersionChange();
        break;
      }

      default:
        unreachable(msg);
    }
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
  readonly #startMs: number;
  readonly #db: StatementRunner;
  readonly #version: LexiVersion;

  constructor(db: StatementRunner) {
    this.#startMs = Date.now();

    // Although the Replicator / Incremental Syncer is the only writer of the replica,
    // a `BEGIN CONCURRENT` transaction is used to allow View Syncers to simulate
    // (i.e. and `ROLLBACK`) changes on historic snapshots of the database for the
    // purpose of IVM).
    //
    // This TransactionProcessor is the only logic that will actually
    // `COMMIT` any transactions to the replica.
    db.beginConcurrent();
    const {nextStateVersion} = getReplicationVersions(db);
    this.#db = db;
    this.#version = nextStateVersion;
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
  processInsert(insert: Pgoutput.MessageInsert) {
    const table = liteTableName(insert.relation);
    const row = {
      ...insert.new,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    const key = Object.fromEntries(
      insert.relation.keyColumns.map(col => [col, insert.new[col]]),
    );
    const rawColumns = Object.keys(row);
    const keyColumns = insert.relation.keyColumns.map(c => ident(c));
    const columns = rawColumns.map(c => ident(c));
    const upsert = rawColumns.map(c => `${ident(c)}=EXCLUDED.${ident(c)}`);
    this.#db.run(
      `
      INSERT INTO ${ident(table)} (${columns.join(',')})
        VALUES (${new Array(columns.length).fill('?').join(',')})
        ON CONFLICT (${keyColumns.join(',')})
        DO UPDATE SET ${upsert.join(',')}
      `,
      liteValues(row),
    );

    logSetOp(this.#db, this.#version, table, key);
  }

  processUpdate(update: Pgoutput.MessageUpdate) {
    const table = liteTableName(update.relation);
    const row = {
      ...update.new,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    // update.key is set with the old values if the key has changed.
    const oldKey = update.key;
    const newKey = Object.fromEntries(
      update.relation.keyColumns.map(col => [col, update.new[col]]),
    );
    const currKey = oldKey ?? newKey;
    const setExprs = Object.keys(row).map(col => `${ident(col)}=?`);
    const conds = Object.keys(currKey).map(col => `${ident(col)}=?`);

    this.#db.run(
      `
      UPDATE ${ident(table)}
        SET ${setExprs.join(',')}
        WHERE ${conds.join(' AND ')}
      `,
      [...liteValues(row), ...liteValues(currKey)],
    );

    if (oldKey) {
      logDeleteOp(this.#db, this.#version, table, oldKey);
    }
    logSetOp(this.#db, this.#version, table, newKey);
  }

  processDelete(del: Pgoutput.MessageDelete) {
    // REPLICA IDENTITY DEFAULT means the `key` must be set.
    // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
    assert(del.relation.replicaIdentity === 'default');
    assert(del.key);
    const rowKey = del.key;
    const table = liteTableName(del.relation);

    const conds = Object.keys(rowKey).map(col => `${ident(col)}=?`);
    this.#db.run(
      `DELETE FROM ${ident(table)} WHERE ${conds.join(' AND ')}`,
      liteValues(rowKey),
    );

    logDeleteOp(this.#db, this.#version, table, rowKey);
  }

  processTruncate(truncate: Pgoutput.MessageTruncate) {
    for (const relation of truncate.relations) {
      const table = liteTableName(relation);
      // Update replica data.
      this.#db.run(`DELETE FROM ${ident(table)}`);

      // Update change log.
      logTruncateOp(this.#db, this.#version, table);
    }
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
