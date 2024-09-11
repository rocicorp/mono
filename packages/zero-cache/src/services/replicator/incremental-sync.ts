import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {assert, unreachable} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {liteValues} from 'zero-cache/src/types/lite.js';
import {Database} from 'zqlite/src/db.js';
import {stringify} from '../../types/bigint-json.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {fromLexiVersion, toLexiVersion} from '../../types/lsn.js';
import {liteTableName} from '../../types/names.js';
import {registerPostgresTypeParsers} from '../../types/pg.js';
import type {Source} from '../../types/streams.js';
import {replicationSlot} from '../change-streamer/pg/initial-sync.js';
import {Notifier} from './notifier.js';
import {ReplicaVersionReady} from './replicator.js';
import {logDeleteOp, logSetOp, logTruncateOp} from './schema/change-log.js';
import {
  ZERO_VERSION_COLUMN_NAME,
  getReplicationVersions,
  getSubscriptionState,
  updateReplicationWatermark,
} from './schema/replication-state.js';

// BigInt support from LogicalReplicationService.
registerPostgresTypeParsers();

const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 10000;

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link MessageProcessor}.
 */
export class IncrementalSyncer {
  readonly #upstreamUri: string;
  readonly #replicaID: string;
  readonly #replica: StatementRunner;
  readonly #notifier: Notifier;

  #retryDelay = INITIAL_RETRY_DELAY_MS;
  #service: LogicalReplicationService | undefined;
  #started = false;
  #stopped = false;

  constructor(upstreamUri: string, replicaID: string, replica: Database) {
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replica = new StatementRunner(replica);
    this.#notifier = new Notifier();
  }

  async run(lc: LogContext) {
    assert(!this.#started, `IncrementalSyncer has already been started`);
    lc.info?.(`Starting IncrementalSyncer`);

    // Notify any waiting subscribers that the replica is ready to be read.
    this.#started = true;
    this.#notifier.notifySubscribers();

    const {publications, watermark} = getSubscriptionState(this.#replica);
    let lastLSN = fromLexiVersion(watermark);

    lc.info?.(`Syncing publications ${publications}`);
    while (!this.#stopped) {
      const service = new LogicalReplicationService(
        {connectionString: this.#upstreamUri},
        {acknowledge: {auto: false, timeoutSeconds: 0}},
      );
      this.#service = service;

      const processor = new MessageProcessor(
        this.#replica,
        (lsn: string) => {
          if (!lastLSN || toLexiVersion(lastLSN) < toLexiVersion(lsn)) {
            lastLSN = lsn;
          }
          void service.acknowledge(lsn);
        },
        () => this.#notifier.notifySubscribers(),
        (lc: LogContext, err: unknown) => this.stop(lc, err),
      );
      this.#service.on('data', (lsn: string, message: Pgoutput.Message) => {
        this.#retryDelay = INITIAL_RETRY_DELAY_MS; // Reset exponential backoff.
        processor.processMessage(lc, lsn, message);
      });
      this.#service.on(
        'heartbeat',
        (lsn: string, time: number, shouldRespond: boolean) => {
          if (shouldRespond) {
            lc.debug?.(`keepalive (lastLSN: ${lastLSN}): ${lsn}, ${time}`);
            void service.acknowledge(lastLSN ?? '0/0');
          }
        },
      );

      try {
        // TODO: Start from the last acknowledged LSN.
        await this.#service.subscribe(
          new PgoutputPlugin({protoVersion: 1, publicationNames: publications}),
          replicationSlot(this.#replicaID),
          lastLSN,
        );
      } catch (e) {
        if (!this.#stopped) {
          await this.#service.stop();
          const delay = this.#retryDelay;
          this.#retryDelay = Math.min(this.#retryDelay * 2, MAX_RETRY_DELAY_MS);
          lc.error?.(`Error in Replication Stream. Retrying in ${delay}ms`, e);
          await sleep(delay);
        }
      }
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  subscribe(): Source<ReplicaVersionReady> {
    return this.#notifier.subscribe();
  }

  async stop(lc: LogContext, err?: unknown) {
    if (this.#service) {
      if (err) {
        lc.error?.('IncrementalSyncer stopped with error', err);
      } else {
        lc.info?.(`Stopping IncrementalSyncer`);
      }
      this.#stopped = true;
      await this.#service.stop();
    }
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
  readonly lsn: string;

  constructor(lsn: string) {
    super(`LSN ${lsn} has already been processed.`);
    this.lsn = lsn;
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
  readonly #acknowledge: (lsn: string) => unknown;
  readonly #notifyVersionChange: () => void;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  #currentTx: TransactionProcessor | null = null;

  #failure: Error | undefined;

  constructor(
    db: StatementRunner,
    acknowledge: (lsn: string) => unknown,
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
      this.#failure = ensureError(err);
      lc.error?.('Message Processing failed:', this.#failure);
      this.#failService(lc, this.#failure);
    }
  }

  processMessage(lc: LogContext, lsn: string, message: Pgoutput.Message) {
    lc = lc.withContext('lsn', lsn);
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return;
    }
    try {
      this.#processMessage(lc, message);
    } catch (e) {
      if (e instanceof ReplayedTransactionError) {
        lc.info?.(e);
        this.#acknowledge(e.lsn);
      } else {
        this.#fail(lc, e);
      }
    }
  }

  #processMessage(lc: LogContext, msg: Pgoutput.Message) {
    if (msg.tag === 'begin') {
      if (this.#currentTx) {
        throw new Error(`Already in a transaction ${stringify(msg)}`);
      }
      this.#currentTx = new TransactionProcessor(this.#db, msg);
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
      case 'relation':
        this.#processRelation(msg);
        break;
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

        const elapsedMs = tx.processCommit(msg);
        lc.debug?.(`Committed tx (${elapsedMs} ms)`);

        const lsn = msg.commitEndLsn;
        assert(lsn);
        this.#acknowledge(lsn);
        this.#notifyVersionChange();
        break;
      }

      // Unexpected message types
      case 'type':
        throw new Error(
          `Custom types are not supported (received "${msg.typeName}")`,
        );
      case 'origin':
        // We do not set the `origin` option in the pgoutput parameters:
        // https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        throw new Error(`Unexpected ORIGIN message ${stringify(msg)}`);
      case 'message':
        // We do not set the `messages` option in the pgoutput parameters:
        // https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        throw new Error(`Unexpected MESSAGE message ${stringify(msg)}`);

      default:
        unreachable(msg);
    }
  }

  #processRelation(rel: Pgoutput.MessageRelation) {
    if (rel.replicaIdentity !== 'default') {
      throw new Error(
        // REPLICA IDENTITY DEFAULT is the default setting for all tables.
        // We require this so that the replication stream sends the PRIMARY KEY
        // columns in the MessageRelation message.
        //
        // REPLICA IDENTITY FULL, on the other hand, handles all columns as "key"
        // columns and hinders our ability to detect when the actual key columns change.
        // It is not expected that anyone is changing the default; this check is here
        // for defensive completeness.
        `REPLICA IDENTITY for ${rel.schema}.${rel.name} must be DEFAULT, found ${rel.replicaIdentity}`,
      );
    }
    // TODO: Check columns, keys, etc. for schema syncing.
  }
}

/**
 * The {@link TransactionProcessor} handles the sequence of messages from
 * upstream, from `BEGIN` to `COMMIT` and executes the corresponding mutations
 * on the {@link postgres.TransactionSql} on the replica.
 *
 * When applying row contents to the replica, the `_0_version` column is added / updated,
 * and a corresponding entry in the `ChangeLog` is added. The version value is derived
 * from the LSN of the preceding transaction (stored as the `nextStateVersion` in the
 * `ReplicationState` table).
 *
 *   Side note: The previous implementation used the LSN of the new transaction as the
 *   `stateVersion` of its constituent rows, but this is not compatible with the
 *   streaming (in-progress) transaction protocol, for which the LSN of the
 *   transaction is not known until the commit. To prepare for supporting streaming
 *   transactions, the LSN of the _previous_ commit is used instead, which provides
 *   an equally suitable deterministic function for row versions.
 *
 * Also of interest is the fact that all INSERT Messages are logically applied as
 * UPSERTs. See {@link processInsert} for the underlying motivation.
 */
class TransactionProcessor {
  readonly #startMs: number;
  readonly #db: StatementRunner;
  readonly #version: LexiVersion;

  constructor(db: StatementRunner, _: Pgoutput.MessageBegin) {
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
   * replayed transactions (e.g. if an LSN acknowledgement was lost). In the case
   * of a replayed transaction, the final commit results in an rollback if the
   * lsn is earlier than what has already been processed. See {@link processCommit}.
   *
   * Note that a transaction replay can be detected at the BEGIN message since it
   * contains the commitEndLsn, but that would not generalize to streaming transactions
   * for which the commitEndLsn is not known until STREAM COMMIT.
   *
   * This UPSERT strategy instead handles both protocols by accepting all messages and
   * making the COMMIT/ROLLBACK decision when the commitEndLsn is guaranteed to be
   * available.
   *
   * Note that transaction replays should theoretically never happen because the
   * replication stream is started with the replica's current `watermark` (which would
   * be ahead of the upstream's `confirmed_flush_lsn` if an acknowledgement were lost).
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

  processCommit(commit: Pgoutput.MessageCommit) {
    const lsn = commit.commitEndLsn;
    // The field is technically nullable because readLsn() returns null for "0/0",
    // but in practice that can never happen for a `commitEndLsn`.
    assert(lsn);

    const nextVersion = toLexiVersion(lsn);
    if (nextVersion <= this.#version) {
      this.#db.rollback();
      throw new ReplayedTransactionError(lsn);
    }
    updateReplicationWatermark(this.#db, toLexiVersion(lsn));
    this.#db.commit();

    const elapsedMs = Date.now() - this.#startMs;
    return elapsedMs;
  }
}
