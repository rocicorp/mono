import {PG_UNIQUE_VIOLATION} from '@drdgvhbh/postgres-error-codes';
import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {
  ControlFlowError,
  Statement,
  TransactionPool,
} from '../../db/transaction-pool.js';
import {epochMicrosToTimestampTz} from '../../types/big-time.js';
import {stringify} from '../../types/bigint-json.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {registerPostgresTypeParsers} from '../../types/pg.js';
import {rowKeyHash} from '../../types/row-key.js';
import {
  PUB_PREFIX,
  ZERO_VERSION_COLUMN_NAME,
  replicationSlot,
} from './initial-sync.js';
import {CREATE_INVALIDATION_TABLES} from './invalidation.js';
import {PublicationInfo, getPublicationInfo} from './tables/published.js';
import {toLexiVersion} from './types/lsn.js';

/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables} after
 * the logical replication handoff when initial data synchronization has completed.
 */
export const CREATE_REPLICATION_TABLES =
  // The transaction log maps each LSN to transaction information.
  // Note that the lsn may become optional for supporting non-Postgres upstreams.
  `
  CREATE SCHEMA IF NOT EXISTS _zero;
  CREATE TABLE _zero."TxLog" (
    "stateVersion" VARCHAR(38) NOT NULL,
    lsn            PG_LSN      NOT NULL,
    time           TIMESTAMPTZ NOT NULL,
    xid            INTEGER     NOT NULL,
    PRIMARY KEY("stateVersion")
  );
` +
  // The change log contains row changes.
  //
  // * `op`        : 't' for table truncation, 's' for set (insert/update), and 'd' for delete
  // * `rowKeyHash`: Hash of the row key for row identification (see {@link rowKeyHash}). Empty string for truncate op.
  // * `rowKey`    : JSON row key, as `{[$columnName]: $columnValue}`, or NULL for TRUNCATE
  // * `row`       : JSON formatted full row contents, NULL for DELETE / TRUNCATE
  //
  // Note that the `row` data is stored as JSON rather than JSONB to prioritize write
  // throughput, as replication is critical bottleneck in the system. Row values are
  // only needed for catchup, for which JSONB is not particularly advantageous over JSON.
  `
  CREATE TABLE _zero."ChangeLog" (
    "stateVersion" VARCHAR(38)  NOT NULL,
    "tableName"    VARCHAR(128) NOT NULL,
    "op"           CHAR         NOT NULL,
    "rowKeyHash"   VARCHAR(22)  NOT NULL,
    "rowKey"       JSON,
    "row"          JSON,
    CONSTRAINT PK_change_log PRIMARY KEY("stateVersion", "tableName", "rowKeyHash")
  );
`;

/**
 * Migration step that sets up the initialized Sync Replica for incremental replication.
 * This includes:
 *
 * * Setting up the internal _zero tables that track replication state.
 *
 * * Removing the _0_version DEFAULT (used only for initial sync)
 *   and requiring that it be NOT NULL. This is a defensive measure to
 *   enforce that the incremental replication logic always sets the _0_version.
 */
export async function setupReplicationTables(
  lc: LogContext,
  _replicaID: string,
  tx: postgres.TransactionSql,
  upstreamUri: string,
) {
  lc.info?.(`Setting up replication tables for ${upstreamUri}`);

  const replicated = await getPublicationInfo(tx, 'zero_');
  const alterStmts = Object.keys(replicated.tables).map(
    table =>
      `
      ALTER TABLE ${table} 
        ALTER COLUMN ${ZERO_VERSION_COLUMN_NAME} DROP DEFAULT, 
        ALTER COLUMN ${ZERO_VERSION_COLUMN_NAME} SET NOT NULL;
        `,
  );

  await tx.unsafe(
    alterStmts.join('') +
      CREATE_REPLICATION_TABLES +
      CREATE_INVALIDATION_TABLES,
  );
}

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
  readonly #replica: postgres.Sql;

  // This lock ensures that transactions are processed serially, even
  // across re-connects to the upstream db.
  readonly #txSerializer = new Lock();

  #retryDelay = INITIAL_RETRY_DELAY_MS;
  #service: LogicalReplicationService | undefined;
  #started = false;
  #stopped = false;

  constructor(upstreamUri: string, replicaID: string, replica: postgres.Sql) {
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replica = replica;
  }

  async start(lc: LogContext) {
    assert(!this.#started, `IncrementalSyncer has already been started`);
    this.#started = true;

    lc.info?.(`Starting IncrementalSyncer`);
    const replicated = await getPublicationInfo(this.#replica, PUB_PREFIX);
    const publicationNames = replicated.publications.map(p => p.pubname);

    lc.info?.(`Syncing publications ${publicationNames}`);
    while (!this.#stopped) {
      const service = new LogicalReplicationService(
        {connectionString: this.#upstreamUri},
        {acknowledge: {auto: false, timeoutSeconds: 0}},
      );
      this.#service = service;

      const processor = new MessageProcessor(
        this.#replica,
        replicated,
        this.#txSerializer,
        (lsn: string) => service.acknowledge(lsn),
        (lc: LogContext, err: unknown) => this.stop(lc, err),
      );
      this.#service.on(
        'data',
        async (lsn: string, message: Pgoutput.Message) => {
          this.#retryDelay = INITIAL_RETRY_DELAY_MS; // Reset exponential backoff.
          await processor.processMessage(lc, lsn, message);
        },
      );

      try {
        // TODO: Start from the last acknowledged LSN.
        await this.#service.subscribe(
          new PgoutputPlugin({protoVersion: 1, publicationNames}),
          replicationSlot(this.#replicaID),
        );
      } catch (e) {
        if (!this.#stopped) {
          const delay = this.#retryDelay;
          this.#retryDelay = Math.min(this.#retryDelay * 2, MAX_RETRY_DELAY_MS);
          lc.error?.(`Error in Replication Stream. Retrying in ${delay}ms`, e);
          await sleep(delay);
        }
      }
    }
    lc.info?.('IncrementalSyncer stopped');
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

class PrecedingTransactionError extends ControlFlowError {
  constructor(err: unknown) {
    super(err);
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
 *
 * Note that the processing of transactions must be serialized to guarantee that each
 * transaction can see the results of its predecessors. This is done with the
 * singleton `txSerializer` lock created in the IncrementalSyncer service.
 *
 * The logic for handling a transaction happens in two stages.
 *
 * 1. In the **Assembly** stage, logical replication messages from upstream are
 *    gathered by the MessageProcessor and passed to the TransactionProcessor.
 *    At the very first `Begin` message, a downstream Postgres transaction
 *    is enqueued to be started in the `txSerializer` lock.
 *
 * 2. In the **Processing** stage, all preceding transactions have completed,
 *    the downstream Postgres transaction has started, and the Transaction
 *    Processor executes statements on it.
 *
 * Note that the two stages can overlap; for example, a transaction with a
 * large number of messages may still be streaming in when the downstream
 * transaction handle becomes ready. However, it is more common for the
 * transaction to have already been assembled when it comes time for it
 * to be processed, either because it has a small number of messages, or
 * because a preceding transaction is still being processed while the next
 * one is assembled.
 *
 * Here is an example timeline of assembly stages `A*` and
 * their corresponding processing stages `P*`:
 *
 * ```
 *  ----> Upstream Logical Replication Messages ---->
 * ---------------------------     -------------------
 * |      A1       | A2 | A3 |     |   A4   |   A5   |
 * -------------------------------------------------------------------------
 *         |      P1        |   P2   |   P3    |      |   P4   |    P5     |
 *         -------------------------------------      ----------------------
 *                      ----> Downstream Transactions ---->
 * ```
 *
 * This is important to understand in the context of error handling. Although
 * errors are not expected to happen in the steady state, error handling is
 * necessary to avoid corrupting the replica with a state that is
 * inconsistent with a snapshot of upstream.
 *
 * An error may happen in the Assembly stage (e.g. unexpected Message formats,
 * unsupported schema changes), or the Processing stage (e.g. query execution
 * errors, constraint violations, etc.). The desired behavior when encountering
 * an error is to:
 *
 * 1. allow all preceding transactions to successfully finish processing
 *
 * 2. cancel/rollback the erroneous transaction, and disallow all subsequent
 *    transactions from proceeding
 *
 * 3. shut down the service (after which manual intervention is likely needed
 *    to address the unhandled condition).
 *
 * In order to satisfy (1) and (2), error handling is plumbed through the
 * TransactionProcessor object so that it is always surfaced in the Processing
 * stage, even if the error was encountered in the Assembly stage.
 *
 * In the unlikely event that an error is encountered _between_ assembling
 * transactions (e.g. an unexpected Message between the last MessageCommit
 * and the next MessageBegin) and there is no TransactionProcessor being
 * assembled, a callback to fail the service is manually enqueued on the
 * `txSerializer` to allow preceding transactions to complete before shutting
 * down.
 *
 * It follows that, from an implementation perspective, the MessageProcessor's
 * failure handling must always be done from within the `txSerializer` lock.
 */
// Exported for testing.
export class MessageProcessor {
  readonly #replica: postgres.Sql;
  readonly #replicated: PublicationInfo;
  readonly #txSerializer: Lock;
  readonly #acknowledge: (lsn: string) => unknown;
  readonly #failService: (lc: LogContext, err: unknown) => void;

  #failure: Error | undefined;
  #tx: TransactionProcessor | undefined;

  constructor(
    replica: postgres.Sql,
    replicated: PublicationInfo,
    txSerializer: Lock,
    acknowledge: (lsn: string) => unknown,
    failService: (lc: LogContext, err: unknown) => void,
  ) {
    this.#replica = replica;
    this.#replicated = replicated;
    this.#txSerializer = txSerializer;
    this.#acknowledge = acknowledge;
    this.#failService = failService;
  }

  #createAndEnqueueNewTransaction(
    lc: LogContext,
    commitLsn: string,
  ): TransactionProcessor {
    const txProcessor = new TransactionProcessor(lc, commitLsn);
    void this.#txSerializer.withLock(async () => {
      try {
        if (this.#failure) {
          // If a preceding transaction failed, all subsequent transactions must also fail.
          txProcessor.fail(new PrecedingTransactionError(this.#failure));
        }
        await txProcessor.execute(this.#replica);
        this.#acknowledge(commitLsn);
        lc.debug?.(`Committed tx`);
      } catch (e) {
        if (
          // A unique violation on the TxLog means that the transaction has already been
          // processed. This is not a real error, and can happen, for example, if the upstream
          // the connection was lost before the acknowledgment was sent. Recover by resending
          // the acknowledgement, and continue processing the stream.
          e instanceof postgres.PostgresError &&
          e.code === PG_UNIQUE_VIOLATION &&
          e.schema_name === '_zero' &&
          e.table_name === 'TxLog'
        ) {
          this.#acknowledge(commitLsn);
          lc.debug?.(`Skipped repeat tx`);
        } else {
          this.#failInLock(lc, e);
        }
      }
    });
    return txProcessor;
  }

  /** See {@link MessageProcessor} documentation for error handling semantics. */
  #fail(lc: LogContext, err: unknown) {
    if (this.#tx) {
      // If a current transaction is being assembled, fail it so that the `err` is surfaced
      // from within the transaction's processing stage, i.e. from within the `txSerializer`
      // lock via the TransactionProcessor's `setFailed` rejection callback.
      this.#tx.fail(err);
    } else {
      // Otherwise, manually enqueue the failure on the `txSerializer` to allow previous
      // transactions to complete, and prevent subsequent transactions from proceeding.
      void this.#txSerializer.withLock(() => {
        this.#failInLock(lc, err);
      });
    }
  }

  // This must be called from within the txSerializer lock to allow pending
  // (not-failed) transactions to complete.
  #failInLock(lc: LogContext, err: unknown) {
    if (!this.#failure) {
      this.#failure = ensureError(err);
      lc.error?.('Message Processing failed:', this.#failure);
      this.#failService(lc, this.#failure);
    }
  }

  async processMessage(lc: LogContext, lsn: string, message: Pgoutput.Message) {
    lc = lc.withContext('lsn', lsn);
    if (this.#failure) {
      lc.debug?.(`Dropping ${message.tag}`);
      return;
    }
    try {
      await this.#processMessage(lc, lsn, message);
    } catch (e) {
      this.#fail(lc, e);
    }
  }

  #processMessage(lc: LogContext, lsn: string, message: Pgoutput.Message) {
    if (message.tag === 'begin') {
      const {commitLsn} = message;
      assert(commitLsn);

      if (this.#tx) {
        throw new Error(`Already in a transaction ${stringify(message)}`);
      }
      this.#tx = this.#createAndEnqueueNewTransaction(
        lc.withContext('txBegin', lsn).withContext('txCommit', commitLsn),
        commitLsn,
      );
      return this.#tx.processBegin(message);
    }

    // For non-begin messages, there should be a TransactionProcessor set.
    if (!this.#tx) {
      throw new Error(
        `Received message outside of transaction: ${stringify(message)}`,
      );
    }
    switch (message.tag) {
      case 'relation':
        return this.#processRelation(message);
      case 'insert':
        return this.#tx.processInsert(message);
      case 'update':
        return this.#tx.processUpdate(message);
      case 'delete':
        return this.#tx.processDelete(message);
      case 'truncate':
        return this.#tx.processTruncate(message);
      case 'commit': {
        // Undef this.#tx to allow the assembly of the next transaction.
        const tx = this.#tx;
        this.#tx = undefined;
        return tx.processCommit(message);
      }
      case 'origin':
        // We are agnostic as to which node a transaction originated from.
        lc.info?.('Ignoring ORIGIN message in replication stream', message);
        return;
      case 'type':
        throw new Error(
          `Custom types are not supported (received "${message.typeName}")`,
        );
      default:
        // TODO: Determine what the "Message" message is.
        // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#:~:text=Identifies%20the%20message%20as%20a%20logical%20decoding%20message.
        lc.error?.(
          `Received unexpected message of type ${message.tag}`,
          message,
        );
        throw new Error(
          `Don't know how to handle message of type ${message.tag}`,
        );
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
    // For now, just reference the variable to appease the compiler.
    this.#replicated;
  }
}

/**
 * The {@link TransactionProcessor} handles the sequence of messages from
 * upstream, from `BEGIN` to `COMMIT` and executes the corresponding mutations
 * on the {@link postgres.TransactionSql} on the replica.
 */
class TransactionProcessor {
  readonly #version: LexiVersion;
  readonly #pool: TransactionPool;

  constructor(lc: LogContext, lsn: string) {
    this.#version = toLexiVersion(lsn);
    this.#pool = new TransactionPool(lc.withContext('tx', this.#version));
  }

  execute(db: postgres.Sql) {
    return this.#pool.run(db);
  }

  fail(err: unknown) {
    this.#pool.fail(err);
  }

  processBegin(begin: Pgoutput.MessageBegin) {
    const row = {
      stateVersion: this.#version,
      lsn: begin.commitLsn,
      time: epochMicrosToTimestampTz(begin.commitTime.valueOf()),
      xid: begin.xid,
    };

    return this.#pool.process(tx => [tx`INSERT INTO _zero."TxLog" ${tx(row)}`]);
  }

  processInsert(insert: Pgoutput.MessageInsert) {
    const row = {
      ...insert.new,
      [ZERO_VERSION_COLUMN_NAME]: this.#version,
    };
    const key = Object.fromEntries(
      insert.relation.keyColumns.map(col => [col, insert.new[col]]),
    );

    return this.#pool.process(tx => [
      tx`INSERT INTO ${tx(table(insert.relation))} ${tx(row)}`,
      ...this.#upsertChanges(tx, insert.relation, key, row),
    ]);
  }

  processUpdate(update: Pgoutput.MessageUpdate) {
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

    return this.#pool.process(tx => {
      const conds = Object.entries(currKey).map(
        ([col, val]) => tx`${tx(col)} = ${val}`,
      );

      // Note: The flatMap() dance for dynamic filters is a bit obtuse, but it is
      //       what the Postgres.js author recommends until there's a better api for it.
      //       https://github.com/porsager/postgres/issues/807#issuecomment-1949924843
      return [
        tx`
        UPDATE ${tx(table(update.relation))}
          SET ${tx(row)}
          WHERE ${conds.flatMap((k, i) => (i ? [tx` AND `, k] : k))}`,
        ...this.#upsertChanges(tx, update.relation, newKey, row, oldKey),
      ];
    });
  }

  processDelete(del: Pgoutput.MessageDelete) {
    return this.#pool.process(tx => {
      // REPLICA IDENTITY DEFAULT means the `key` must be set.
      // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
      assert(del.relation.replicaIdentity === 'default');
      assert(del.key);
      const conds = Object.entries(del.key).map(
        ([col, val]) => tx`${tx(col)} = ${val}`,
      );

      // Note: The flatMap() dance for dynamic filters is a bit obtuse, but it is
      //       what the Postgres.js author recommends until there's a better api for it.
      //       https://github.com/porsager/postgres/issues/807#issuecomment-1949924843
      return [
        tx`
      DELETE FROM ${tx(table(del.relation))} 
        WHERE ${conds.flatMap((k, i) => (i ? [tx` AND `, k] : k))} `,
        ...this.#upsertChanges(tx, del.relation, del.key),
      ];
    });
  }

  processTruncate(truncate: Pgoutput.MessageTruncate) {
    const tables = truncate.relations.map(r => `${r.schema}.${r.name}`);

    return this.#pool.process(tx => [
      tx`TRUNCATE ${tx(tables)}`,
      ...truncate.relations
        .map(relation => this.#upsertChanges(tx, relation))
        .flat(),
    ]);
  }

  processCommit(_: Pgoutput.MessageCommit) {
    this.#pool.setDone();
  }

  #upsertChanges(
    tx: postgres.TransactionSql,
    relation: Pgoutput.MessageRelation,
    key?: Record<string, postgres.JSONValue>,
    row?: Record<string, postgres.JSONValue>,
    oldKey?: Record<string, postgres.JSONValue> | null,
  ) {
    const change: ChangeLogEntry = {
      stateVersion: this.#version,
      tableName: table(relation),
      op: row ? 's' : key ? 'd' : 't',
      rowKeyHash: key ? rowKeyHash(key) : '', // Empty string for truncate,
      rowKey: key ?? null,
      row: row ?? null,
    };
    const changes: Statement[] = [];
    if (!key) {
      // For truncate, first remove all ChangeLog entries for the table
      // in this transaction.
      changes.push(tx`
      DELETE FROM _zero."ChangeLog"
        WHERE "stateVersion" = ${this.#version} AND
              "tableName" = ${table(relation)};`);
    }
    if (oldKey) {
      // If an update changed the row key, insert a delete for the oldKey.
      const del: ChangeLogEntry = {
        stateVersion: this.#version,
        tableName: table(relation),
        op: 'd',
        rowKeyHash: rowKeyHash(oldKey),
        rowKey: oldKey,
        row: null,
      };
      changes.push(tx`
      INSERT INTO _zero."ChangeLog" ${tx(del)} 
        ON CONFLICT ON CONSTRAINT PK_change_log
        DO UPDATE SET 
          op = EXCLUDED.op,
          row = EXCLUDED.row;`);
    }
    changes.push(tx`
      INSERT INTO _zero."ChangeLog" ${tx(change)} 
        ON CONFLICT ON CONSTRAINT PK_change_log
        DO UPDATE SET 
          op = EXCLUDED.op,
          row = EXCLUDED.row;`);
    return changes;
  }
}

type ChangeLogEntry = {
  stateVersion: string;
  tableName: string;
  op: 't' | 's' | 'd';
  rowKeyHash: string;
  rowKey: postgres.JSONValue | null;
  row: postgres.JSONValue | null;
};

function table(table: Pgoutput.MessageRelation): string {
  return `${table.schema}.${table.name}`;
}
