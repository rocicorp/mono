import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import type postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {epochMicrosToTimestampTz} from '../../types/big-time.js';
import type {LexiVersion} from '../../types/lexi-version.js';
import {
  PUB_PREFIX,
  ZERO_VERSION_COLUMN_NAME,
  replicationSlot,
} from './initial-sync.js';
import {PublicationInfo, getPublicationInfo} from './tables/published.js';
import {toLexiVersion} from './types/lsn.js';

/**
 * Replication metadata, used for invalidation and catchup. These tables
 * are created atomically with the logical replication handoff, after initial
 * data synchronization has completed.
 */
export const CREATE_REPLICATION_TABLES =
  // The transaction log maps each LSN to transaction information.
  // Note that the lsn may become optional for supporting non-Postgres upstreams.
  `
  CREATE SCHEMA IF NOT EXISTS _zero;
  CREATE TABLE _zero."TxLog" (
    "dbVersion" VARCHAR(38) NOT NULL,
    lsn PG_LSN              NOT NULL,
    time TIMESTAMPTZ        NOT NULL,
    xid INTEGER             NOT NULL,
    PRIMARY KEY("dbVersion")
  );
` +
  // The change log contains row changes.
  //
  // * `op`: 'i' for INSERT, 'u' for UPDATE, 'd' for DELETE, 't' for TRUNCATE
  // * `row_key`: Empty string for the TRUNCATE op (because primary keys cannot be NULL).
  // * `row`: JSON formatted full row contents, NULL for DELETE / TRUNCATE
  //
  // Note that the `row` data is stored as JSON rather than JSONB to prioritize write
  // throughput, as replication is critical bottleneck in the system. Row values are
  // only needed for catchup, for which JSONB is not particularly advantageous over JSON.
  `
  CREATE TABLE _zero."ChangeLog" (
    "dbVersion" VARCHAR(38)  NOT NULL,
    "tableName" VARCHAR(128) NOT NULL,
    "rowKey" TEXT            NOT NULL,
    op CHAR(1)               NOT NULL,
    row JSON,
    PRIMARY KEY("dbVersion", "tableName", "rowKey")
  );
` +
  // Invalidation registry.
  //
  // * `spec` defines the invalidation function to run,
  //
  // * `bits` indicates the number of bits used to create the
  //    corresponding tag in the `invalidation_index`. The 'spec' is requested
  //    by View Syncers, while 'bits' is decided by the system.
  //
  //    For example, we may decide to start off with 32-bit hashes and later
  //    determine that it is worth increasing the table size to 40-bit hashes
  //    in order to reduce the number of collisions. During the transition, the
  //    Replicator would compute both sizes until the new size has sufficient
  //    coverage (over old versions).
  //
  // * `fromDBVersion` indicates when the Replicator first started running
  //   the filter. CVRs at or newer than the version are considered covered.
  //
  // * `lastRequested` records (approximately) the last time the spec was
  //   requested. This is not exact. It may only be updated if the difference
  //   exceeds some interval, for example. This is used to clean up specs that
  //   are no longer used.
  `
CREATE TABLE _zero."InvalidationRegistry" (
  spec TEXT                   NOT NULL,
  bits SMALLINT               NOT NULL,
  "fromDBVersion" VARCHAR(38) NOT NULL,
  "lastRequested" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(spec, bits)
);
` +
  // Invalidation index.
  `
CREATE TABLE _zero."InvalidationIndex" (
  hash        BIGINT      NOT NULL,
  "dbVersion" VARCHAR(38) NOT NULL,
  PRIMARY KEY(hash)
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

  await tx.unsafe(alterStmts.join('') + CREATE_REPLICATION_TABLES);
}

const INITIAL_RETRY_DELAY = 100;
const MAX_RETRY_DELAY = 10000;

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

  #retryDelay = INITIAL_RETRY_DELAY;
  #service: LogicalReplicationService | undefined;
  #stopped = false;

  constructor(upstreamUri: string, replicaID: string, replica: postgres.Sql) {
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replica = replica;
  }

  async start(lc: LogContext) {
    assert(
      this.#service === undefined,
      `IncrementalSyncer has already been started`,
    );

    lc.info?.(`Starting IncrementalSyncer`);
    const replicated = await getPublicationInfo(this.#replica, PUB_PREFIX);
    const publicationNames = replicated.publications.map(p => p.pubname);

    // This lock ensures that transactions are processed serially, even
    // across re-connects to the upstream db.
    const txSerializer = new Lock();

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
        txSerializer,
        (lsn: string) => service.acknowledge(lsn),
      );
      this.#service.on(
        'data',
        async (lsn: string, message: Pgoutput.Message) => {
          this.#retryDelay = INITIAL_RETRY_DELAY; // Reset exponential backoff.
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
          this.#retryDelay = Math.min(this.#retryDelay * 2, MAX_RETRY_DELAY);
          lc.error?.(`Error in Replication Stream. Retrying in ${delay}ms`, e);
          await sleep(delay);
        }
      }
    }
    lc.info?.('IncrementalSyncer stopped');
  }

  async stop(lc: LogContext) {
    if (this.#service) {
      lc.info?.(`Stopping IncrementalSyncer`);
      this.#stopped = true;
      await this.#service.stop();
    }
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
 */
class MessageProcessor {
  readonly #replica: postgres.Sql;
  readonly #replicated: PublicationInfo;
  readonly #txSerializer: Lock;
  readonly #acknowledge: (lsn: string) => unknown;

  #tx: TransactionProcessor | undefined;

  constructor(
    replica: postgres.Sql,
    replicated: PublicationInfo,
    txSerializer: Lock,
    acknowledge: (lsn: string) => unknown,
  ) {
    this.#replica = replica;
    this.#replicated = replicated;
    this.#txSerializer = txSerializer;
    this.#acknowledge = acknowledge;
  }

  processMessage(lc: LogContext, lsn: string, message: Pgoutput.Message) {
    if (message.tag === 'begin') {
      if (this.#tx) {
        throw new Error(`Already in a transaction ${safeStringify(message)}`);
      }
      assert(message.commitLsn);

      // The two resolvers are used to coordinate the beginning and ending of the transaction
      // processing within the `txSerializer` lock.
      const {promise: tx, resolve: setTx} = resolver<postgres.TransactionSql>();
      const {promise: processed, resolve: setProcessed} = resolver();

      void this.#txSerializer.withLock(async () => {
        await this.#replica.begin(tx => {
          lc.debug?.('Began tx', safeJSON(message));
          setTx(tx); // Allows TransactionProcessor to start processing its queue of messages.
          return processed; // Signalled by the TransactionProcessor when processing its MessageCommit.
        });
        this.#acknowledge(lsn);
        lc.debug?.(`Committed tx`, safeJSON(message));
      });

      this.#tx = new TransactionProcessor(message.commitLsn, tx, setProcessed);
      return this.#tx.processBegin(message);
    }
    // For non-begin messages, there should be a TransactionProcessor set.
    if (!this.#tx) {
      throw new Error(
        `Received message outside of transaction: ${safeStringify(message)}`,
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
        // Undef this.#tx to allow the queuing of the next transaction.
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
  readonly #tx: Promise<postgres.TransactionSql>;
  readonly #setProcessed: () => void;
  readonly #processLock = new Lock();

  constructor(
    lsn: string,
    tx: Promise<postgres.TransactionSql>,
    setProcessed: () => void,
  ) {
    this.#version = toLexiVersion(lsn);
    this.#tx = tx;
    this.#setProcessed = setProcessed;
  }

  /**
   * Ensures that all messages are processed serially. The callback returns
   * the postgres statement to execute, or `undefined` if no statement is needed.
   */
  #process(
    statement: (
      tx: postgres.TransactionSql,
    ) => postgres.PendingQuery<readonly postgres.MaybeRow[]> | undefined,
  ) {
    return this.#processLock.withLock(async () => {
      // Note: This will block until it is this Transaction's turn to be processed,
      // as coordinated by the `#txSerializer` logic in the {@link MessageProceessor}.
      const tx = await this.#tx;

      // execute() sends the statement to the replica. Note that we do not `await`
      // the result, as the transaction itself automatically guarantees serialization.
      // On the contrary, avoiding the `await` allows the processing of next message
      // to begin while the replica is applying the statement.
      void statement(tx)?.execute();
    });
  }

  processBegin(begin: Pgoutput.MessageBegin) {
    return this.#process(
      tx =>
        // Note: This is how redundant (already seen) transactions are prevented.
        // TODO: Determine how to handle the resulting error.
        tx`INSERT INTO _zero."TxLog" ${tx({
          dbVersion: this.#version,
          lsn: begin.commitLsn,
          time: epochMicrosToTimestampTz(begin.commitTime.valueOf()),
          xid: begin.xid,
        })}`,
    );
  }

  processInsert(insert: Pgoutput.MessageInsert) {
    return this.#process(tx => {
      const row = {
        ...insert.new,
        [ZERO_VERSION_COLUMN_NAME]: this.#version,
      };

      return tx`
      INSERT INTO ${tx(table(insert))} ${tx(row)}`;
    });
  }

  processUpdate(update: Pgoutput.MessageUpdate) {
    return this.#process(tx => {
      const row = {
        ...update.new,
        [ZERO_VERSION_COLUMN_NAME]: this.#version,
      };
      const key =
        // update.key is set with the old values if the key has changed.
        update.key ??
        // Otherwise, the key must be determined from the "new" values.
        Object.fromEntries(
          update.relation.keyColumns.map(col => [col, update.new[col]]),
        );
      const conds = Object.entries(key).map(
        ([col, val]) => tx`${tx(col)} = ${val}`,
      );

      // Note: The flatMap() dance for dynamic filters is a bit obtuse, but it is
      //       what the Postgres.js author recommends until there's a better api for it.
      //       https://github.com/porsager/postgres/issues/807#issuecomment-1949924843
      return tx`
      UPDATE ${tx(table(update))}
        SET ${tx(row)}
        WHERE ${conds.flatMap((k, i) => (i ? [tx` AND `, k] : k))}`;
    });
  }

  processDelete(del: Pgoutput.MessageDelete) {
    return this.#process(tx => {
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
      return tx`
      DELETE FROM ${tx(table(del))} 
        WHERE ${conds.flatMap((k, i) => (i ? [tx` AND `, k] : k))} `;
    });
  }

  processTruncate(truncate: Pgoutput.MessageTruncate) {
    return this.#process(tx => {
      const tables = truncate.relations.map(r => `${r.schema}.${r.name}`);
      return tx`TRUNCATE ${tx(tables)}`;
    });
  }

  processCommit(_: Pgoutput.MessageCommit) {
    return this.#process(() => {
      this.#setProcessed();
      return undefined;
    });
  }
}

function table(msg: {relation: Pgoutput.MessageRelation}): string {
  return `${msg.relation.schema}.${msg.relation.name}`;
}

function safeJSON(m: object) {
  let replaced: Record<string, string> | undefined;
  Object.entries(m).map(([key, value]) => {
    if (typeof value === 'bigint') {
      if (!replaced) {
        replaced = {};
      }
      replaced[key] = value.toString();
    }
  });
  return !replaced ? m : {...m, ...replaced};
}

function safeStringify(m: object) {
  return JSON.stringify(safeJSON(m));
}
