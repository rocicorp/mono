import {
  PG_ADMIN_SHUTDOWN,
  PG_OBJECT_IN_USE,
} from '@drdgvhbh/postgres-error-codes';
import {Lock} from '@rocicorp/lock';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import type {
  MessageMessage,
  MessageRelation,
} from 'pg-logical-replication/dist/output-plugins/pgoutput/pgoutput.types.ts';
import {DatabaseError} from 'pg-protocol';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {assert} from '../../../../../shared/src/asserts.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {
  intersection,
  symmetricDifferences,
} from '../../../../../shared/src/set-utils.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {ShortLivedClient} from '../../../db/short-lived-client.ts';
import type {
  ColumnSpec,
  PublishedTableSpec,
  TableSpec,
} from '../../../db/specs.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {stringify} from '../../../types/bigint-json.ts';
import {oneAfter, type LexiVersion} from '../../../types/lexi-version.ts';
import {
  pgClient,
  registerPostgresTypeParsers,
  type PostgresDB,
} from '../../../types/pg.ts';
import {Subscription} from '../../../types/subscription.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-streamer/change-streamer-service.ts';
import {
  AutoResetSignal,
  type ReplicationConfig,
} from '../../change-streamer/schema/tables.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import type {
  DataChange,
  Identifier,
  MessageDelete,
} from '../protocol/current/data.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
  Data,
} from '../protocol/current/downstream.ts';
import {replicationSlot, type InitialSyncOptions} from './initial-sync.ts';
import {fromLexiVersion, toLexiVersion, type LSN} from './lsn.ts';
import {replicationEventSchema, type DdlUpdateEvent} from './schema/ddl.ts';
import {updateShardSchema} from './schema/init.ts';
import {getPublicationInfo, type PublishedSchema} from './schema/published.ts';
import {
  getInternalShardConfig,
  INTERNAL_PUBLICATION_PREFIX,
  replicaIdentitiesForTablesWithoutPrimaryKeys,
  type InternalShardConfig,
} from './schema/shard.ts';
import {validate} from './schema/validation.ts';
import type {ShardConfig} from './shard-config.ts';
import {initSyncSchema} from './sync-schema.ts';

// BigInt support from LogicalReplicationService.
registerPostgresTypeParsers();

/**
 * Initializes a Postgres change source, including the initial sync of the
 * replica, before streaming changes from the corresponding logical replication
 * stream.
 */
export async function initializePostgresChangeSource(
  lc: LogContext,
  upstreamURI: string,
  shard: ShardConfig,
  replicaDbFile: string,
  syncOptions: InitialSyncOptions,
): Promise<{replicationConfig: ReplicationConfig; changeSource: ChangeSource}> {
  await initSyncSchema(
    lc,
    `replica-${shard.id}`,
    shard,
    replicaDbFile,
    upstreamURI,
    syncOptions,
  );

  const replica = new Database(lc, replicaDbFile);
  const replicationConfig = getSubscriptionState(new StatementRunner(replica));
  replica.close();

  if (shard.publications.length) {
    // Verify that the publications match what has been synced.
    const requested = [...shard.publications].sort();
    const replicated = replicationConfig.publications
      .filter(p => !p.startsWith(INTERNAL_PUBLICATION_PREFIX))
      .sort();
    if (!deepEqual(requested, replicated)) {
      throw new Error(
        `Invalid ShardConfig. Requested publications [${requested}] do not match synced publications: [${replicated}]`,
      );
    }
  }

  // Check that upstream is properly setup, and throw an AutoReset to re-run
  // initial sync if not.
  const db = pgClient(lc, upstreamURI);
  try {
    await checkAndUpdateUpstream(lc, db, shard);
  } finally {
    await db.end();
  }

  const changeSource = new PostgresChangeSource(
    lc,
    upstreamURI,
    shard.id,
    replicationConfig,
  );

  return {replicationConfig, changeSource};
}

async function checkAndUpdateUpstream(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardConfig,
) {
  const slot = replicationSlot(shard.id);
  const result = await db<{restartLSN: LSN | null}[]>`
  SELECT restart_lsn as "restartLSN" FROM pg_replication_slots WHERE slot_name = ${slot}`;
  if (result.length === 0) {
    throw new AutoResetSignal(`replication slot ${slot} is missing`);
  }
  const [{restartLSN}] = result;
  if (restartLSN === null) {
    throw new AutoResetSignal(
      `replication slot ${slot} has been invalidated for exceeding the max_slot_wal_keep_size`,
    );
  }
  // Perform any shard schema updates
  await updateShardSchema(lc, db, {
    id: shard.id,
    publications: shard.publications,
  });
}

const MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE = 5;

/**
 * Postgres implementation of a {@link ChangeSource} backed by a logical
 * replication stream.
 */
class PostgresChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #shardID: string;
  readonly #replicationConfig: ReplicationConfig;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    shardID: string,
    replicationConfig: ReplicationConfig,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#upstreamUri = upstreamUri;
    this.#shardID = shardID;
    this.#replicationConfig = replicationConfig;
  }

  async startStream(clientWatermark: string): Promise<ChangeStream> {
    const db = pgClient(this.#lc, this.#upstreamUri);
    const slot = replicationSlot(this.#shardID);

    try {
      await this.#stopExistingReplicationSlotSubscriber(db, slot);

      const config = await getInternalShardConfig(db, this.#shardID);
      this.#lc.info?.(`starting replication stream @${slot}`);

      // Enabling ssl according to the logic in:
      // https://github.com/brianc/node-postgres/blob/95d7e620ef8b51743b4cbca05dd3c3ce858ecea7/packages/pg-connection-string/index.js#L90
      const url = new URL(this.#upstreamUri);
      let useSSL =
        url.searchParams.get('ssl') !== '0' &&
        url.searchParams.get('sslmode') !== 'disable';
      this.#lc.debug?.(`connecting with ssl=${useSSL} ${url.search}`);

      for (let i = 0; i < MAX_ATTEMPTS_IF_REPLICATION_SLOT_ACTIVE; i++) {
        try {
          // Unlike the postgres.js client, the pg client does not have an option to
          // only use SSL if the server supports it. We achieve it manually by
          // trying SSL first, and then falling back to connecting without SSL.
          return await this.#startStream(slot, clientWatermark, config, useSSL);
        } catch (e) {
          if (e instanceof SSLUnsupportedError) {
            this.#lc.info?.('retrying upstream connection without SSL');
            useSSL = false;
            i--; // don't use up an attempt.
            await this.#stopExistingReplicationSlotSubscriber(db, slot); // Send another SIGTERM to the process
          } else if (
            // error: replication slot "zero_slot_change_source_test_id" is active for PID 268
            e instanceof DatabaseError &&
            e.code === PG_OBJECT_IN_USE
          ) {
            // The freeing up of the replication slot is not transaction;
            // sometimes it takes time for Postgres to consider the slot
            // inactive.
            this.#lc.warn?.(`attempt ${i + 1}: ${String(e)}`, e);
            await sleep(5);
          } else {
            throw e;
          }
        }
      }
      throw new Error('exceeded max attempts to start the Postgres stream');
    } finally {
      await db.end();
    }
  }

  async #startStream(
    slot: string,
    clientWatermark: string,
    shardConfig: InternalShardConfig,
    useSSL: boolean,
  ): Promise<ChangeStream> {
    const changes = Subscription.create<ChangeStreamMessage>({
      cleanup: () => service.stop(),
    });

    // To avoid a race condition when handing off the replication stream
    // between tasks, query the `confirmed_flush_lsn` for the replication
    // slot only after the replication stream starts, as that is when it
    // is guaranteed not to change (i.e. until we ACK a commit).
    const {promise: started, resolve, reject} = resolver();

    const ssl = useSSL ? {rejectUnauthorized: false} : undefined;
    const handleError = (err: Error) => {
      if (
        useSSL &&
        // https://github.com/brianc/node-postgres/blob/8b2768f91d284ff6b97070aaf6602560addac852/packages/pg/lib/connection.js#L74
        err.message === 'The server does not support SSL connections'
      ) {
        reject(new SSLUnsupportedError());
      } else {
        const e = translateError(err);
        reject(e);
        changes.fail(e);
      }
    };

    let acker: Acker | undefined = undefined;
    const changeMaker = new ChangeMaker(
      this.#lc,
      this.#shardID,
      shardConfig,
      this.#upstreamUri,
    );
    const lock = new Lock();
    const service = new LogicalReplicationService(
      {
        connectionString: this.#upstreamUri,
        ssl,
        ['application_name']: `zero-replicator`,
      },
      {acknowledge: {auto: false, timeoutSeconds: 0}},
    )
      .on('start', resolve)
      .on('heartbeat', (lsn, time, respond) => {
        if (respond) {
          // immediately set a timeout that responds with a keepalive if it
          // takes too long for the 'status' message to flow to (and back from)
          // the change-streamer.
          acker?.keepalive();

          // lock to ensure in-order processing
          void lock.withLock(() => {
            changes.push([
              'status',
              {lsn, time},
              {watermark: toLexiVersion(lsn)},
            ]);
          });
        }
      })
      .on('data', (lsn, msg) =>
        // lock to ensure in-order processing
        lock.withLock(async () => {
          for (const change of await changeMaker.makeChanges(lsn, msg)) {
            changes.push(change);
          }
        }),
      )
      .on('error', handleError);

    acker = new Acker(service);

    const clientStart = oneAfter(clientWatermark);
    service
      .subscribe(
        new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: [...this.#replicationConfig.publications],
          messages: true,
        }),
        slot,
        fromLexiVersion(clientStart),
      )
      .then(() => changes.cancel(), handleError);

    await started;

    const {replicaVersion} = this.#replicationConfig;
    this.#lc.info?.(
      `started replication stream@${slot} from ${clientWatermark} (replicaVersion: ${replicaVersion})`,
    );

    return {
      changes,
      acks: {push: status => acker.ack(status[2].watermark)},
    };
  }

  async #stopExistingReplicationSlotSubscriber(
    db: PostgresDB,
    slot: string,
  ): Promise<void> {
    const result = await db<{pid: string | null}[]>`
    SELECT pg_terminate_backend(active_pid), active_pid as pid
      FROM pg_replication_slots WHERE slot_name = ${slot}`;
    if (result.length === 0) {
      // Note: This should not happen as it is checked at initialization time,
      //       but it is technically possible for the replication slot to be
      //       dropped (e.g. manually).
      throw new AbortError(
        `replication slot ${slot} is missing. Delete the replica and resync.`,
      );
    }
    const {pid} = result[0];
    if (pid) {
      this.#lc.info?.(`signaled subscriber ${pid} to shut down`);
    }
  }
}

// Exported for testing.
export class Acker {
  #service: LogicalReplicationService;
  #keepaliveTimer: NodeJS.Timeout | undefined;

  constructor(service: LogicalReplicationService) {
    this.#service = service;
  }

  keepalive() {
    // Sets a timeout to send a standby status update in response to
    // a primary keepalive message.
    //
    // https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-PRIMARY-KEEPALIVE-MESSAGE
    //
    // A primary keepalive message is streamed to the change-streamer as a
    // 'status' message, which in turn responds with an ack. However, in the
    // event that the change-streamer is backed up processing preceding
    // changes, this timeout will fire to send a status update that does not
    // change the confirmed flush position. This timeout must be shorter than
    // the `wal_sender_timeout`, which defaults to 60 seconds.
    //
    // https://www.postgresql.org/docs/current/runtime-config-replication.html#GUC-WAL-SENDER-TIMEOUT
    this.#keepaliveTimer ??= setTimeout(() => this.#sendAck(), 1000);
  }

  ack(watermark: LexiVersion) {
    this.#sendAck(watermark);
  }

  #sendAck(watermark?: LexiVersion) {
    clearTimeout(this.#keepaliveTimer);
    this.#keepaliveTimer = undefined;

    // Note: Sending '0/0' means "keep alive but do not update confirmed_flush_lsn"
    // https://github.com/postgres/postgres/blob/3edc67d337c2e498dad1cd200e460f7c63e512e6/src/backend/replication/walsender.c#L2457
    const lsn = watermark ? fromLexiVersion(watermark) : '0/0';
    void this.#service.acknowledge(lsn);
  }
}

type ReplicationError = {
  lsn: string;
  msg: Pgoutput.Message;
  err: unknown;
  lastLogTime: number;
};

const SET_REPLICA_IDENTITY_DELAY_MS = 500;

class ChangeMaker {
  readonly #lc: LogContext;
  readonly #shardID: string;
  readonly #shardPrefix: string;
  readonly #shardConfig: InternalShardConfig;
  readonly #upstream: ShortLivedClient;

  #replicaIdentityTimer: NodeJS.Timeout | undefined;
  #error: ReplicationError | undefined;

  constructor(
    lc: LogContext,
    shardID: string,
    shardConfig: InternalShardConfig,
    upstreamURI: string,
  ) {
    this.#lc = lc;
    this.#shardID = shardID;
    // Note: This matches the prefix used in pg_logical_emit_message() in pg/schema/ddl.ts.
    this.#shardPrefix = `zero/${shardID}`;
    this.#shardConfig = shardConfig;
    this.#upstream = new ShortLivedClient(
      lc,
      upstreamURI,
      'zero-schema-change-detector',
    );
  }

  async makeChanges(
    lsn: string,
    msg: Pgoutput.Message,
  ): Promise<ChangeStreamMessage[]> {
    if (this.#error) {
      this.#logError(this.#error);
      return [];
    }
    try {
      return await this.#makeChanges(msg);
    } catch (err) {
      this.#error = {lsn, msg, err, lastLogTime: 0};
      this.#logError(this.#error);
      // Rollback the current transaction to avoid dangling transactions in
      // downstream processors (i.e. changeLog, replicator).
      return [
        ['rollback', {tag: 'rollback'}],
        ['control', {tag: 'reset-required'}],
      ];
    }
  }

  #logError(error: ReplicationError) {
    const {lsn, msg, err, lastLogTime} = error;
    const now = Date.now();

    // Output an error to logs as replication messages continue to be dropped,
    // at most once a minute.
    if (now - lastLogTime > 60_000) {
      this.#lc.error?.(
        `Unable to continue replication from LSN ${lsn}: ${String(err)}`,
        // 'content' can be a large byte Buffer. Exclude it from logging output.
        {...msg, content: undefined},
      );
      error.lastLogTime = now;
    }
  }

  // eslint-disable-next-line require-await
  async #makeChanges(msg: Pgoutput.Message): Promise<ChangeStreamData[]> {
    switch (msg.tag) {
      case 'begin':
        return [
          ['begin', msg, {commitWatermark: toLexiVersion(must(msg.commitLsn))}],
        ];

      case 'delete': {
        const key = msg.key ?? msg.old;
        if (!key) {
          throw new Error(
            `Invalid DELETE msg (missing key): ${stringify(msg)}`,
          );
        }
        return [['data', msg.key ? (msg as MessageDelete) : {...msg, key}]];
      }

      case 'insert':
      case 'update':
      case 'truncate':
        return [['data', msg]];

      case 'message':
        if (msg.prefix !== this.#shardPrefix) {
          this.#lc.debug?.('ignoring message for different shard', msg.prefix);
          return [];
        }
        return this.#handleCustomMessage(msg);

      case 'commit':
        return [
          ['commit', msg, {watermark: toLexiVersion(must(msg.commitLsn))}],
        ];

      case 'relation':
        return this.#handleRelation(msg);
      case 'type':
        return []; // Nothing need be done for custom types.
      case 'origin':
        // We do not set the `origin` option in the pgoutput parameters:
        // https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
        throw new Error(`Unexpected ORIGIN message ${stringify(msg)}`);
      default:
        msg satisfies never;
        throw new Error(`Unexpected message type ${stringify(msg)}`);
    }
  }

  #preSchema: PublishedSchema | undefined;

  #handleCustomMessage(msg: MessageMessage) {
    const event = this.#parseReplicationEvent(msg.content);
    // Cancel manual schema adjustment timeouts when an upstream schema change
    // is about to happen, so as to avoid interfering / redundant work.
    clearTimeout(this.#replicaIdentityTimer);

    if (event.type === 'ddlStart') {
      // Store the schema in order to diff it with a potential ddlUpdate.
      this.#preSchema = event.schema;
      return [];
    }
    // ddlUpdate
    const changes = this.#makeSchemaChanges(
      must(this.#preSchema, `ddlUpdate received without a ddlStart`),
      event,
    ).map(change => ['data', change] satisfies Data);

    this.#lc
      .withContext('query', event.context.query)
      .info?.(`${changes.length} schema change(s)`, changes);

    const replicaIdentities = replicaIdentitiesForTablesWithoutPrimaryKeys(
      event.schema,
    );
    if (replicaIdentities) {
      this.#replicaIdentityTimer = setTimeout(async () => {
        try {
          await replicaIdentities.apply(this.#lc, this.#upstream.db);
        } catch (err) {
          this.#lc.warn?.(`error setting replica identities`, err);
        }
      }, SET_REPLICA_IDENTITY_DELAY_MS);
    }

    return changes;
  }

  /**
   *  A note on operation order:
   *
   * Postgres will drop related indexes when columns are dropped,
   * but SQLite will error instead (https://sqlite.org/forum/forumpost/2e62dba69f?t=c&hist).
   * The current workaround is to drop indexes first.
   *
   * Also note that although it should not be possible to both rename and
   * add/drop tables/columns in a single statement, the operations are
   * ordered to handle that possibility, by always dropping old entities,
   * then modifying kept entities, and then adding new entities.
   *
   * Thus, the order of replicating DDL updates is:
   * - drop indexes
   * - drop tables
   * - alter tables
   *   - drop columns
   *   - alter columns
   *   - add columns
   * - create tables
   * - create indexes
   *
   * In the future the replication logic should be improved to handle this
   * behavior in SQLite by dropping dependent indexes manually before dropping
   * columns. This, for example, would be needed to properly support changing
   * the type of a column that's indexed.
   */
  #makeSchemaChanges(
    preSchema: PublishedSchema,
    update: DdlUpdateEvent,
  ): DataChange[] {
    const [prevTbl, prevIdx] = specsByID(preSchema);
    const [nextTbl, nextIdx] = specsByID(update.schema);
    const changes: DataChange[] = [];

    // Validate the new table schemas
    for (const table of nextTbl.values()) {
      validate(this.#lc, this.#shardID, table);
    }

    const [droppedIdx, createdIdx] = symmetricDifferences(prevIdx, nextIdx);
    for (const id of droppedIdx) {
      const {schema, name} = must(prevIdx.get(id));
      changes.push({tag: 'drop-index', id: {schema, name}});
    }

    // DROP
    const [droppedTbl, createdTbl] = symmetricDifferences(prevTbl, nextTbl);
    for (const id of droppedTbl) {
      const {schema, name} = must(prevTbl.get(id));
      changes.push({tag: 'drop-table', id: {schema, name}});
    }
    // ALTER
    const tables = intersection(prevTbl, nextTbl);
    for (const id of tables) {
      changes.push(
        ...this.#getTableChanges(must(prevTbl.get(id)), must(nextTbl.get(id))),
      );
    }
    // CREATE
    for (const id of createdTbl) {
      const spec = must(nextTbl.get(id));
      changes.push({tag: 'create-table', spec});
    }

    // Add indexes last since they may reference tables / columns that need
    // to be created first.
    for (const id of createdIdx) {
      const spec = must(nextIdx.get(id));
      changes.push({tag: 'create-index', spec});
    }
    return changes;
  }

  #getTableChanges(oldTable: TableSpec, newTable: TableSpec): DataChange[] {
    const changes: DataChange[] = [];
    if (
      oldTable.schema !== newTable.schema ||
      oldTable.name !== newTable.name
    ) {
      changes.push({
        tag: 'rename-table',
        old: {schema: oldTable.schema, name: oldTable.name},
        new: {schema: newTable.schema, name: newTable.name},
      });
    }
    const table = {schema: newTable.schema, name: newTable.name};
    const oldColumns = columnsByID(oldTable.columns);
    const newColumns = columnsByID(newTable.columns);

    // DROP
    const [dropped, added] = symmetricDifferences(oldColumns, newColumns);
    for (const id of dropped) {
      const {name: column} = must(oldColumns.get(id));
      changes.push({tag: 'drop-column', table, column});
    }

    // ALTER
    const both = intersection(oldColumns, newColumns);
    for (const id of both) {
      const {name: oldName, ...oldSpec} = must(oldColumns.get(id));
      const {name: newName, ...newSpec} = must(newColumns.get(id));
      // The three things that we care about are:
      // 1. name
      // 2. type
      // 3. not-null
      if (
        oldName !== newName ||
        oldSpec.dataType !== newSpec.dataType ||
        oldSpec.notNull !== newSpec.notNull
      ) {
        changes.push({
          tag: 'update-column',
          table,
          old: {name: oldName, spec: oldSpec},
          new: {name: newName, spec: newSpec},
        });
      }
    }

    // ADD
    for (const id of added) {
      const {name, ...spec} = must(newColumns.get(id));
      changes.push({tag: 'add-column', table, column: {name, spec}});
    }
    return changes;
  }

  #parseReplicationEvent(content: Uint8Array) {
    const str =
      content instanceof Buffer
        ? content.toString('utf-8')
        : new TextDecoder().decode(content);
    const json = JSON.parse(str);
    return v.parse(json, replicationEventSchema, 'passthrough');
  }

  /**
   * If `ddlDetection === true`, relation messages are irrelevant,
   * as schema changes are detected by event triggers that
   * emit custom messages.
   *
   * For degraded-mode replication (`ddlDetection === false`):
   * 1. query the current published schemas on upstream
   * 2. compare that with the InternalShardConfig.initialSchema
   * 3. compare that with the incoming MessageRelation
   * 4. On any discrepancy, throw an UnsupportedSchemaChangeError
   *    to halt replication.
   *
   * Note that schemas queried in step [1] will be *post-transaction*
   * schemas, which are not necessarily suitable for actually processing
   * the statements in the transaction being replicated. In other words,
   * this mechanism cannot be used to reliably *replicate* schema changes.
   * However, they serve the purpose determining if schemas have changed.
   */
  async #handleRelation(rel: MessageRelation): Promise<ChangeStreamData[]> {
    const {publications, ddlDetection, initialSchema} = this.#shardConfig;
    if (ddlDetection) {
      return [];
    }
    assert(initialSchema); // Written in initial-sync
    const currentSchema = await getPublicationInfo(
      this.#upstream.db,
      publications,
    );
    if (schemasDifferent(initialSchema, currentSchema, this.#lc)) {
      throw new UnsupportedSchemaChangeError();
    }
    // Even if the currentSchema is equal to the initialSchema, the
    // MessageRelation itself must be checked to detect transient
    // schema changes within the transaction (e.g. adding and dropping
    // a table, or renaming a column and then renaming it back).
    const orel = initialSchema.tables.find(t => t.oid === rel.relationOid);
    if (!orel) {
      // Can happen if a table is created and then dropped in the same transaction.
      this.#lc.info?.(`relation not in initialSchema: ${stringify(rel)}`);
      throw new UnsupportedSchemaChangeError();
    }
    if (relationDifferent(orel, rel)) {
      this.#lc.info?.(
        `relation has changed within the transaction: ${stringify(orel)}`,
        rel,
      );
      throw new UnsupportedSchemaChangeError();
    }
    return [];
  }
}

export function schemasDifferent(
  a: PublishedSchema,
  b: PublishedSchema,
  lc?: LogContext,
) {
  // Note: ignore indexes since changes need not to halt replication
  return (
    a.tables.length !== b.tables.length ||
    a.tables.some((at, i) => {
      const bt = b.tables[i];
      if (tablesDifferent(at, bt)) {
        lc?.info?.(`table ${stringify(at)} has changed`, bt);
        return true;
      }
      return false;
    })
  );
}

// ColumnSpec comparator
const byColumnPos = (a: [string, ColumnSpec], b: [string, ColumnSpec]) =>
  a[1].pos < b[1].pos ? -1 : a[1].pos > b[1].pos ? 1 : 0;

export function tablesDifferent(a: PublishedTableSpec, b: PublishedTableSpec) {
  if (
    a.oid !== b.oid ||
    a.schema !== b.schema ||
    a.name !== b.name ||
    !deepEqual(a.primaryKey, b.primaryKey)
  ) {
    return true;
  }
  const acols = Object.entries(a.columns).sort(byColumnPos);
  const bcols = Object.entries(b.columns).sort(byColumnPos);
  return (
    acols.length !== bcols.length ||
    acols.some(([aname, acol], i) => {
      const [bname, bcol] = bcols[i];
      return (
        aname !== bname ||
        acol.pos !== bcol.pos ||
        acol.typeOID !== bcol.typeOID ||
        acol.notNull !== bcol.notNull
      );
    })
  );
}

export function relationDifferent(a: PublishedTableSpec, b: MessageRelation) {
  if (
    a.oid !== b.relationOid ||
    a.schema !== b.schema ||
    a.name !== b.name ||
    !deepEqual(a.primaryKey, b.keyColumns)
  ) {
    return true;
  }
  const acols = Object.entries(a.columns).sort(byColumnPos);
  const bcols = b.columns;
  return (
    acols.length !== bcols.length ||
    acols.some(([aname, acol], i) => {
      const bcol = bcols[i];
      return aname !== bcol.name || acol.typeOID !== bcol.typeOid;
    })
  );
}

function translateError(e: unknown): Error {
  if (!(e instanceof Error)) {
    return new Error(String(e));
  }
  if (e instanceof DatabaseError && e.code === PG_ADMIN_SHUTDOWN) {
    return new ShutdownSignal(e);
  }
  return e;
}
const idString = (id: Identifier) => `${id.schema}.${id.name}`;

function specsByID(published: PublishedSchema) {
  return [
    // It would have been nice to use a CustomKeyMap here, but we rely on set-utils
    // operations which use plain Sets.
    new Map(published.tables.map(t => [t.oid, t])),
    new Map(published.indexes.map(i => [idString(i), i])),
  ] as const;
}

function columnsByID(
  columns: Record<string, ColumnSpec>,
): Map<number, ColumnSpec & {name: string}> {
  const colsByID = new Map<number, ColumnSpec & {name: string}>();
  for (const [name, spec] of Object.entries(columns)) {
    // The `pos` field is the `attnum` in `pg_attribute`, which is a stable
    // identifier for the column in this table (i.e. never reused).
    colsByID.set(spec.pos, {...spec, name});
  }
  return colsByID;
}

class SSLUnsupportedError extends Error {}

export class UnsupportedSchemaChangeError extends Error {
  readonly name = 'UnsupportedSchemaChangeError';

  constructor() {
    super(
      'Replication halted. Schema changes cannot be reliably replicated without event trigger support. Resync the replica to recover.',
    );
  }
}

class ShutdownSignal extends AbortError {
  readonly name = 'ShutdownSignal';

  constructor(cause: unknown) {
    super(
      'shutdown signal received (e.g. another zero-cache taking over the replication stream)',
      {
        cause,
      },
    );
  }
}
