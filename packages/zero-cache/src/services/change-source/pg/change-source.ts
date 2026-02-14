import {
  PG_ADMIN_SHUTDOWN,
  PG_OBJECT_IN_USE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {stringify} from '../../../../../shared/src/bigint-json.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {
  equals,
  intersection,
  symmetricDifferences,
} from '../../../../../shared/src/set-utils.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {mapPostgresToLiteColumn} from '../../../db/pg-to-lite.ts';
import type {ColumnSpec, PublishedTableSpec} from '../../../db/specs.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {type LexiVersion} from '../../../types/lexi-version.ts';
import {pgClient, type PostgresDB} from '../../../types/pg.ts';
import {
  upstreamSchema,
  type ShardConfig,
  type ShardID,
} from '../../../types/shards.ts';
import {
  majorVersionFromString,
  majorVersionToString,
} from '../../../types/state-version.ts';
import type {Sink} from '../../../types/streams.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {
  getSubscriptionState,
  type SubscriptionState,
} from '../../replicator/schema/replication-state.ts';
import type {ChangeSource, ChangeStream} from '../change-source.ts';
import {BackfillManager} from '../common/backfill-manager.ts';
import {ChangeStreamMultiplexer} from '../common/change-stream-multiplexer.ts';
import type {BackfillRequest, JSONObject} from '../protocol/current.ts';
import type {
  Identifier,
  MessageRelation,
  SchemaChange,
} from '../protocol/current/data.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
  Data,
} from '../protocol/current/downstream.ts';
import type {TableMetadata} from './backfill-metadata.ts';
import {streamBackfill} from './backfill-stream.ts';
import {type InitialSyncOptions} from './initial-sync.ts';
import type {
  Message,
  MessageMessage,
  MessageRelation as PostgresRelation,
} from './logical-replication/pgoutput.types.ts';
import {subscribe} from './logical-replication/stream.ts';
import {fromBigInt, toStateVersionString, type LSN} from './lsn.ts';
import {replicationEventSchema, type DdlUpdateEvent} from './schema/ddl.ts';
import {updateShardSchema} from './schema/init.ts';
import {
  getPublicationInfo,
  type PublishedSchema,
  type PublishedTableWithReplicaIdentity,
} from './schema/published.ts';
import {
  dropShard,
  getInternalShardConfig,
  getReplicaAtVersion,
  internalPublicationPrefix,
  legacyReplicationSlot,
  replicaIdentitiesForTablesWithoutPrimaryKeys,
  replicationSlotExpression,
  type InternalShardConfig,
  type Replica,
} from './schema/shard.ts';
import {validate} from './schema/validation.ts';
import {initSyncSchema} from './sync-schema.ts';

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
): Promise<{subscriptionState: SubscriptionState; changeSource: ChangeSource}> {
  await initSyncSchema(
    lc,
    `replica-${shard.appID}-${shard.shardNum}`,
    shard,
    replicaDbFile,
    upstreamURI,
    syncOptions,
  );

  const replica = new Database(lc, replicaDbFile);
  const subscriptionState = getSubscriptionState(new StatementRunner(replica));
  replica.close();

  // Check that upstream is properly setup, and throw an AutoReset to re-run
  // initial sync if not.
  const db = pgClient(lc, upstreamURI);
  try {
    const upstreamReplica = await checkAndUpdateUpstream(
      lc,
      db,
      shard,
      subscriptionState,
    );

    const changeSource = new PostgresChangeSource(
      lc,
      upstreamURI,
      shard,
      upstreamReplica,
    );

    return {subscriptionState, changeSource};
  } finally {
    await db.end();
  }
}

async function checkAndUpdateUpstream(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardConfig,
  {replicaVersion, publications: subscribed}: SubscriptionState,
) {
  // Perform any shard schema updates
  await updateShardSchema(lc, sql, shard, replicaVersion);

  const upstreamReplica = await getReplicaAtVersion(
    lc,
    sql,
    shard,
    replicaVersion,
  );
  if (!upstreamReplica) {
    throw new AutoResetSignal(
      `No replication slot for replica at version ${replicaVersion}`,
    );
  }

  // Verify that the publications match what is being replicated.
  const requested = [...shard.publications].sort();
  const replicated = upstreamReplica.publications
    .filter(p => !p.startsWith(internalPublicationPrefix(shard)))
    .sort();
  if (!deepEqual(requested, replicated)) {
    lc.warn?.(`Dropping shard to change publications to: [${requested}]`);
    await sql.unsafe(dropShard(shard.appID, shard.shardNum));
    throw new AutoResetSignal(
      `Requested publications [${requested}] do not match configured ` +
        `publications: [${replicated}]`,
    );
  }

  // Sanity check: The subscription state on the replica should have the
  // same publications. This should be guaranteed by the equivalence of the
  // replicaVersion, but it doesn't hurt to verify.
  if (!deepEqual(upstreamReplica.publications, subscribed)) {
    throw new AutoResetSignal(
      `Upstream publications [${upstreamReplica.publications}] do not ` +
        `match subscribed publications [${subscribed}]`,
    );
  }

  // Verify that the publications exist.
  const exists = await sql`
    SELECT pubname FROM pg_publication WHERE pubname IN ${sql(subscribed)};
  `.values();
  if (exists.length !== subscribed.length) {
    throw new AutoResetSignal(
      `Upstream publications [${exists.flat()}] do not contain ` +
        `all subscribed publications [${subscribed}]`,
    );
  }

  const {slot} = upstreamReplica;
  const result = await sql<
    {restartLSN: LSN | null; walStatus: string | null}[]
  > /*sql*/ `
    SELECT restart_lsn as "restartLSN", wal_status as "walStatus" FROM pg_replication_slots
      WHERE slot_name = ${slot}`;
  if (result.length === 0) {
    throw new AutoResetSignal(`replication slot ${slot} is missing`);
  }
  const [{restartLSN, walStatus}] = result;
  if (restartLSN === null || walStatus === 'lost') {
    throw new AutoResetSignal(
      `replication slot ${slot} has been invalidated for exceeding the max_slot_wal_keep_size`,
    );
  }
  return upstreamReplica;
}

// Parameterize this if necessary. In practice starvation may never happen.
const MAX_LOW_PRIORITY_DELAY_MS = 1000;

type ReservationState = {
  lastWatermark?: string;
};

/**
 * Postgres implementation of a {@link ChangeSource} backed by a logical
 * replication stream.
 */
class PostgresChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #shard: ShardID;
  readonly #replica: Replica;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    shard: ShardID,
    replica: Replica,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#upstreamUri = upstreamUri;
    this.#shard = shard;
    this.#replica = replica;
  }

  async startStream(
    clientWatermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<ChangeStream> {
    const db = pgClient(this.#lc, this.#upstreamUri);
    const {slot} = this.#replica;

    let cleanup = promiseVoid;
    try {
      ({cleanup} = await this.#stopExistingReplicationSlotSubscribers(
        db,
        slot,
      ));
      const config = await getInternalShardConfig(db, this.#shard);
      this.#lc.info?.(`starting replication stream@${slot}`);
      return await this.#startStream(
        db,
        slot,
        clientWatermark,
        config,
        backfillRequests,
      );
    } finally {
      void cleanup.then(() => db.end());
    }
  }

  async #startStream(
    db: PostgresDB,
    slot: string,
    clientWatermark: string,
    shardConfig: InternalShardConfig,
    backfillRequests: BackfillRequest[],
  ): Promise<ChangeStream> {
    const clientStart = majorVersionFromString(clientWatermark) + 1n;
    const {messages, acks} = await subscribe(
      this.#lc,
      db,
      slot,
      [...shardConfig.publications],
      clientStart,
    );

    // The ChangeStreamMultiplexer facilitates cooperative streaming from
    // the main replication stream and backfill streams initiated by the
    // BackfillManager.
    const changes = new ChangeStreamMultiplexer(this.#lc, clientWatermark);
    const backfillManager = new BackfillManager(this.#lc, changes, req =>
      streamBackfill(this.#lc, this.#upstreamUri, this.#replica, req),
    );
    changes
      .addProducers(messages, backfillManager)
      .addListeners(backfillManager);
    backfillManager.run(clientWatermark, backfillRequests);

    const acker = new Acker(acks);

    const changeMaker = new ChangeMaker(
      this.#lc,
      this.#shard,
      shardConfig,
      this.#replica.initialSchema,
      this.#upstreamUri,
    );

    void (async function () {
      try {
        let reservation: ReservationState | null = null;
        let inTransaction = false;

        for await (const [lsn, msg] of messages) {
          // Note: no reservation is needed for pushStatus().
          if (msg.tag === 'keepalive') {
            changes.pushStatus([
              'status',
              {ack: msg.shouldRespond},
              {watermark: majorVersionToString(lsn)},
            ]);

            // If we're not in a transaction but the last reservation was kept
            // because of pending keepalives in the queue, release the
            // reservation.
            if (!inTransaction && reservation?.lastWatermark) {
              changes.release(reservation.lastWatermark);
              reservation = null;
            }
            continue;
          }

          if (!reservation) {
            const res = changes.reserve('replication');
            typeof res === 'string' || (await res); // awaits should be uncommon
            reservation = {};
          }

          let lastChange: ChangeStreamMessage | undefined;
          for (const change of await changeMaker.makeChanges(lsn, msg)) {
            await changes.push(change); // Allow the change-streamer to push back.
            lastChange = change;
          }

          switch (lastChange?.[0]) {
            case 'begin':
              inTransaction = true;
              break;
            case 'commit':
              inTransaction = false;
              reservation.lastWatermark = lastChange[2].watermark;
              if (
                messages.queued === 0 ||
                changes.waiterDelay() > MAX_LOW_PRIORITY_DELAY_MS
              ) {
                // After each transaction, release the reservation:
                // - if there are no pending upstream messages
                // - or if a low priority request has been waiting for longer
                //   than MAX_LOW_PRIORITY_DELAY_MS. This is to prevent
                //   (backfill) starvation on very active upstreams.
                changes.release(reservation.lastWatermark);
                reservation = null;
              }
              break;
          }
        }
      } catch (e) {
        // Note: no need to worry about reservations here since downstream
        //       is being completely canceled.
        changes.fail(translateError(e));
      }
    })();

    this.#lc.info?.(
      `started replication stream@${slot} from ${clientWatermark} (replicaVersion: ${
        this.#replica.version
      })`,
    );

    return {
      changes: changes.asSource(),
      acks: {push: status => acker.ack(status[2].watermark)},
    };
  }

  /**
   * Stops replication slots associated with this shard, and returns
   * a `cleanup` task that drops any slot other than the specified
   * `slotToKeep`.
   *
   * Note that replication slots created after `slotToKeep` (as indicated by
   * the timestamp suffix) are preserved, as those are newly syncing replicas
   * that will soon take over the slot.
   */
  async #stopExistingReplicationSlotSubscribers(
    sql: PostgresDB,
    slotToKeep: string,
  ): Promise<{cleanup: Promise<void>}> {
    const slotExpression = replicationSlotExpression(this.#shard);
    const legacySlotName = legacyReplicationSlot(this.#shard);

    // Note: `slot_name <= slotToKeep` uses a string compare of the millisecond
    // timestamp, which works until it exceeds 13 digits (sometime in 2286).
    const result = await sql<{slot: string; pid: string | null}[]>`
    SELECT slot_name as slot, pg_terminate_backend(active_pid), active_pid as pid
      FROM pg_replication_slots 
      WHERE (slot_name LIKE ${slotExpression} OR slot_name = ${legacySlotName})
            AND slot_name <= ${slotToKeep}`;
    if (result.length === 0) {
      throw new AbortError(
        `replication slot ${slotToKeep} is missing. A different ` +
          `replication-manager should now be running on a new ` +
          `replication slot.`,
      );
    }
    // Clean up the replicas table.
    const replicasTable = `${upstreamSchema(this.#shard)}.replicas`;
    await sql`DELETE FROM ${sql(replicasTable)} WHERE slot != ${slotToKeep}`;

    const pids = result.filter(({pid}) => pid !== null).map(({pid}) => pid);
    if (pids.length) {
      this.#lc.info?.(`signaled subscriber ${pids} to shut down`);
    }
    const otherSlots = result
      .filter(({slot}) => slot !== slotToKeep)
      .map(({slot}) => slot);
    return {
      cleanup: otherSlots.length
        ? this.#dropReplicationSlots(sql, otherSlots)
        : promiseVoid,
    };
  }

  async #dropReplicationSlots(sql: PostgresDB, slots: string[]) {
    this.#lc.info?.(`dropping other replication slot(s) ${slots}`);
    for (let i = 0; i < 5; i++) {
      try {
        await sql`
          SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
            WHERE slot_name IN ${sql(slots)}
        `;
        this.#lc.info?.(`successfully dropped ${slots}`);
        return;
      } catch (e) {
        // error: replication slot "zero_slot_change_source_test_id" is active for PID 268
        if (
          e instanceof postgres.PostgresError &&
          e.code === PG_OBJECT_IN_USE
        ) {
          // The freeing up of the replication slot is not transactional;
          // sometimes it takes time for Postgres to consider the slot
          // inactive.
          this.#lc.debug?.(`attempt ${i + 1}: ${String(e)}`, e);
        } else {
          this.#lc.warn?.(`error dropping ${slots}`, e);
        }
        await sleep(1000);
      }
    }
    this.#lc.warn?.(`maximum attempts exceeded dropping ${slots}`);
  }
}

// Exported for testing.
export class Acker {
  #acks: Sink<bigint>;
  #keepaliveTimer: NodeJS.Timeout | undefined;

  constructor(acks: Sink<bigint>) {
    this.#acks = acks;
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
    const lsn = watermark ? majorVersionFromString(watermark) : 0n;
    this.#acks.push(lsn);
  }
}

type ReplicationError = {
  lsn: bigint;
  msg: Message;
  err: unknown;
  lastLogTime: number;
};

const SET_REPLICA_IDENTITY_DELAY_MS = 500;

class ChangeMaker {
  readonly #lc: LogContext;
  readonly #shardPrefix: string;
  readonly #shardConfig: InternalShardConfig;
  readonly #initialSchema: PublishedSchema;
  readonly #upstreamDB: PostgresDB;

  #replicaIdentityTimer: NodeJS.Timeout | undefined;
  #error: ReplicationError | undefined;

  constructor(
    lc: LogContext,
    {appID, shardNum}: ShardID,
    shardConfig: InternalShardConfig,
    initialSchema: PublishedSchema,
    upstreamURI: string,
  ) {
    this.#lc = lc;
    // Note: This matches the prefix used in pg_logical_emit_message() in pg/schema/ddl.ts.
    this.#shardPrefix = `${appID}/${shardNum}`;
    this.#shardConfig = shardConfig;
    this.#initialSchema = initialSchema;
    this.#upstreamDB = pgClient(lc, upstreamURI, {
      ['idle_timeout']: 10, // only used occasionally
      connection: {['application_name']: 'zero-schema-change-detector'},
    });
  }

  async makeChanges(lsn: bigint, msg: Message): Promise<ChangeStreamMessage[]> {
    if (this.#error) {
      this.#logError(this.#error);
      return [];
    }
    try {
      return await this.#makeChanges(msg);
    } catch (err) {
      this.#error = {lsn, msg, err, lastLogTime: 0};
      this.#logError(this.#error);

      const message = `Unable to continue replication from LSN ${fromBigInt(lsn)}`;
      const errorDetails: JSONObject = {error: message};
      if (err instanceof UnsupportedSchemaChangeError) {
        errorDetails.reason = err.description;
        errorDetails.context = err.ddlUpdate.context;
      } else {
        errorDetails.reason = String(err);
      }

      // Rollback the current transaction to avoid dangling transactions in
      // downstream processors (i.e. changeLog, replicator).
      return [
        ['rollback', {tag: 'rollback'}],
        ['control', {tag: 'reset-required', message, errorDetails}],
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
        `Unable to continue replication from LSN ${fromBigInt(lsn)}: ${String(
          err,
        )}`,
        err instanceof UnsupportedSchemaChangeError
          ? err.ddlUpdate.context
          : // 'content' can be a large byte Buffer. Exclude it from logging output.
            {...msg, content: undefined},
      );
      error.lastLogTime = now;
    }
  }

  // oxlint-disable-next-line require-await
  async #makeChanges(msg: Message): Promise<ChangeStreamData[]> {
    switch (msg.tag) {
      case 'begin':
        return [
          [
            'begin',
            {...msg, json: 's'},
            {commitWatermark: toStateVersionString(must(msg.commitLsn))},
          ],
        ];

      case 'delete': {
        if (!(msg.key ?? msg.old)) {
          throw new Error(
            `Invalid DELETE msg (missing key): ${stringify(msg)}`,
          );
        }
        return [
          [
            'data',
            {
              ...msg,
              relation: makeRelation(msg.relation),
              // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-DELETE
              key: must(msg.old ?? msg.key),
            },
          ],
        ];
      }

      case 'update': {
        return [
          [
            'data',
            {
              ...msg,
              relation: makeRelation(msg.relation),
              // https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE
              key: msg.old ?? msg.key,
            },
          ],
        ];
      }

      case 'insert':
        return [['data', {...msg, relation: makeRelation(msg.relation)}]];
      case 'truncate':
        return [['data', {...msg, relations: msg.relations.map(makeRelation)}]];

      case 'message':
        if (msg.prefix !== this.#shardPrefix) {
          this.#lc.debug?.('ignoring message for different shard', msg.prefix);
          return [];
        }
        return this.#handleCustomMessage(msg);

      case 'commit':
        return [
          [
            'commit',
            msg,
            {watermark: toStateVersionString(must(msg.commitLsn))},
          ],
        ];

      case 'relation':
        return this.#handleRelation(msg);
      case 'type':
        return []; // Nothing need be done for custom types.
      case 'origin':
        // No need to detect replication loops since we are not a
        // PG replication source.
        return [];
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
      .withContext('tag', event.event.tag)
      .withContext('query', event.context.query)
      .info?.(`${changes.length} schema change(s)`, {changes});

    const replicaIdentities = replicaIdentitiesForTablesWithoutPrimaryKeys(
      event.schema,
    );
    if (replicaIdentities) {
      this.#replicaIdentityTimer = setTimeout(async () => {
        try {
          await replicaIdentities.apply(this.#lc, this.#upstreamDB);
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
  ): SchemaChange[] {
    try {
      const [prevTbl, prevIdx] = specsByID(preSchema);
      const [nextTbl, nextIdx] = specsByID(update.schema);
      const changes: SchemaChange[] = [];

      // Validate the new table schemas
      for (const table of nextTbl.values()) {
        validate(this.#lc, table);
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
          ...this.#getTableChanges(
            must(prevTbl.get(id)),
            must(nextTbl.get(id)),
          ),
        );
      }
      // CREATE
      for (const id of createdTbl) {
        const spec = must(nextTbl.get(id));
        changes.push({
          tag: 'create-table',
          spec,
          metadata: getMetadata(spec),
        });
      }

      // Add indexes last since they may reference tables / columns that need
      // to be created first.
      for (const id of createdIdx) {
        const spec = must(nextIdx.get(id));
        changes.push({tag: 'create-index', spec});
      }
      return changes;
    } catch (e) {
      throw new UnsupportedSchemaChangeError(String(e), update, {cause: e});
    }
  }

  #getTableChanges(
    oldTable: PublishedTableWithReplicaIdentity,
    newTable: PublishedTableWithReplicaIdentity,
  ): SchemaChange[] {
    const changes: SchemaChange[] = [];
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
    const oldMetadata = getMetadata(oldTable);
    const newMetadata = getMetadata(newTable);
    if (!deepEqual(oldMetadata, newMetadata)) {
      changes.push({
        tag: 'update-table-metadata',
        table: {schema: newTable.schema, name: newTable.name},
        old: oldMetadata,
        new: newMetadata,
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
      const column = {name, spec};
      // Validate that the ChangeProcessor will accept the column change.
      mapPostgresToLiteColumn(table.name, column);
      changes.push({
        tag: 'add-column',
        table,
        column,
        tableMetadata: getMetadata(newTable),
      });
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
  async #handleRelation(rel: PostgresRelation): Promise<ChangeStreamData[]> {
    const {publications, ddlDetection} = this.#shardConfig;
    if (ddlDetection) {
      return [];
    }
    const currentSchema = await getPublicationInfo(
      this.#upstreamDB,
      publications,
    );
    const difference = getSchemaDifference(this.#initialSchema, currentSchema);
    if (difference !== null) {
      throw new MissingEventTriggerSupport(difference);
    }
    // Even if the currentSchema is equal to the initialSchema, the
    // MessageRelation itself must be checked to detect transient
    // schema changes within the transaction (e.g. adding and dropping
    // a table, or renaming a column and then renaming it back).
    const orel = this.#initialSchema.tables.find(
      t => t.oid === rel.relationOid,
    );
    if (!orel) {
      // Can happen if a table is created and then dropped in the same transaction.
      throw new MissingEventTriggerSupport(
        `relation not in initialSchema: ${stringify(rel)}`,
      );
    }
    if (relationDifferent(orel, rel)) {
      throw new MissingEventTriggerSupport(
        `relation has changed within the transaction: ${stringify(orel)} vs ${stringify(rel)}`,
      );
    }
    return [];
  }
}

function getSchemaDifference(
  a: PublishedSchema,
  b: PublishedSchema,
): string | null {
  // Note: ignore indexes since changes need not to halt replication
  if (a.tables.length !== b.tables.length) {
    return `tables created or dropped`;
  }
  for (let i = 0; i < a.tables.length; i++) {
    const at = a.tables[i];
    const bt = b.tables[i];
    const difference = getTableDifference(at, bt);
    if (difference) {
      return difference;
    }
  }
  return null;
}

// ColumnSpec comparator
const byColumnPos = (a: [string, ColumnSpec], b: [string, ColumnSpec]) =>
  a[1].pos < b[1].pos ? -1 : a[1].pos > b[1].pos ? 1 : 0;

function getTableDifference(
  a: PublishedTableSpec,
  b: PublishedTableSpec,
): string | null {
  if (a.oid !== b.oid || a.schema !== b.schema || a.name !== b.name) {
    return `Table "${a.name}" differs from table "${b.name}"`;
  }
  if (!deepEqual(a.primaryKey, b.primaryKey)) {
    return `Primary key of table "${a.name}" has changed`;
  }
  const acols = Object.entries(a.columns).sort(byColumnPos);
  const bcols = Object.entries(b.columns).sort(byColumnPos);
  if (
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
  ) {
    return `Columns of table "${a.name}" have changed`;
  }
  return null;
}

export function relationDifferent(a: PublishedTableSpec, b: PostgresRelation) {
  if (a.oid !== b.relationOid || a.schema !== b.schema || a.name !== b.name) {
    return true;
  }
  if (
    // The MessageRelation's `keyColumns` field contains the columns in column
    // declaration order, whereas the PublishedTableSpec's `primaryKey`
    // contains the columns in primary key (i.e. index) order. Do an
    // order-agnostic compare here since it is not possible to detect
    // key-order changes from the MessageRelation message alone.
    b.replicaIdentity === 'default' &&
    !equals(new Set(a.primaryKey), new Set(b.keyColumns))
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
  if (e instanceof postgres.PostgresError && e.code === PG_ADMIN_SHUTDOWN) {
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

function getMetadata(table: PublishedTableWithReplicaIdentity): TableMetadata {
  return {
    schemaOID: must(table.schemaOID),
    relationOID: table.oid,
    rowKey: Object.fromEntries(
      table.replicaIdentityColumns.map(k => [
        k,
        {attNum: table.columns[k].pos},
      ]),
    ),
  };
}

// Avoid sending the `columns` from the Postgres MessageRelation message.
// They are not used downstream and the message can be large.
function makeRelation(relation: PostgresRelation): MessageRelation {
  // Avoid sending the `columns` from the Postgres MessageRelation message.
  // They are not used downstream and the message can be large.
  const {columns: _, keyColumns, replicaIdentity, ...rest} = relation;
  return {
    ...rest,
    rowKey: {
      columns: keyColumns,
      type: replicaIdentity,
    },
    // For now, deprecated columns are sent for backwards compatibility.
    // These can be removed when bumping the MIN_PROTOCOL_VERSION to 5.
    keyColumns,
    replicaIdentity,
  };
}

class UnsupportedSchemaChangeError extends Error {
  readonly name = 'UnsupportedSchemaChangeError';
  readonly description: string;
  readonly ddlUpdate: DdlUpdateEvent;

  constructor(
    description: string,
    ddlUpdate: DdlUpdateEvent,
    options?: ErrorOptions,
  ) {
    super(
      `Replication halted. Resync the replica to recover: ${description}`,
      options,
    );
    this.description = description;
    this.ddlUpdate = ddlUpdate;
  }
}

class MissingEventTriggerSupport extends Error {
  readonly name = 'MissingEventTriggerSupport';

  constructor(msg: string) {
    super(
      `${msg}. Schema changes cannot be reliably replicated without event trigger support.`,
    );
  }
}

// TODO(0xcadams): should this be a ProtocolError?
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
