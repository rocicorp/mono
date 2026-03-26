import {
  PG_ADMIN_SHUTDOWN,
  PG_OBJECT_IN_USE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import postgres from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {assert} from '../../../../../shared/src/asserts.ts';
import {stringify} from '../../../../../shared/src/bigint-json.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {mapValues} from '../../../../../shared/src/objects.ts';
import {
  equals,
  intersection,
  symmetricDifferences,
} from '../../../../../shared/src/set-utils.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  mapPostgresToLiteColumn,
  UnsupportedColumnDefaultError,
} from '../../../db/pg-to-lite.ts';
import {runTx} from '../../../db/run-transaction.ts';
import type {
  ColumnSpec,
  PublishedIndexSpec,
  PublishedTableSpec,
} from '../../../db/specs.ts';
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
  getSubscriptionStateAndContext,
  type SubscriptionState,
  type SubscriptionStateAndContext,
} from '../../replicator/schema/replication-state.ts';
import type {ChangeSource, ChangeStream} from '../change-source.ts';
import {BackfillManager} from '../common/backfill-manager.ts';
import {
  ChangeStreamMultiplexer,
  type Listener,
} from '../common/change-stream-multiplexer.ts';
import {initReplica} from '../common/replica-schema.ts';
import type {
  BackfillRequest,
  DownstreamStatusMessage,
  JSONObject,
} from '../protocol/current.ts';
import type {
  ColumnAdd,
  Identifier,
  MessageRelation,
  SchemaChange,
  TableCreate,
} from '../protocol/current/data.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
  Data,
} from '../protocol/current/downstream.ts';
import type {ColumnMetadata, TableMetadata} from './backfill-metadata.ts';
import {streamBackfill} from './backfill-stream.ts';
import {
  initialSync,
  type InitialSyncOptions,
  type ServerContext,
} from './initial-sync.ts';
import type {
  Message,
  MessageMessage,
  MessageRelation as PostgresRelation,
} from './logical-replication/pgoutput.types.ts';
import {subscribe, type StreamMessage} from './logical-replication/stream.ts';
import {fromBigInt, toStateVersionString, type LSN} from './lsn.ts';
import {
  replicationEventSchema,
  type DdlUpdateEvent,
  type SchemaSnapshotEvent,
} from './schema/ddl.ts';
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
  context: ServerContext,
  lagReportIntervalMs?: number,
): Promise<{subscriptionState: SubscriptionState; changeSource: ChangeSource}> {
  await initReplica(
    lc,
    `replica-${shard.appID}-${shard.shardNum}`,
    replicaDbFile,
    (log, tx) => initialSync(log, shard, tx, upstreamURI, syncOptions, context),
  );

  const replica = new Database(lc, replicaDbFile);
  const subscriptionState = getSubscriptionStateAndContext(
    new StatementRunner(replica),
  );
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
      context,
      lagReportIntervalMs ?? null,
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
  {
    replicaVersion,
    publications: subscribed,
    initialSyncContext,
  }: SubscriptionStateAndContext,
) {
  // Perform any shard schema updates
  await updateShardSchema(lc, sql, shard, replicaVersion);

  const upstreamReplica = await getReplicaAtVersion(
    lc,
    sql,
    shard,
    replicaVersion,
    initialSyncContext,
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
  readonly #db: PostgresDB;
  readonly #upstreamUri: string;
  readonly #shard: ShardID;
  readonly #replica: Replica;
  readonly #context: ServerContext;
  readonly #lagReporter: LagReporter | null;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    shard: ShardID,
    replica: Replica,
    context: ServerContext,
    lagReportIntervalMs: number | null,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#db = pgClient(lc, upstreamUri, {
      // used occasionally for schema changes, periodically for lag reporting
      ['idle_timeout']: 60,
      connection: {['application_name']: 'zero-replication-monitor'},
    });
    this.#upstreamUri = upstreamUri;
    this.#shard = shard;
    this.#replica = replica;
    this.#context = context;
    this.#lagReporter = lagReportIntervalMs
      ? new LagReporter(
          lc.withContext('component', 'lag-reporter'),
          shard,
          this.#db,
          lagReportIntervalMs,
        )
      : null;
  }

  startLagReporter(): Promise<{nextSendTimeMs: number}> | null {
    return this.#lagReporter ? this.#lagReporter.initiateLagReport() : null;
  }

  async startStream(
    clientWatermark: string,
    backfillRequests: BackfillRequest[] = [],
  ): Promise<ChangeStream> {
    const {slot} = this.#replica;

    await this.#stopExistingReplicationSlotSubscribers(slot);
    const config = await getInternalShardConfig(this.#db, this.#shard);
    this.#lc.info?.(`starting replication stream@${slot}`);
    return this.#startStream(slot, clientWatermark, config, backfillRequests);
  }

  async #startStream(
    slot: string,
    clientWatermark: string,
    shardConfig: InternalShardConfig,
    backfillRequests: BackfillRequest[],
  ): Promise<ChangeStream> {
    const clientStart = majorVersionFromString(clientWatermark) + 1n;
    const {messages, acks} = await subscribe(
      this.#lc,
      this.#db,
      slot,
      [...shardConfig.publications],
      clientStart,
    );
    const acker = new Acker(acks);

    // The ChangeStreamMultiplexer facilitates cooperative streaming from
    // the main replication stream and backfill streams initiated by the
    // BackfillManager.
    const changes = new ChangeStreamMultiplexer(this.#lc, clientWatermark);
    const backfillManager = new BackfillManager(this.#lc, changes, req =>
      streamBackfill(this.#lc, this.#upstreamUri, this.#replica, req),
    );
    changes
      .addProducers(messages, backfillManager)
      .addListeners(backfillManager, acker);
    backfillManager.run(clientWatermark, backfillRequests);

    const changeMaker = new ChangeMaker(
      this.#lc,
      this.#shard,
      shardConfig,
      this.#db,
      this.#replica.initialSchema,
    );

    /**
     * Determines if the incoming message is transactional, otherwise handling
     * non-transactional messages with a downstream status message.
     */
    const isTransactionalMessage = (
      lsn: bigint,
      msg: StreamMessage[1],
    ): msg is Message => {
      if (msg.tag === 'keepalive') {
        changes.pushStatus([
          'status',
          {ack: msg.shouldRespond},
          {watermark: majorVersionToString(lsn)},
        ]);
        return false;
      }
      if (
        msg.tag === 'message' &&
        msg.prefix === this.#lagReporter?.messagePrefix
      ) {
        changes.pushStatus(this.#lagReporter.processLagReport(msg));
        return false;
      }
      return true;
    };

    void (async () => {
      try {
        let reservation: ReservationState | null = null;
        let inTransaction = false;

        for await (const [lsn, msg] of messages) {
          if (!isTransactionalMessage(lsn, msg)) {
            // If we're not in a transaction but the last reservation was kept
            // because of pending keepalives or lag reports in the queue,
            // release the reservation.
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
        const err = translateError(e);
        if (err instanceof ShutdownSignal) {
          // Log the new state of the replica to surface information about the
          // server that sent the shutdown signal, if any.
          await this.#logCurrentReplicaInfo();
        }
        changes.fail(err);
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

  async #logCurrentReplicaInfo() {
    try {
      const replica = await getReplicaAtVersion(
        this.#lc,
        this.#db,
        this.#shard,
        this.#replica.version,
      );
      if (replica) {
        this.#lc.info?.(
          `Shutdown signal from replica@${this.#replica.version}: ${stringify(replica.subscriberContext)}`,
        );
      }
    } catch (e) {
      this.#lc.warn?.(`error logging replica info`, e);
    }
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
  async #stopExistingReplicationSlotSubscribers(slotToKeep: string) {
    const slotExpression = replicationSlotExpression(this.#shard);
    const legacySlotName = legacyReplicationSlot(this.#shard);

    const result = await runTx(this.#db, async sql => {
      // Note: `slot_name <= slotToKeep` uses a string compare of the millisecond
      // timestamp, which works until it exceeds 13 digits (sometime in 2286).
      const result = await sql<
        {slot: string; pid: string | null; terminated: boolean | null}[]
      > /*sql*/ `
      SELECT slot_name as slot, pg_terminate_backend(active_pid) as terminated, active_pid as pid
        FROM pg_replication_slots 
        WHERE (slot_name LIKE ${slotExpression} OR slot_name = ${legacySlotName})
              AND slot_name <= ${slotToKeep}`;
      this.#lc.info?.(
        `terminated replication slots: ${JSON.stringify(result)}`,
      );
      const replicasTable = `${upstreamSchema(this.#shard)}.replicas`;
      const replicasBefore = await sql`
        SELECT slot, version, "initialSyncContext", "subscriberContext" 
          FROM ${sql(replicasTable)} ORDER BY slot`;

      if (result.length === 0) {
        const shardSlots = await sql`
        SELECT slot_name as slot, active, active_pid as pid
          FROM pg_replication_slots
          WHERE slot_name LIKE ${slotExpression} OR slot_name = ${legacySlotName}
          ORDER BY slot_name`;
        this.#lc.warn?.(
          `slot ${slotToKeep} not found while cleaning subscribers`,
          {slots: shardSlots, replicas: replicasBefore},
        );
        throw new AbortError(
          `replication slot ${slotToKeep} is missing. A different ` +
            `replication-manager should now be running on a new ` +
            `replication slot.`,
        );
      }
      // Clear the state of the older replicas.
      this.#lc.info?.(
        `replicas before cleanup (slotToKeep=${slotToKeep}): ${JSON.stringify(
          replicasBefore,
        )}`,
      );
      await sql`
        DELETE FROM ${sql(replicasTable)} WHERE slot < ${slotToKeep}`;
      await sql`
        UPDATE ${sql(replicasTable)} 
          SET "subscriberContext" = ${this.#context}
          WHERE slot = ${slotToKeep}`;
      const replicasAfter = await sql<{slot: string; version: string}[]>`
      SELECT slot, version FROM ${sql(replicasTable)} ORDER BY slot`;
      this.#lc.info?.(
        `replicas after cleanup (slotToKeep=${slotToKeep}): ${JSON.stringify(
          replicasAfter,
        )}`,
      );
      return result;
    });

    const pids = result.filter(({pid}) => pid !== null).map(({pid}) => pid);
    if (pids.length) {
      this.#lc.info?.(`signaled subscriber ${pids} to shut down`);
    }
    const otherSlots = result
      .filter(({slot}) => slot !== slotToKeep)
      .map(({slot}) => slot);

    if (otherSlots.length) {
      void this.#dropReplicationSlots(otherSlots).catch(e =>
        this.#lc.warn?.(`error dropping replication slots`, e),
      );
    }
  }

  async #dropReplicationSlots(slots: string[]) {
    this.#lc.info?.(`dropping other replication slot(s) ${slots}`);
    const sql = this.#db;
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
export class Acker implements Listener {
  #acks: Sink<bigint>;
  #waitingForDownstreamAck: string | null = null;

  constructor(acks: Sink<bigint>) {
    this.#acks = acks;
  }

  onChange(change: ChangeStreamMessage): void {
    switch (change[0]) {
      case 'status':
        const {watermark} = change[2];
        if (change[1].ack) {
          this.#expectDownstreamAck(watermark);
        } else {
          // Keepalives with shouldRespond = false are sent to Listeners,
          // but for efficiency they are not sent downstream to the
          // change-streamer. Ack them here if the change-streamer is caught
          // up. This updates the replication slot's `confirmed_flush_lsn`
          // more quickly (rather than waiting for the periodic shouldRespond),
          // which is useful for monitoring replication slot lag.
          this.#ackIfDownstreamIsCaughtUp(watermark);
        }
        break;
      case 'begin':
        // Mark the commit watermark as being expected so that any intermediate
        // shouldRespond=false watermarks, which will be at the
        // commitWatermark, are *not* acked, as the ack must come from
        // change-streamer after it commits the transaction.
        if (!change[1].skipAck) {
          this.#expectDownstreamAck(change[2].commitWatermark);
        }
        break;
    }
  }

  #expectDownstreamAck(watermark: string) {
    this.#waitingForDownstreamAck = watermark;
  }

  ack(watermark: LexiVersion) {
    if (
      this.#waitingForDownstreamAck &&
      this.#waitingForDownstreamAck <= watermark
    ) {
      this.#waitingForDownstreamAck = null;
    }
    this.#sendAck(watermark);
  }

  #ackIfDownstreamIsCaughtUp(watermark: string) {
    if (this.#waitingForDownstreamAck === null) {
      this.#sendAck(watermark);
    }
  }

  #sendAck(watermark: LexiVersion) {
    const lsn = majorVersionFromString(watermark);
    this.#acks.push(lsn);
  }
}

const lagReportSchema = v.object({
  id: v.string(),
  sendTimeMs: v.number(),
  commitTimeMs: v.number(),
});

export type LagReport = v.Infer<typeof lagReportSchema>;

class LagReporter {
  static readonly MESSAGE_SUFFIX = '/lag-report/v1';

  readonly #lc: LogContext;
  readonly messagePrefix: string;

  // Weird issue with oxlint, which thinks:
  // × eslint(no-unused-private-class-members): 'db' is defined but never used.
  // oxlint-disable-next-line eslint(no-unused-private-class-members)
  readonly #db: PostgresDB;
  readonly #lagIntervalMs: number;

  #pgVersion: number | undefined;
  #lastReportID: string = '';
  #timer: NodeJS.Timeout | undefined;

  constructor(
    lc: LogContext,
    shard: ShardID,
    db: PostgresDB,
    lagIntervalMs: number,
  ) {
    this.#lc = lc;
    this.messagePrefix = `${shard.appID}/${shard.shardNum}${LagReporter.MESSAGE_SUFFIX}`;
    this.#db = db;
    this.#lagIntervalMs = lagIntervalMs;
  }

  async #getPgVersion() {
    if (this.#pgVersion === undefined) {
      const [{pgVersion}] = await this.#db<{pgVersion: number}[]> /*sql*/ `
        SELECT current_setting('server_version_num')::int as "pgVersion"`;
      this.#pgVersion = pgVersion;
    }
    return this.#pgVersion;
  }

  async initiateLagReport(now = Date.now()) {
    const pgVersion = this.#pgVersion ?? (await this.#getPgVersion());
    this.#lastReportID = nanoid();

    if (pgVersion >= 170000) {
      await this.#db /*sql*/ `
        SELECT pg_logical_emit_message(
          false,
          ${this.messagePrefix},
          json_build_object(
            'id', ${this.#lastReportID}::text,
            'sendTimeMs', ${now}::int8,
            'commitTimeMs', extract(epoch from now()) * 1000
          )::text,
          true
        );
    `;
    } else {
      // Versions before PG 17 do not support the final `flush` option of
      // pg_logical_emit_message(). This results in an extra 50~100ms latency
      // for replication reports when the db is idle, which is still
      // acceptable for the purpose for alerting on pathological lag, for
      // which the threshold is much higher (e.g. many seconds).
      await this.#db /*sql*/ `
        SELECT pg_logical_emit_message(
          false,
          ${this.messagePrefix},
          json_build_object(
            'id', ${this.#lastReportID}::text,
            'sendTimeMs', ${now}::int8,
            'commitTimeMs', extract(epoch from now()) * 1000
          )::text
        );
    `;
    }
    return {nextSendTimeMs: now};
  }

  #scheduleNextReport(delayMs: number) {
    clearTimeout(this.#timer);
    this.#timer = setTimeout(async () => {
      try {
        await this.initiateLagReport();
      } catch (e) {
        this.#lc.warn?.(`error initiating lag report`, e);
        this.#scheduleNextReport(this.#lagIntervalMs);
      }
    }, delayMs);
  }

  processLagReport(msg: MessageMessage): DownstreamStatusMessage {
    assert(
      msg.prefix === this.messagePrefix,
      `unexpected message prefix: ${msg.prefix}`,
    );
    const report = parseLogicalMessageContent(msg, lagReportSchema);
    const now = Date.now();
    const nextSendTimeMs = Math.max(
      now,
      report.sendTimeMs + this.#lagIntervalMs,
    );
    if (report.id === this.#lastReportID) {
      // Only schedule the next report when receiving the previous report.
      // For historic reports in the WAL, or reports generated by other
      // replication-managers, status messages are still sent downstream,
      // but the next report is not actually scheduled.
      this.#scheduleNextReport(nextSendTimeMs - now);
    }
    const {sendTimeMs, commitTimeMs} = report;
    return [
      'status',
      {
        ack: false,
        lagReport: {
          lastTimings: {
            sendTimeMs,
            commitTimeMs,
            receiveTimeMs: now,
          },
          nextSendTimeMs,
        },
      },
      {watermark: toStateVersionString(msg.messageLsn ?? '0/0')},
    ];
  }
}

type ReplicationError = {
  lsn: bigint;
  msg: Message;
  err: unknown;
  lastLogTime: number;
};

const SET_REPLICA_IDENTITY_DELAY_MS = 50;

class ChangeMaker {
  readonly #lc: LogContext;
  readonly #shardPrefix: string;
  readonly #shardConfig: InternalShardConfig;
  readonly #initialSchema: PublishedSchema;
  readonly #db: PostgresDB;

  #replicaIdentityTimer: NodeJS.Timeout | undefined;
  #error: ReplicationError | undefined;

  constructor(
    lc: LogContext,
    {appID, shardNum}: ShardID,
    shardConfig: InternalShardConfig,
    db: PostgresDB,
    initialSchema: PublishedSchema,
  ) {
    this.#lc = lc;
    // Note: This matches the prefix used in pg_logical_emit_message() in pg/schema/ddl.ts.
    this.#shardPrefix = `${appID}/${shardNum}`;
    this.#shardConfig = shardConfig;
    this.#initialSchema = initialSchema;
    this.#db = db;
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
        errorDetails.context = err.event.context;
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
          ? err.event.context
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
        if (!msg.prefix.startsWith(this.#shardPrefix)) {
          this.#lc.debug?.('ignoring message for different shard', msg.prefix);
          return [];
        }
        switch (msg.prefix.substring(this.#shardPrefix.length)) {
          case '': // Legacy prefix
          case '/ddl':
            return this.#handleDdlMessage(msg);
          default:
            this.#lc.debug?.('ignoring unknown message type', msg.prefix);
            return [];
        }

      case 'commit':
        this.#lastSnapshotInTx = undefined;
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
  #lastSnapshotInTx: PublishedSchema | undefined;

  #handleDdlMessage(msg: MessageMessage) {
    const event = parseLogicalMessageContent(msg, replicationEventSchema);
    // Cancel manual schema adjustment timeouts when an upstream schema change
    // is about to happen, so as to avoid interfering / redundant work.
    clearTimeout(this.#replicaIdentityTimer);

    let previousSchema: PublishedSchema | null;
    const {type} = event;
    switch (type) {
      case 'ddlStart':
        // Store the schema in order to diff it with a subsequent ddlUpdate.
        this.#preSchema = event.schema;
        return [];
      case 'ddlUpdate':
        // guaranteed by event triggers
        previousSchema = must(
          this.#preSchema,
          `ddlUpdate received without a ddlStart`,
        );
        break;
      case 'schemaSnapshot':
        previousSchema = this.#lastSnapshotInTx ?? null;
        break;
      default: // Ignore unknown types for forwards compatibility
        this.#lc.info?.(`ignoring unknown ddl message type: ${type}`);
        return [];
    }

    // Store the schema (from either a ddlUpdate or schemaSnapshot) to
    // diff against the next schemaSnapshot.
    this.#lastSnapshotInTx = event.schema;
    if (!previousSchema) {
      this.#lc.info?.(`received ${msg.prefix}/${type} event`);
      return []; // First schemaSnapshot in the tx.
    }
    this.#lc.info?.(`processing ${msg.prefix}/${type} event`, event);

    const changes = this.#makeSchemaChanges(previousSchema, event).map(
      change => ['data', change] satisfies Data,
    );

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
          await replicaIdentities.apply(this.#lc, this.#db);
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
    update: DdlUpdateEvent | SchemaSnapshotEvent,
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

      // Detect modified indexes (same name, different definition).
      // This happens when a constraint is dropped and recreated with the
      // same name in a single ALTER TABLE statement.
      // Note: We compare using stable column attnums rather than names,
      // because table/column renames change the index spec cosmetically
      // (tableName, column keys) without the index actually being recreated.
      const keptIdx = intersection(prevIdx, nextIdx);
      for (const id of keptIdx) {
        if (
          isIndexStructurallyChanged(
            must(prevIdx.get(id)),
            must(nextIdx.get(id)),
            prevTbl,
            nextTbl,
          )
        ) {
          droppedIdx.add(id);
          createdIdx.add(id);
        }
      }

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
      // ALTER TABLE | ALTER PUBLICATION
      const tables = intersection(prevTbl, nextTbl);
      for (const id of tables) {
        changes.push(
          ...this.#getTableChanges(
            must(prevTbl.get(id)),
            must(nextTbl.get(id)),
            update.event.tag,
          ),
        );
      }
      // CREATE
      for (const id of createdTbl) {
        const spec = must(nextTbl.get(id));
        const createTable: TableCreate = {
          tag: 'create-table',
          spec,
          metadata: getMetadata(spec),
        };
        if (!update.event.tag.startsWith('CREATE')) {
          // Tables introduced to the publication via ALTER statements
          // or the COMMENT statement (from schemaSnapshots) must be
          // backfilled.
          createTable.backfill = mapValues(spec.columns, ({pos: attNum}) => ({
            attNum,
          })) satisfies Record<string, ColumnMetadata>;
        }
        changes.push(createTable);
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
    ddlTag: string,
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

    // All columns introduced by a publication change require backfill
    // (which appear as ALTER PUBLICATION or COMMENT tags).
    // Columns created by ALTER TABLE, on the other hand, only require
    // backfill if they have non-constant defaults.
    const alwaysBackfill = ddlTag !== 'ALTER TABLE';

    // ADD
    for (const id of added) {
      const {name, ...spec} = must(newColumns.get(id));
      const column = {name, spec};
      const addColumn: ColumnAdd = {
        tag: 'add-column',
        table,
        column,
        tableMetadata: getMetadata(newTable),
      };
      if (alwaysBackfill) {
        addColumn.column.spec.dflt = null;
        addColumn.backfill = {attNum: spec.pos} satisfies ColumnMetadata;
      } else {
        // Determine if the ChangeProcessor will accept the column add as is.
        try {
          mapPostgresToLiteColumn(table.name, column);
        } catch (e) {
          if (!(e instanceof UnsupportedColumnDefaultError)) {
            // Note: mapPostgresToLiteColumn is not expected to throw any other
            // types of errors.
            throw e;
          }
          // If the column has an unsupported default (e.g. an expression or a
          // generated value), create the column as initially hidden with a
          // `null` default, and publish it after backfilling the values from
          // upstream. Note that this does require that the table have a valid
          // REPLICA IDENTITY, since backfill relies on merging new data with
          // an existing row.
          this.#lc.info?.(
            `Backfilling column ${table.name}.${name}: ${String(e)}`,
          );
          addColumn.column.spec.dflt = null;
          addColumn.backfill = {attNum: spec.pos} satisfies ColumnMetadata;
        }
      }
      changes.push(addColumn);
    }
    return changes;
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
    const currentSchema = await getPublicationInfo(this.#db, publications);
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

/**
 * Determines if an index was structurally changed (e.g. constraint dropped
 * and recreated with different columns) vs cosmetically changed (e.g. the
 * index spec changed because the table or a column was renamed).
 *
 * Compares boolean properties directly and resolves column names to their
 * stable attnums (pg_attribute `attnum`) for the column comparison.
 */
function isIndexStructurallyChanged(
  prev: PublishedIndexSpec,
  next: PublishedIndexSpec,
  prevTables: Map<number, PublishedTableWithReplicaIdentity>,
  nextTables: Map<number, PublishedTableWithReplicaIdentity>,
): boolean {
  if (
    prev.unique !== next.unique ||
    prev.isPrimaryKey !== next.isPrimaryKey ||
    prev.isReplicaIdentity !== next.isReplicaIdentity ||
    prev.isImmediate !== next.isImmediate
  ) {
    return true;
  }

  const prevTable = findTableBySchemaAndName(
    prevTables,
    prev.schema,
    prev.tableName,
  );
  const nextTable = findTableBySchemaAndName(
    nextTables,
    next.schema,
    next.tableName,
  );
  if (!prevTable || !nextTable) {
    // Can't resolve tables; conservatively treat as changed.
    return true;
  }

  const prevEntries = Object.entries(prev.columns);
  const nextEntries = Object.entries(next.columns);
  if (prevEntries.length !== nextEntries.length) {
    return true;
  }

  // Resolve column names → attnums and compare.
  const prevByAttnum = new Map<number | undefined, string>(
    prevEntries.map(([name, dir]) => [prevTable.columns[name]?.pos, dir]),
  );
  const nextByAttnum = new Map<number | undefined, string>(
    nextEntries.map(([name, dir]) => [nextTable.columns[name]?.pos, dir]),
  );

  if (prevByAttnum.has(undefined) || nextByAttnum.has(undefined)) {
    // Column not found in table spec; conservatively treat as changed.
    return true;
  }
  if (prevByAttnum.size !== nextByAttnum.size) {
    return true;
  }
  for (const [attnum, dir] of prevByAttnum) {
    if (nextByAttnum.get(attnum) !== dir) {
      return true;
    }
  }
  return false;
}

function findTableBySchemaAndName(
  tables: Map<number, PublishedTableWithReplicaIdentity>,
  schema: string,
  name: string,
): PublishedTableWithReplicaIdentity | undefined {
  for (const table of tables.values()) {
    if (table.schema === schema && table.name === name) {
      return table;
    }
  }
  return undefined;
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
  readonly event: DdlUpdateEvent | SchemaSnapshotEvent;

  constructor(
    description: string,
    event: DdlUpdateEvent | SchemaSnapshotEvent,
    options?: ErrorOptions,
  ) {
    super(
      `Replication halted. Resync the replica to recover: ${description}`,
      options,
    );
    this.description = description;
    this.event = event;
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

function parseLogicalMessageContent<T>(
  {content}: MessageMessage,
  schema: v.Type<T>,
) {
  const str =
    content instanceof Buffer
      ? content.toString('utf-8')
      : new TextDecoder().decode(content);
  const json = JSON.parse(str);
  return v.parse(json, schema, 'passthrough');
}
