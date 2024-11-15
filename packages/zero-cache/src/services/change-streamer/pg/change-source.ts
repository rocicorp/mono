import {PG_ADMIN_SHUTDOWN} from '@drdgvhbh/postgres-error-codes';
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
} from 'pg-logical-replication/dist/output-plugins/pgoutput/pgoutput.types.js';
import {DatabaseError} from 'pg-protocol';
import {AbortError} from '../../../../../shared/src/abort-error.js';
import {assert} from '../../../../../shared/src/asserts.js';
import {deepEqual} from '../../../../../shared/src/json.js';
import {must} from '../../../../../shared/src/must.js';
import {
  intersection,
  symmetricDifference,
} from '../../../../../shared/src/set-utils.js';
import {sleep} from '../../../../../shared/src/sleep.js';
import * as v from '../../../../../shared/src/valita.js';
import {Database} from '../../../../../zqlite/src/db.js';
import {ShortLivedClient} from '../../../db/short-lived-client.js';
import type {
  ColumnSpec,
  PublishedTableSpec,
  TableSpec,
} from '../../../db/specs.js';
import {StatementRunner} from '../../../db/statements.js';
import {stringify} from '../../../types/bigint-json.js';
import {max, oneAfter, versionFromLexi} from '../../../types/lexi-version.js';
import {
  pgClient,
  registerPostgresTypeParsers,
  type PostgresDB,
} from '../../../types/pg.js';
import {Subscription} from '../../../types/subscription.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import type {ChangeSource, ChangeStream} from '../change-streamer-service.js';
import type {Commit, Data, DownstreamChange} from '../change-streamer.js';
import type {DataChange, Identifier, MessageDelete} from '../schema/change.js';
import type {ReplicationConfig} from '../schema/tables.js';
import {replicationSlot} from './initial-sync.js';
import {fromLexiVersion, toLexiVersion} from './lsn.js';
import {replicationEventSchema, type DdlUpdateEvent} from './schema/ddl.js';
import {updateShardSchema} from './schema/init.js';
import {getPublicationInfo, type PublishedSchema} from './schema/published.js';
import {
  getInternalShardConfig,
  INTERNAL_PUBLICATION_PREFIX,
  type InternalShardConfig,
} from './schema/shard.js';
import {validate} from './schema/validation.js';
import type {ShardConfig} from './shard-config.js';
import {initSyncSchema} from './sync-schema.js';

// BigInt support from LogicalReplicationService.
registerPostgresTypeParsers();

/**
 * Initializes a Postgres change source, including the initial sync of the
 * replica, before streaming changes from the corresponding logical replication
 * stream.
 */
export async function initializeChangeSource(
  lc: LogContext,
  upstreamURI: string,
  shard: ShardConfig,
  replicaDbFile: string,
): Promise<{replicationConfig: ReplicationConfig; changeSource: ChangeSource}> {
  await initSyncSchema(
    lc,
    `replica-${shard.id}`,
    shard,
    replicaDbFile,
    upstreamURI,
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

  const changeSource = new PostgresChangeSource(
    lc,
    upstreamURI,
    shard.id,
    replicationConfig,
  );

  return {replicationConfig, changeSource};
}

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
    const clientStart = oneAfter(clientWatermark);

    try {
      await this.#stopExistingReplicationSlotSubscriber(db, slot);

      // Perform any shard schema updates
      await updateShardSchema(this.#lc, db, {
        id: this.#shardID,
        publications: this.#replicationConfig.publications,
      });

      const config = await getInternalShardConfig(db, this.#shardID);

      this.#lc.info?.(`starting replication stream @${slot}`);

      // Unlike the postgres.js client, the pg client does not have an option to
      // only use SSL if the server supports it. We achieve it manually by
      // trying SSL first, and then falling back to connecting without SSL.
      try {
        return await this.#startStream(db, slot, clientStart, config, true);
      } catch (e) {
        if (e instanceof SSLUnsupportedError) {
          this.#lc.info?.('retrying upstream connection without SSL');
          return await this.#startStream(db, slot, clientStart, config, false);
        }
        throw e;
      }
    } finally {
      await db.end();
    }
  }

  async #startStream(
    db: PostgresDB,
    slot: string,
    clientStart: string,
    shardConfig: InternalShardConfig,
    useSSL: boolean,
  ) {
    let lastLSN = '0/0';

    const ack = (commit?: Commit) => {
      if (commit) {
        const {watermark} = commit[2];
        lastLSN = fromLexiVersion(watermark);
      }
      void service.acknowledge(lastLSN);
    };

    const changes = Subscription.create<DownstreamChange>({
      cleanup: () => service.stop(),
    });

    // To avoid a race condition when handing off the replication stream
    // between tasks, query the `confirmed_flush_lsn` for the replication
    // slot only after the replication stream starts, as that is when it
    // is guaranteed not to change (i.e. until we ACK a commit).
    const {promise: nextWatermark, resolve, reject} = resolver<string>();

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
      .on('start', () =>
        this.#getNextWatermark(db, slot, clientStart).then(
          resolve,
          handleError,
        ),
      )
      .on('heartbeat', (_lsn, _time, respond) => {
        respond && ack();
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

    service
      .subscribe(
        new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: this.#replicationConfig.publications,
          messages: true,
        }),
        slot,
        fromLexiVersion(clientStart),
      )
      .then(() => changes.cancel(), handleError);

    const initialWatermark = await nextWatermark;
    this.#lc.info?.(
      `replication stream@${slot} started at ${initialWatermark}`,
    );
    return {initialWatermark, changes, acks: {push: ack}};
  }

  async #stopExistingReplicationSlotSubscriber(
    db: PostgresDB,
    slot: string,
  ): Promise<void> {
    const result = await db<{pid: string}[]>`
    SELECT pg_terminate_backend(active_pid), active_pid as pid
      FROM pg_replication_slots WHERE slot_name = ${slot} and active = true`;
    if (result.length === 0) {
      this.#lc.debug?.(`no existing subscriber to replication slot`);
    } else {
      const {pid} = result[0];
      this.#lc.info?.(`signaled subscriber ${pid} to shut down`);

      // This reduces flakiness in which unit tests often fail with
      // an error when starting the replication stream:
      //
      // error: replication slot "zero_slot_change_source_test_id" is active for PID 268
      //
      // Presumably, waiting for small interval before connecting to Postgres
      // would also reduce this occurrence in production.
      await sleep(5);
    }
  }

  // Sometimes the `confirmed_flush_lsn` gets wiped, e.g.
  //
  // ```
  //  slot_name | restart_lsn | confirmed_flush_lsn
  //  -----------+-------------+---------------------
  //  zero_slot | 8F/38ACB2F8 | 0/1
  // ```
  //
  // Using the greatest of three values should always yield a correct result:
  // * `clientWatermark`    : ahead of `confirmed_flush_lsn` if an ACK was lost,
  //                          or if the `confirmed_flush_lsn` was wiped.
  // * `confirmed_flush_lsn`: ahead of the `clientWatermark` if the ChangeDB was wiped.
  // * `restart_lsn`        : if both the `confirmed_flush_lsn` and ChangeDB were wiped.
  async #getNextWatermark(
    db: PostgresDB,
    slot: string,
    clientStart: string,
  ): Promise<string> {
    const result = await db<{restart: string; confirmed: string}[]>`
      SELECT restart_lsn as restart, confirmed_flush_lsn as confirmed FROM pg_replication_slots 
        WHERE slot_name = ${slot}`;
    if (result.length === 0) {
      throw new Error(`Upstream is missing replication slot ${slot}`);
    }
    const {restart, confirmed} = result[0];
    const confirmedWatermark = toLexiVersion(confirmed);
    const restartWatermark = toLexiVersion(restart);

    // Postgres sometimes stores the `confirmed_flush_lsn` as is (making it an even number),
    // and sometimes it stores the lsn + 1.
    // Normalize this behavior to produce consistent starting points.
    const confirmedWatermarkIsEven =
      versionFromLexi(confirmedWatermark) % 2n === 0n;

    this.#lc.info?.(
      `confirmed_flush_lsn:${confirmed}, restart_lsn:${restart}, clientWatermark:${fromLexiVersion(
        clientStart,
      )}`,
    );
    return max(
      confirmedWatermarkIsEven
        ? oneAfter(confirmedWatermark)
        : confirmedWatermark,
      oneAfter(restartWatermark),
      clientStart,
    );
  }
}

type ReplicationError = {
  lsn: string;
  msg: Pgoutput.Message;
  err: unknown;
  lastLogTime: number;
};

class ChangeMaker {
  readonly #lc: LogContext;
  readonly #shardID: string;
  readonly #shardPrefix: string;
  readonly #shardConfig: InternalShardConfig;
  readonly #upstream: ShortLivedClient;

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
  ): Promise<DownstreamChange[]> {
    if (this.#error) {
      this.#logError(this.#error);
      return [];
    }
    try {
      return await this.#makeChanges(lsn, msg);
    } catch (err) {
      this.#error = {lsn, msg, err, lastLogTime: 0};
      this.#logError(this.#error);
      // Rollback the current transaction to avoid dangling transactions in
      // downstream processors (i.e. changeLog, replicator).
      return [['rollback', {tag: 'rollback'}]];
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
  async #makeChanges(
    lsn: string,
    msg: Pgoutput.Message,
  ): Promise<DownstreamChange[]> {
    switch (msg.tag) {
      case 'begin':
        return [['begin', msg]];

      case 'delete':
        assert(msg.key);
        return [['data', msg as MessageDelete]];

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

      case 'commit': {
        const watermark = toLexiVersion(lsn);
        return [['commit', msg, {watermark}]];
      }

      case 'relation':
        return this.#handleRelation(msg);
      case 'type':
        throw new Error(
          `Custom types are not supported (received "${msg.typeName}")`,
        );
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

    this.#lc.info?.(
      `${changes.length} schema change(s) for ${event.context.query}`,
      changes,
    );

    return changes;
  }

  /**
   *  A note on operation order:
   *
   * Postgres will drop related indexes when columns are dropped,
   * but SQLite will error instead (https://sqlite.org/forum/forumpost/2e62dba69f?t=c&hist).
   * The current workaround is to drop indexes first.
   *
   * More generally, the order of replicating DDL updates is:
   * - drop indexes
   * - alter tables
   * - drop tables
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
    const [prevTables, prevIndexes] = specsByName(preSchema);
    const [nextTables, nextIndexes] = specsByName(update.schema);
    const {tag} = update.event;
    const changes: DataChange[] = [];

    // Validate the new table schemas
    for (const table of nextTables.values()) {
      validate(this.#lc, this.#shardID, table);
    }

    const [dropped, created] = symmetricDifference(
      new Set(prevIndexes.keys()),
      new Set(nextIndexes.keys()),
    );

    // Drop indexes first so that allow dropping dependent objects.
    for (const id of dropped) {
      const {schema, name} = must(prevIndexes.get(id));
      changes.push({tag: 'drop-index', id: {schema, name}});
    }

    if (tag === 'ALTER PUBLICATION') {
      const tables = intersection(
        new Set(prevTables.keys()),
        new Set(nextTables.keys()),
      );
      for (const id of tables) {
        changes.push(
          ...this.#getAddedOrDroppedColumnChanges(
            must(prevTables.get(id)),
            must(nextTables.get(id)),
          ),
        );
      }
    } else if (tag === 'ALTER TABLE') {
      const altered = idString(update.event.table);
      const table = must(nextTables.get(altered));
      const prevTable = prevTables.get(altered);
      if (!prevTable) {
        // table rename. Find the old name.
        let old: Identifier | undefined;
        for (const [id, {schema, name}] of prevTables.entries()) {
          if (!nextTables.has(id)) {
            old = {schema, name};
            break;
          }
        }
        if (!old) {
          throw new Error(`can't find previous table: ${stringify(update)}`);
        }
        changes.push({tag: 'rename-table', old, new: table});
      } else {
        changes.push(...this.#getSingleColumnChange(prevTable, table));
      }
    }

    // Added/dropped tables are handled in the same way for most DDL updates, with
    // the exception being `ALTER TABLE`, for which a table rename should not be
    // confused as a drop + add.
    if (tag !== 'ALTER TABLE') {
      const [dropped, created] = symmetricDifference(
        new Set(prevTables.keys()),
        new Set(nextTables.keys()),
      );
      for (const id of dropped) {
        const {schema, name} = must(prevTables.get(id));
        changes.push({tag: 'drop-table', id: {schema, name}});
      }
      for (const id of created) {
        const spec = must(nextTables.get(id));
        changes.push({tag: 'create-table', spec});
      }
    }

    // Add indexes last since they may reference tables / columns that need
    // to be created first.
    for (const id of created) {
      const spec = must(nextIndexes.get(id));
      changes.push({tag: 'create-index', spec});
    }
    return changes;
  }

  // ALTER PUBLICATION can only add and drop columns, but never change them.
  #getAddedOrDroppedColumnChanges(
    oldTable: TableSpec,
    newTable: TableSpec,
  ): DataChange[] {
    const table = {schema: newTable.schema, name: newTable.name};
    const [dropped, added] = symmetricDifference(
      new Set(Object.keys(oldTable.columns)),
      new Set(Object.keys(newTable.columns)),
    );

    const changes: DataChange[] = [];
    for (const column of dropped) {
      changes.push({tag: 'drop-column', table, column});
    }
    for (const name of added) {
      changes.push({
        tag: 'add-column',
        table,
        column: {name, spec: newTable.columns[name]},
      });
    }

    return changes;
  }

  // ALTER TABLE can add, drop, or change/rename a single column.
  #getSingleColumnChange(
    oldTable: TableSpec,
    newTable: TableSpec,
  ): DataChange[] {
    const table = {schema: newTable.schema, name: newTable.name};
    const [d, a] = symmetricDifference(
      new Set(Object.keys(oldTable.columns)),
      new Set(Object.keys(newTable.columns)),
    );
    const dropped = [...d];
    const added = [...a];
    assert(
      dropped.length <= 1 && added.length <= 1,
      `too many dropped [${[dropped]}] or added [${[added]}] columns`,
    );
    if (dropped.length === 1 && added.length === 1) {
      const oldName = dropped[0];
      const newName = added[0];
      return [
        {
          tag: 'update-column',
          table,
          old: {name: oldName, spec: oldTable.columns[oldName]},
          new: {name: newName, spec: newTable.columns[newName]},
        },
      ];
    } else if (added.length) {
      const name = added[0];
      return [
        {
          tag: 'add-column',
          table,
          column: {name, spec: newTable.columns[name]},
        },
      ];
    } else if (dropped.length) {
      return [{tag: 'drop-column', table, column: dropped[0]}];
    }
    // Not a rename, add, or drop. Find the column with a relevant update.
    for (const [name, oldSpec] of Object.entries(oldTable.columns)) {
      const newSpec = newTable.columns[name];
      // Besides the name, we only care about the data type.
      // Default values and constraints are not relevant.
      if (oldSpec.dataType !== newSpec.dataType) {
        return [
          {
            tag: 'update-column',
            table,
            old: {name, spec: oldSpec},
            new: {name, spec: newSpec},
          },
        ];
      }
    }
    return [];
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
  async #handleRelation(rel: MessageRelation): Promise<DownstreamChange[]> {
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
        acol.typeOID !== bcol.typeOID
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
    return new AbortError(e.message, {cause: e});
  }
  return e;
}
const idString = (id: Identifier) => `${id.schema}.${id.name}`;

function specsByName(published: PublishedSchema) {
  return [
    // It would have been nice to use a CustomKeyMap here, but we rely on set-utils
    // operations which use plain Sets.
    new Map(published.tables.map(t => [idString(t), t])),
    new Map(published.indexes.map(i => [idString(i), i])),
  ] as const;
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
