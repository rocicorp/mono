import {PG_ADMIN_SHUTDOWN} from '@drdgvhbh/postgres-error-codes';
import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {DatabaseError} from 'pg-protocol';
import {AbortError} from 'shared/src/abort-error.js';
import {deepEqual} from 'shared/src/json.js';
import {sleep} from 'shared/src/sleep.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {max, oneAfter} from 'zero-cache/src/types/lexi-version.js';
import {
  pgClient,
  registerPostgresTypeParsers,
  type PostgresDB,
} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import type {ChangeSource, ChangeStream} from '../change-streamer-service.js';
import type {Commit, DownstreamChange} from '../change-streamer.js';
import type {Change} from '../schema/change.js';
import type {ReplicationConfig} from '../schema/tables.js';
import {replicationSlot} from './initial-sync.js';
import {fromLexiVersion, toLexiVersion} from './lsn.js';
import {INTERNAL_PUBLICATION_PREFIX} from './schema/zero.js';
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
    'change-streamer',
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

      const service = new LogicalReplicationService(
        {connectionString: this.#upstreamUri},
        {acknowledge: {auto: false, timeoutSeconds: 0}},
      )
        .on('start', () =>
          this.getNextWatermark(db, slot, clientStart)
            .then(resolve)
            .catch(e => reject(translateError(e))),
        )
        .on('heartbeat', (_lsn, _time, respond) => {
          respond && ack();
        })
        .on('data', (lsn, msg) => {
          const change = messageToDownstream(lsn, msg);
          if (change) {
            changes.push(change);
          }
        })
        .on('error', err => {
          this.#lc.error?.('error from upstream postgres', err);
          changes.fail(err);
        });

      await this.stopExistingReplicationSlotSubscriber(db, slot);

      this.#lc.info?.(`starting replication stream @${slot}`);
      service
        .subscribe(
          new PgoutputPlugin({
            protoVersion: 1,
            publicationNames: this.#replicationConfig.publications,
          }),
          slot,
          fromLexiVersion(clientStart),
        )
        .catch(e => {
          const err = translateError(e);
          reject(err);
          changes.fail(err);
        })
        .finally(() => changes.cancel());

      const initialWatermark = await nextWatermark;
      this.#lc.info?.(
        `replication stream@${slot} started at ${initialWatermark}`,
      );

      return {initialWatermark, changes, acks: {push: ack}};
    } finally {
      await db.end();
    }
  }

  async stopExistingReplicationSlotSubscriber(
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
  async getNextWatermark(
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

    this.#lc.info?.(
      `confirmed_flush_lsn:${confirmed}, restart_lsn:${restart}, clientWatermark:${fromLexiVersion(
        clientStart,
      )}`,
    );
    return max(
      oneAfter(confirmedWatermark),
      oneAfter(restartWatermark),
      clientStart,
    );
  }
}

function messageToDownstream(
  lsn: string,
  msg: Pgoutput.Message,
): DownstreamChange | undefined {
  const change = msg as Change;
  const {tag} = change;
  switch (tag) {
    case 'begin':
      return ['begin', change];
    case 'insert':
    case 'update':
    case 'delete':
    case 'truncate':
      return ['data', change];
    case 'commit': {
      const watermark = toLexiVersion(lsn);
      return ['commit', change, {watermark}];
    }

    default:
      change satisfies never; // All Change types are covered.

      // But we can technically receive other Message types.
      switch (msg.tag) {
        case 'relation':
          return undefined; // Explicitly ignored. Schema handling is TODO.
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
          throw new Error(`Unexpected message type ${stringify(msg)}`);
      }
  }
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
