import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import postgres from 'postgres';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {registerPostgresTypeParsers} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import {ChangeSource, ChangeStream} from '../change-streamer-service.js';
import {Commit, DownstreamChange} from '../change-streamer.js';
import {Change} from '../schema/change.js';
import {ReplicationConfig} from '../schema/tables.js';
import {replicationSlot} from './initial-sync.js';
import {fromLexiVersion, toLexiVersion} from './lsn.js';
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
  replicaID: string,
  replicaDbFile: string,
): Promise<ChangeSource> {
  await initSyncSchema(
    lc,
    'change-streamer',
    replicaID,
    replicaDbFile,
    upstreamURI,
  );

  const replica = new Database(lc, replicaDbFile);
  const replicationConfig = getSubscriptionState(new StatementRunner(replica));

  return new PostgresChangeSource(
    lc,
    upstreamURI,
    replicaID,
    replicationConfig,
  );
}

/**
 * Postgres implementation of a {@link ChangeSource} backed by a logical
 * replication stream.
 */
class PostgresChangeSource implements ChangeSource {
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #replicaID: string;
  readonly #replicationConfig: ReplicationConfig;

  constructor(
    lc: LogContext,
    upstreamUri: string,
    replicaID: string,
    replicationConfig: ReplicationConfig,
  ) {
    this.#lc = lc.withContext('component', 'change-source');
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replicationConfig = replicationConfig;
  }

  async startStream(): Promise<ChangeStream> {
    // Note: Starting a replication stream at '0/0' defaults to starting at
    // the slot's `confirmed_flush_lsn`, as detailed in
    // https://www.postgresql.org/docs/current/protocol-replication.html#PROTOCOL-REPLICATION-START-REPLICATION-SLOT-LOGICAL
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
    const {promise: confirmedFlushLSN, resolve, reject} = resolver<string>();

    const service = new LogicalReplicationService(
      {connectionString: this.#upstreamUri},
      {acknowledge: {auto: false, timeoutSeconds: 0}},
    )
      .on('start', () => this.getConfirmedFlushLSN().then(resolve, reject))
      .on('heartbeat', (_lsn, _time, respond) => {
        respond && ack();
      })
      .on('data', (lsn, msg) => {
        const change = messageToDownstream(lsn, msg);
        if (change) {
          changes.push(change);
        }
      });

    this.#lc.info?.(`starting replication stream`);
    service
      .subscribe(
        new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: this.#replicationConfig.publications,
        }),
        replicationSlot(this.#replicaID),
        lastLSN,
      )
      .catch(e => {
        reject(e);
        changes.fail(e instanceof Error ? e : new Error(String(e)));
      })
      .finally(() => changes.cancel());

    const lsn = await confirmedFlushLSN;
    const watermark = toLexiVersion(lsn);
    this.#lc.info?.(`replication stream started at ${lsn} (${watermark})`);

    return {
      initialWatermark: watermark,
      changes,
      acks: {push: ack},
    };
  }

  /**
   * When a replication slot is created its `confirmed_flush_lsn` is uninitialized, e.g.
   *
   * ```
   *  slot_name | restart_lsn | confirmed_flush_lsn
   *  -----------+-------------+---------------------
   *  zero_slot | 8F/38ACB2F8 | 0/1
   * ```
   *
   * Using the greater of the `restart_lsn` and `confirmed_flush_lsn` produces
   * the desired initial watermark.
   */
  async getConfirmedFlushLSN(): Promise<string> {
    const db = postgres(this.#upstreamUri);
    const slot = replicationSlot(this.#replicaID);
    try {
      const result = await db<{restart: string; confirmed: string}[]>`
      SELECT restart_lsn as restart, confirmed_flush_lsn as confirmed
        FROM pg_replication_slots 
        WHERE slot_name = ${slot}`;
      if (result.length === 1) {
        const {restart, confirmed} = result[0];
        return toLexiVersion(confirmed) > toLexiVersion(restart)
          ? confirmed
          : restart;
      }
      throw new Error(`Upstream is missing replication slot ${slot}`);
    } finally {
      await db.end();
    }
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
