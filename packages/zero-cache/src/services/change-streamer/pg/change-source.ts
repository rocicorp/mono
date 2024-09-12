import {LogContext} from '@rocicorp/logger';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {assertString} from 'shared/src/asserts.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {registerPostgresTypeParsers} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import {ChangeSource, ChangeStream} from '../change-streamer-service.js';
import {ChangeEntry} from '../change-streamer.js';
import {Change, MessageCommit} from '../schema/change.js';
import {ReplicationConfig} from '../schema/tables.js';
import {replicationSlot} from './initial-sync.js';
import {toLexiVersion} from './lsn.js';
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

  startStream(): ChangeStream {
    let lastLSN = '0/0';

    const ack = (commit?: MessageCommit) => {
      if (commit) {
        assertString(commit.commitEndLsn);
        lastLSN = commit.commitEndLsn;
      }
      void service.acknowledge(lastLSN);
    };

    const changes = Subscription.create<ChangeEntry>({
      cleanup: () => service.stop(),
    });

    const service = new LogicalReplicationService(
      {connectionString: this.#upstreamUri},
      {acknowledge: {auto: false, timeoutSeconds: 0}},
    )
      .on('heartbeat', (_lsn, _time, respond) => {
        respond && ack();
      })
      .on('data', (lsn, msg) => {
        const change = messageToChangeEntry(lsn, msg);
        if (change) {
          changes.push(change);
        }
      });

    this.#lc.debug?.('starting upstream replication stream');
    service
      .subscribe(
        new PgoutputPlugin({
          protoVersion: 1,
          publicationNames: this.#replicationConfig.publications,
        }),
        replicationSlot(this.#replicaID),
        lastLSN,
      )
      .catch(e => changes.fail(e instanceof Error ? e : new Error(String(e))))
      .finally(() => changes.cancel());

    return {changes, acks: {push: ack}};
  }
}

function messageToChangeEntry(lsn: string, msg: Pgoutput.Message) {
  const change = msg as Change;
  const {tag} = change;
  switch (tag) {
    case 'begin':
    case 'insert':
    case 'update':
    case 'delete':
    case 'truncate':
    case 'commit': {
      const watermark = toLexiVersion(lsn, tag);
      return {watermark, change};
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
