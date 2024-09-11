import {LogContext} from '@rocicorp/logger';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {toLexiVersion} from 'zero-cache/src/types/lsn.js';
import {registerPostgresTypeParsers} from 'zero-cache/src/types/pg.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {getSubscriptionState} from '../../replicator/schema/replication-state.js';
import {ChangeSource, ChangeStream} from '../change-streamer-service.js';
import {ChangeEntry} from '../change-streamer.js';
import {Change} from '../schema/change.js';
import {ReplicationConfig} from '../schema/tables.js';
import {replicationSlot} from './initial-sync.js';
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
    this.#lc = lc;
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replicationConfig = replicationConfig;
  }

  startStream(): ChangeStream {
    let lastLSN = '0/0';

    const ack = (commit?: Pgoutput.MessageCommit) => {
      lastLSN = commit?.commitEndLsn ?? lastLSN;
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

/**
 * Postgres defines the "Log sequence number (LSN)" as a value that
 * "increases monotonically with each new WAL record":
 *
 * https://www.postgresql.org/docs/current/glossary.html#:~:text=Log%20sequence%20number%20(LSN)
 *
 * and a WAL record being a "low-level description of an individual data change".
 *
 * Unfortunately, 'begin' and 'commit' transaction markers are not technically
 * data changes, and while they often have their own LSNs, it is not always
 * the case.
 *
 * In fact, a 'begin' message always has the same LSN as its transaction's first
 * data change. Moreover, executing commands in quick succession can result
 * in a 'commit', the next 'begin', and the subsequent data change all sharing
 * the same LSN:
 *
 *
 * ```json
 * "8F/38B017C0": {
 *   "tag": "insert",
 *   "relation": {
 *   ...
 * }
 * "8F/38B01E18": {
 *   "tag": "commit",
 *   "flags": 0,
 *   "commitLsn": "0000008F/38B01DE8",
 *   "commitEndLsn": "0000008F/38B01E18",
 *   "commitTime": "BigInt(1726014579672237)"
 * },
 * "8F/38B01E18": {
 *   "tag": "begin",
 *   "commitLsn": "0000008F/38B01E98",
 *   "commitTime": "BigInt(1726014580746075)",
 *   "xid": 494599
 * },
 * "8F/38B01E18": {
 *   "tag": "insert",
 *   "relation": {
 *   ...
 * }
 * "8F/38B01EC8": {
 *   "tag": "commit",
 *   "flags": 0,
 *   "commitLsn": "0000008F/38B01E98",
 *   "commitEndLsn": "0000008F/38B01EC8",
 *   "commitTime": "BigInt(1726014580746075)"
 * },
 * ```
 *
 * This renders the LSN very difficult to use as a watermark. Even attaching
 * the position of the message within the transaction (with 'begin' starting at 0)
 * does not work since the 'commit' from the previous transaction would be sorted
 * after the 'begin' from the next one if they shared the same LSN.
 *
 * The workaround to convert an LSN to a monotonic value assumes that all
 * WAL records are more than 2 bytes in size (a safe assumption), and offsets
 * the LSN associated with a 'commit' by -2 and that of a 'begin' by -1.
 * This ensures that the resulting watermarks are strictly monotonic and
 * sorted in stream order.
 */
export function lsnOffset(change: Pick<Change, 'tag'>) {
  const {tag} = change;
  switch (tag) {
    case 'begin':
      return -1;
    case 'commit':
      return -2;
    default:
      return 0;
  }
}

function messageToChangeEntry(lsn: string, msg: Pgoutput.Message) {
  const change = msg as Change;
  switch (change.tag) {
    case 'begin':
    case 'insert':
    case 'update':
    case 'delete':
    case 'truncate':
    case 'commit': {
      const watermark = toLexiVersion(lsn, lsnOffset(change));
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
