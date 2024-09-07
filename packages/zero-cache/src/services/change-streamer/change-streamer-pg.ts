import {LogContext} from '@rocicorp/logger';
import {
  LogicalReplicationService,
  Pgoutput,
  PgoutputPlugin,
} from 'pg-logical-replication';
import {sleep} from 'shared/src/sleep.js';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {stringify} from 'zero-cache/src/types/bigint-json.js';
import {toLexiVersion} from 'zero-cache/src/types/lsn.js';
import {
  PostgresDB,
  registerPostgresTypeParsers,
} from 'zero-cache/src/types/pg.js';
import {CancelableAsyncIterable} from 'zero-cache/src/types/streams.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {replicationSlot} from '../replicator/initial-sync.js';
import {getSubscriptionState} from '../replicator/schema/replication-state.js';
import {
  ChangeStreamerService,
  Downstream,
  ErrorType,
  SubscriberContext,
} from './change-streamer.js';
import {Forwarder} from './forwarder.js';
import {Change} from './schema/change.js';
import {initChangeStreamerSchema} from './schema/init.js';
import {ensureReplicationConfig, ReplicationConfig} from './schema/tables.js';
import {Storer} from './storer.js';
import {Subscriber} from './subscriber.js';

// BigInt support from LogicalReplicationService.
registerPostgresTypeParsers();

const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 10000;

/**
 * Performs initialization and schema migrations and creates a
 * PostgresChangeStreamer instance.
 */
export async function initializeStreamer(
  lc: LogContext,
  changeDB: PostgresDB,
  upstreamUri: string,
  replicaID: string,
  replica: Database,
): Promise<ChangeStreamerService> {
  const {watermark: _, ...replicationConfig} = getSubscriptionState(
    new StatementRunner(replica),
  );

  // Make sure the ChangeLog DB is set up.
  await initChangeStreamerSchema(lc, changeDB);
  await ensureReplicationConfig(lc, changeDB, replicationConfig);

  return new PostgresChangeStreamer(
    lc,
    changeDB,
    upstreamUri,
    replicaID,
    replicationConfig,
  );
}

/**
 * The PostgresChangeStreamer implementation connects to a logical
 * replication slot on the upstream Postgres and dispatches messages
 * in the replication stream to a {@link Forwarder} and {@link Storer}
 * to execute the forward-store-ack procedure described in
 * {@link ChangeStreamer}.
 *
 * Connecting clients first need to be "caught up" to the current watermark
 * (from stored change log entries) before new entries are forwarded to
 * them. This is non-trivial because the replication stream may be in the
 * middle of a pending streamed Transaction for which some entries have
 * already been forwarded but are not yet committed to the store.
 *
 *
 * ```
 * ------------------------------- - - - - - - - - - - - - - - - - - - -
 * | Historic changes in storage |  Pending (streamed) tx  |   Next tx
 * ------------------------------- - - - - - - - - - - - - - - - - - - -
 *                                           Replication stream
 *                                           >  >  >  >  >  >  >  >  >
 *           ^  ---> required catchup --->   ^
 * Subscriber watermark               Subscription begins
 * ```
 *
 * Preemptively buffering the changes of every pending transaction
 * would be wasteful and consume too much memory for large transactions.
 *
 * Instead, the streamer synchronously dispatches changes and subscriptions
 * to the {@link Forwarder} and the {@link Storer} such that the two
 * components are aligned as to where in the stream the subscription started.
 * The two components then coordinate catchup and handoff via the
 * {@link Subscriber} object with the following algorithm:
 *
 * * If the streamer is in the middle of a pending Transaction, the
 *   Subscriber is "queued" on both the Forwarder and the Storer. In this
 *   state, new changes are *not* forwarded to the Subscriber, and catchup
 *   is not yet executed.
 * * Once the commit message for the pending Transaction is processed
 *   by the Storer, it begins catchup on the Subscriber (with a READONLY
 *   snapshot so that it does not block subsequent storage operations).
 *   This catchup is thus guaranteed to load the change log entries of
 *   that last Transaction.
 * * When the Forwarder processes that same commit message, it moves the
 *   Subscriber from the "queued" to the "active" set of clients such that
 *   the Subscriber begins receiving new changes, starting from the next
 *   Transaction.
 * * The Subscriber does not forward those changes, however, if its catchup
 *   is not complete. Until then, it buffers the changes in memory.
 * * Once catchup is complete, the buffered changes are immediately sent
 *   and the Subscriber henceforth forwards changes as they are received.
 *
 * In the (common) case where the streamer is not in the middle of a pending
 * transaction when a subscription begins, the Storer begins catchup
 * immediately and the Forwarder directly adds the Subscriber to its active
 * set. However, the Subscriber still buffers any forwarded messages until
 * its catchup is complete.
 */
class PostgresChangeStreamer implements ChangeStreamerService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #replicaID: string;
  readonly #replicationConfig: ReplicationConfig;
  readonly #storer: Storer;
  readonly #forwarder: Forwarder;

  #service: LogicalReplicationService | undefined;
  #lastLSN = '0/0';
  #stopped = false;

  constructor(
    lc: LogContext,
    changeDB: PostgresDB,
    upstreamUri: string,
    replicaID: string,
    replicationConfig: ReplicationConfig,
  ) {
    this.id = `change-streamer:${replicaID}`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replicationConfig = replicationConfig;
    this.#storer = new Storer(lc, changeDB, this.#ack);
    this.#forwarder = new Forwarder();
  }

  async run() {
    void this.#storer.run();

    let retryDelay = INITIAL_RETRY_DELAY_MS;

    while (!this.#stopped) {
      this.#service = new LogicalReplicationService(
        {connectionString: this.#upstreamUri},
        {acknowledge: {auto: false, timeoutSeconds: 0}},
      )
        .on('heartbeat', this.#handleHeartbeat)
        .on('data', this.#processMessage)
        .on('data', () => {
          retryDelay = INITIAL_RETRY_DELAY_MS; // Reset exponential backoff.
        });

      try {
        this.#lc.debug?.('starting upstream replication stream');
        await this.#service.subscribe(
          new PgoutputPlugin({
            protoVersion: 1,
            publicationNames: this.#replicationConfig.publications,
          }),
          replicationSlot(this.#replicaID),
          this.#lastLSN,
        );
      } catch (e) {
        if (!this.#stopped) {
          await this.#service.stop();
          this.#service = undefined;

          const delay = retryDelay;
          retryDelay = Math.min(delay * 2, MAX_RETRY_DELAY_MS);
          this.#lc.error?.(
            `Error in Replication Stream. Retrying in ${delay}ms`,
            e,
          );
          await sleep(delay);
        }
      }
    }
    this.#lc.info?.('ChangeStreamer stopped');
  }

  readonly #processMessage = (lsn: string, msg: Pgoutput.Message) => {
    const change = msg as Change;
    switch (change.tag) {
      case 'begin':
      case 'insert':
      case 'update':
      case 'delete':
      case 'truncate':
      case 'commit': {
        const watermark = toLexiVersion(lsn);
        const changeEntry = {watermark, change};
        this.#storer.store(changeEntry);
        this.#forwarder.forward(changeEntry);
        return;
      }

      default:
        change satisfies never; // All change types are covered.
        break;
    }

    switch (msg.tag) {
      case 'relation':
        break; // Explicitly ignored. Schema handling is TODO.
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
  };

  readonly #handleHeartbeat = (
    _lsn: string,
    _time: number,
    respond: boolean,
  ) => {
    if (respond) {
      void this.#ack();
    }
  };

  readonly #ack = (commit?: Pgoutput.MessageCommit) => {
    this.#lastLSN = commit?.commitEndLsn ?? this.#lastLSN;
    return this.#service?.acknowledge(this.#lastLSN);
  };

  subscribe(ctx: SubscriberContext): CancelableAsyncIterable<Downstream> {
    const {id, watermark} = ctx;
    const downstream = Subscription.create<Downstream>({
      cleanup: () => this.#forwarder.remove(id, subscriber),
    });
    const subscriber = new Subscriber(id, watermark, downstream);
    if (ctx.replicaVersion !== this.#replicationConfig.replicaVersion) {
      subscriber.close(ErrorType.WrongReplicaVersion);
    } else {
      this.#lc.debug?.(`adding subscriber ${subscriber.id}`);
      this.#forwarder.add(subscriber);
      this.#storer.catchup(subscriber);
    }
    return downstream;
  }

  async stop() {
    this.#lc.info?.('Stopping ChangeStreamer');
    this.#stopped = true;
    await this.#service?.stop();
    await this.#storer.stop();
  }
}
