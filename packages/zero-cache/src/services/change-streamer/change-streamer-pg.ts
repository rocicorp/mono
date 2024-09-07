import {LogContext} from '@rocicorp/logger';
import {Database} from 'better-sqlite3';
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
import {replicationSlot} from '../replicator/initial-sync.js';
import {getSubscriptionState} from '../replicator/schema/replication-state.js';
import {Service} from '../service.js';
import {Archiver} from './archiver.js';
import {
  ChangeStreamer,
  Downstream,
  ErrorType,
  SubscriberContext,
} from './change-streamer.js';
import {Forwarder} from './forwarder.js';
import {Change} from './schema/change.js';
import {initChangeStreamerSchema} from './schema/init.js';
import {ensureReplicationConfig, ReplicationConfig} from './schema/tables.js';
import {Subscriber} from './subscriber.js';

// BigInt support from LogicalReplicationService.
registerPostgresTypeParsers();

const INITIAL_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 10000;

export class PostgresChangeStreamer implements ChangeStreamer, Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #upstreamUri: string;
  readonly #replicaID: string;
  readonly #replica: StatementRunner;
  readonly #changeDB: PostgresDB;
  readonly #archiver: Archiver;
  readonly #forwarder: Forwarder;

  #stopped = false;
  #service: LogicalReplicationService | undefined;
  #replicationConfig: ReplicationConfig | undefined;
  #lastLSN = '0/0';

  constructor(
    lc: LogContext,
    changeDB: PostgresDB,
    upstreamUri: string,
    replicaID: string,
    replica: Database,
  ) {
    this.id = `change-streamer:${replicaID}`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#changeDB = changeDB;
    this.#upstreamUri = upstreamUri;
    this.#replicaID = replicaID;
    this.#replica = new StatementRunner(replica);
    this.#archiver = new Archiver(lc, changeDB, this.#ack);
    this.#forwarder = new Forwarder();
  }

  async run() {
    // Set the #replicationConfig as soon as run() is called so that
    // subscribe() requests can succeed immediately.
    const {watermark: _, ...replicationConfig} = getSubscriptionState(
      this.#replica,
    );
    this.#replicationConfig = replicationConfig;

    // Make sure the ChangeLog DB is setup.
    await initChangeStreamerSchema(this.#lc, this.#changeDB);
    await ensureReplicationConfig(
      this.#lc,
      this.#changeDB,
      this.#replicationConfig,
    );
    void this.#archiver.run();

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
    this.#lc.debug?.(`processing message ${stringify(msg)}`);

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
        this.#archiver.archive(changeEntry);
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
    if (ctx.replicaVersion !== this.#replicationConfig?.replicaVersion) {
      subscriber.close(ErrorType.WrongReplicaVersion);
    } else {
      this.#lc.debug?.(`adding subscriber ${subscriber.id}`);
      this.#forwarder.add(subscriber);
      this.#archiver.catchup(subscriber);
    }
    return downstream;
  }

  async stop() {
    this.#lc.info?.('Stopping ChangeStreamer');
    this.#stopped = true;
    await this.#service?.stop();
    await this.#archiver.stop();
  }
}
