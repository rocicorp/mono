import {LogContext} from '@rocicorp/logger';
import {StatementRunner} from 'zero-cache/src/db/statements.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Sink, Source} from 'zero-cache/src/types/streams.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {Database} from 'zqlite/src/db.js';
import {getSubscriptionState} from '../replicator/schema/replication-state.js';
import {RunningState} from '../running-state.js';
import {
  ChangeEntry,
  ChangeStreamerService,
  Downstream,
  ErrorType,
  SubscriberContext,
} from './change-streamer.js';
import {Forwarder} from './forwarder.js';
import {MessageCommit} from './schema/change.js';
import {initChangeStreamerSchema} from './schema/init.js';
import {ensureReplicationConfig, ReplicationConfig} from './schema/tables.js';
import {Storer} from './storer.js';
import {Subscriber} from './subscriber.js';

/**
 * Performs initialization and schema migrations to initialize a ChangeStreamerImpl.
 */
export async function initializeStreamer(
  lc: LogContext,
  changeDB: PostgresDB,
  changeSource: ChangeSource,
  replica: Database,
): Promise<ChangeStreamerService> {
  const replicationConfig = getSubscriptionState(new StatementRunner(replica));

  // Make sure the ChangeLog DB is set up.
  await initChangeStreamerSchema(lc, changeDB);
  await ensureReplicationConfig(lc, changeDB, replicationConfig);

  return new ChangeStreamerImpl(lc, changeDB, replicationConfig, changeSource);
}

export type ChangeStream = {
  changes: Source<ChangeEntry>;

  /**
   * A Sink to push the MessageCommit messages that have been successfully
   * stored by the {@link Storer}. The ACKs should contain the full MessageCommit
   * that was received from the `changes` Source, which may contain implementation
   * specific fields needed by the ChangeSource implementation.
   */
  acks: Sink<MessageCommit>;
};

/** Encapsulates an upstream-specific implementation of a stream of Changes. */
export interface ChangeSource {
  /**
   * Starts a stream of changes, with a corresponding sink for upstream
   * acknowledgements.
   */
  startStream(): ChangeStream;
}

/**
 * Upstream-agnostic dispatch of messages in a {@link ChangeStream} to a
 * {@link Forwarder} and {@link Storer} to execute the forward-store-ack
 * procedure described in {@link ChangeStreamer}.
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
class ChangeStreamerImpl implements ChangeStreamerService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #replicationConfig: ReplicationConfig;
  readonly #source: ChangeSource;
  readonly #storer: Storer;
  readonly #forwarder: Forwarder;

  readonly #state = new RunningState('ChangeStreamer');
  #stream: ChangeStream | undefined;

  constructor(
    lc: LogContext,
    changeDB: PostgresDB,
    replicationConfig: ReplicationConfig,
    source: ChangeSource,
  ) {
    this.id = `change-streamer`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#replicationConfig = replicationConfig;
    this.#source = source;
    this.#storer = new Storer(
      lc,
      changeDB,
      commit => this.#stream?.acks.push(commit),
    );
    this.#forwarder = new Forwarder();
  }

  async run() {
    void this.#storer.run();

    while (this.#state.shouldRun()) {
      const stream = this.#source.startStream();
      this.#stream = stream;

      try {
        for await (const changeEntry of stream.changes) {
          this.#state.resetBackoff();

          this.#storer.store(changeEntry);
          this.#forwarder.forward(changeEntry);
        }
      } catch (e) {
        this.#lc.error?.(`Error in Replication Stream.`, e);
      } finally {
        stream.changes.cancel();
        this.#stream = undefined;
      }

      await this.#state.backoff(this.#lc);
    }
    this.#lc.info?.('ChangeStreamer stopped');
  }

  subscribe(ctx: SubscriberContext): Source<Downstream> {
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
    this.#state.stop(this.#lc);
    this.#stream?.changes.cancel();
    await this.#storer.stop();
  }
}
