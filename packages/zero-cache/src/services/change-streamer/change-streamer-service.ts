import {LogContext} from '@rocicorp/logger';
import {
  LexiVersion,
  versionFromLexi,
  versionToLexi,
} from 'zero-cache/src/types/lexi-version.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Sink, Source} from 'zero-cache/src/types/streams.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {RunningState} from '../running-state.js';
import {
  ChangeStreamerService,
  Commit,
  Downstream,
  DownstreamChange,
  ErrorType,
  SubscriberContext,
} from './change-streamer.js';
import {Forwarder} from './forwarder.js';
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
  replicationConfig: ReplicationConfig,
): Promise<ChangeStreamerService> {
  // Make sure the ChangeLog DB is set up.
  await initChangeStreamerSchema(lc, changeDB);
  await ensureReplicationConfig(lc, changeDB, replicationConfig);

  const {replicaVersion} = replicationConfig;
  return new ChangeStreamerImpl(lc, changeDB, replicaVersion, changeSource);
}

/**
 * Internally all Downstream messages (not just commits) are given a watermark.
 * These are used for internal ordering for:
 * 1. Replaying new changes in the Storer
 * 2. Filtering old changes in the Subscriber
 *
 * However, only the watermark for `Commit` messages are exposed to
 * subscribers, as that is the only semantically correct watermark to
 * use for tracking a position in a replication stream.
 */
export type WatermarkedChange = [watermark: string, DownstreamChange];

export type ChangeStream = {
  /**
   * The watermark after which the ChangeStream begins (i.e. exclusive).
   */
  initialWatermark: string;

  changes: Source<DownstreamChange>;

  /**
   * A Sink to push the {@link Commit} messages that have been successfully
   * stored by the {@link Storer}.
   */
  acks: Sink<Commit>;
};

/** Encapsulates an upstream-specific implementation of a stream of Changes. */
export interface ChangeSource {
  /**
   * Starts a stream of changes, with a corresponding sink for upstream
   * acknowledgements.
   */
  startStream(): Promise<ChangeStream>;
}

/**
 * Upstream-agnostic dispatch of messages in a {@link ChangeStream} to a
 * {@link Forwarder} and {@link Storer} to execute the forward-store-ack
 * procedure described in {@link ChangeStreamer}.
 *
 * ### Subscriber Catchup
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
 *
 * ### Watermarks and ordering
 *
 * The ChangeStreamerService depends on its {@link ChangeSource} to send
 * changes in contiguous [`begin`, `data` ..., `data`, `commit`] sequences
 * in commit order. This follows Postgres's Logical Replication Protocol
 * Message Flow:
 *
 * https://www.postgresql.org/docs/16/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES-FLOW
 *
 * > The logical replication protocol sends individual transactions one by one.
 * > This means that all messages between a pair of Begin and Commit messages belong to the same transaction.
 *
 * In order to correctly replay (new) and filter (old) messages to subscribers
 * at different points in the replication stream, these changes must be assigned
 * watermarks such that they preserve the order in which they were received
 * from the ChangeSource.
 *
 * A previous implementation incorrectly derived these watermarks from the Postgres
 * Log Sequence Numbers (LSN) of each message. However, LSNs from concurrent,
 * non-conflicting transactions can overlap, which can result in a `begin` message
 * with an earlier LSN arriving after a `commit` message. For example, the
 * changes for these transactions:
 *
 * ```
 * LSN:   1     2     3  4    5   6   7     8   9      10
 * tx1: begin  data     data     data     commit
 * tx2:             begin    data    data      data  commit
 * ```
 *
 * will arrive as:
 *
 * ```
 * begin1, data2, data4, data6, commit8, begin3, data5, data7, data9, commit10
 * ```
 *
 * Thus, LSN of non-commit messages are not suitable for tracking the sorting
 * order of the replication stream.
 *
 * Instead, the ChangeStreamer uses the following algorithm for deterministic
 * catchup and filtering of changes:
 *
 * * A `commit` message is assigned to a watermark corresponding to its LSN.
 *   These are guaranteed to be in commit order by definition.
 *
 * * `begin` and `data` messages are assigned to the watermark of the
 *   preceding `commit` (the previous transaction, or the replication
 *   slot's starting LSN) plus 1. This guarantees that they will be sorted
 *   after the previously commit transaction even if their LSNs came before it.
 *   This is referred to as the `preCommitWatermark`.
 *
 * * In the ChangeLog DB, messages have a secondary sort column `pos`, which is
 *   the position of the message within its transaction, with the `begin` message
 *   starting at `0`. This guarantees that `begin` and `data` messages will be
 *   fetched in the original ChangeSource order during catchup.
 *
 * `begin` and `data` messages share the same watermark, but this is sufficient for
 * Subscriber filtering because subscribers only know about the `commit` watermarks
 * exposed in the `Downstream` `Commit` message. The Subscriber object thus compares
 * the internal watermarks of the incoming messages against the commit watermark of
 * the caller, updating the watermark at every `Commit` message that is forwarded.
 */
class ChangeStreamerImpl implements ChangeStreamerService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #replicaVersion: string;
  readonly #source: ChangeSource;
  readonly #storer: Storer;
  readonly #forwarder: Forwarder;

  readonly #state = new RunningState('ChangeStreamer');
  #stream: ChangeStream | undefined;

  constructor(
    lc: LogContext,
    changeDB: PostgresDB,
    replicaVersion: string,
    source: ChangeSource,
  ) {
    this.id = `change-streamer`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#replicaVersion = replicaVersion;
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
      let err: unknown;
      try {
        const stream = await this.#source.startStream();
        this.#stream = stream;
        let preCommitWatermark = oneAfter(stream.initialWatermark);

        for await (const change of stream.changes) {
          this.#state.resetBackoff();

          let watermark: string;
          if (change[0] !== 'commit') {
            watermark = preCommitWatermark;
          } else {
            watermark = change[2].watermark;
            preCommitWatermark = oneAfter(watermark); // For the next transaction.
          }

          this.#storer.store([watermark, change]);
          this.#forwarder.forward([watermark, change]);
        }
      } catch (e) {
        err = e;
      } finally {
        this.#stream?.changes.cancel();
        this.#stream = undefined;
      }

      await this.#state.backoff(this.#lc, err);
    }
    this.#lc.info?.('ChangeStreamer stopped');
  }

  subscribe(ctx: SubscriberContext): Source<Downstream> {
    const {id, watermark} = ctx;
    const downstream = Subscription.create<Downstream>({
      cleanup: () => this.#forwarder.remove(id, subscriber),
    });
    const subscriber = new Subscriber(id, watermark, downstream);
    if (ctx.replicaVersion !== this.#replicaVersion) {
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

function oneAfter(watermark: LexiVersion) {
  return versionToLexi(versionFromLexi(watermark) + 1n);
}
