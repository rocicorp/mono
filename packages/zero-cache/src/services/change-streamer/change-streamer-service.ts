import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {unreachable} from '../../../../shared/src/asserts.ts';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import {
  min,
  type AtLeastOne,
  type LexiVersion,
} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {ShardID} from '../../types/shards.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../change-source/change-source.ts';
import {
  type ChangeStreamControl,
  type ChangeStreamData,
} from '../change-source/protocol/current/downstream.ts';
import {publishReplicationError} from '../replicator/replication-status.ts';
import type {SubscriptionState} from '../replicator/schema/replication-state.ts';
import {
  DEFAULT_MAX_RETRY_DELAY_MS,
  RunningState,
  UnrecoverableError,
} from '../running-state.ts';
import {
  type ChangeStreamerService,
  type Downstream,
  type SubscriberContext,
} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import {initChangeStreamerSchema} from './schema/init.ts';
import {
  AutoResetSignal,
  ensureReplicationConfig,
  markResetRequired,
} from './schema/tables.ts';
import {Storer} from './storer.ts';
import {Subscriber} from './subscriber.ts';

/**
 * Performs initialization and schema migrations to initialize a ChangeStreamerImpl.
 */
export async function initializeStreamer(
  lc: LogContext,
  shard: ShardID,
  taskID: string,
  discoveryAddress: string,
  discoveryProtocol: string,
  changeDB: PostgresDB,
  changeSource: ChangeSource,
  subscriptionState: SubscriptionState,
  autoReset: boolean,
  backPressureLimitHeapProportion: number,
  queueSizeBackPressureThresholdOrSetTimeoutFn?: number | typeof setTimeout,
  setTimeoutFn?: typeof setTimeout,
): Promise<ChangeStreamerService> {
  // Backward compatible signature:
  // - Old callers: initializeStreamer(..., backPressureLimitHeapProportion, setTimeoutFn?)
  // - New callers: initializeStreamer(..., backPressureLimitHeapProportion, queueSizeBackPressureThreshold, setTimeoutFn?)
  const queueSizeBackPressureThreshold =
    typeof queueSizeBackPressureThresholdOrSetTimeoutFn === 'number'
      ? queueSizeBackPressureThresholdOrSetTimeoutFn
      : undefined;
  const resolvedSetTimeoutFn =
    typeof queueSizeBackPressureThresholdOrSetTimeoutFn === 'function'
      ? queueSizeBackPressureThresholdOrSetTimeoutFn
      : (setTimeoutFn ?? setTimeout);

  // Make sure the ChangeLog DB is set up.
  await initChangeStreamerSchema(lc, changeDB, shard);
  await ensureReplicationConfig(
    lc,
    changeDB,
    subscriptionState,
    shard,
    autoReset,
  );

  const {replicaVersion} = subscriptionState;
  return new ChangeStreamerImpl(
    lc,
    shard,
    taskID,
    discoveryAddress,
    discoveryProtocol,
    changeDB,
    replicaVersion,
    changeSource,
    autoReset,
    backPressureLimitHeapProportion,
    queueSizeBackPressureThreshold,
    resolvedSetTimeoutFn,
  );
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
export type WatermarkedChange = [watermark: string, ChangeStreamData];

/**
 * Upstream-agnostic dispatch of messages in a {@link ChangeStreamMessage} to a
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
 *
 * ### Cleanup
 *
 * As mentioned in the {@link ChangeStreamer} documentation: "the ChangeStreamer
 * uses a combination of [the "initial", i.e. backup-derived watermark and] ACK
 * responses from connected subscribers to determine the watermark up
 * to which it is safe to purge old change log entries."
 *
 * More concretely:
 *
 * * The `initial`, backup-derived watermark is the earliest to which cleanup
 *   should ever happen.
 *
 * * However, it is possible for the replica backup to be *ahead* of a connected
 *   subscriber; and if a network error causes that subscriber to retry from its
 *   last watermark, the change streamer must support it.
 *
 * Thus, before cleaning up to an `initial` backup-derived watermark, the change
 * streamer first confirms that all connected subscribers have also passed
 * that watermark.
 */
class ChangeStreamerImpl implements ChangeStreamerService {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #changeDB: PostgresDB;
  readonly #replicaVersion: string;
  readonly #source: ChangeSource;
  readonly #storer: Storer;
  readonly #forwarder: Forwarder;

  readonly #autoReset: boolean;
  readonly #state: RunningState;
  readonly #initialWatermarks = new Set<string>();

  // Starting the (Postgres) ChangeStream results in killing the previous
  // Postgres subscriber, potentially creating a gap in which the old
  // change-streamer has shut down and the new change-streamer has not yet
  // been recognized as "healthy" (and thus does not get any requests).
  //
  // To minimize this gap, delay starting the ChangeStream until the first
  // request from a `serving` replicator, indicating that higher level
  // load-balancing / routing logic has begun routing requests to this task.
  readonly #serving = resolver();

  readonly #txCounter = getOrCreateCounter(
    'replication',
    'transactions',
    'Count of replicated transactions',
  );

  #stream: ChangeStream | undefined;

  constructor(
    lc: LogContext,
    shard: ShardID,
    taskID: string,
    discoveryAddress: string,
    discoveryProtocol: string,
    changeDB: PostgresDB,
    replicaVersion: string,
    source: ChangeSource,
    autoReset: boolean,
    backPressureLimitHeapProportion: number,
    queueSizeBackPressureThreshold: number | undefined,
    setTimeoutFn = setTimeout,
  ) {
    this.id = `change-streamer`;
    this.#lc = lc.withContext('component', 'change-streamer');
    this.#shard = shard;
    this.#changeDB = changeDB;
    this.#replicaVersion = replicaVersion;
    this.#source = source;
    this.#storer = new Storer(
      lc,
      shard,
      taskID,
      discoveryAddress,
      discoveryProtocol,
      changeDB,
      replicaVersion,
      consumed => this.#stream?.acks.push(['status', consumed[1], consumed[2]]),
      err => this.stop(err),
      backPressureLimitHeapProportion,
      queueSizeBackPressureThreshold,
    );
    this.#forwarder = new Forwarder();
    this.#autoReset = autoReset;
    this.#state = new RunningState(this.id, undefined, setTimeoutFn);
  }

  async run() {
    this.#lc.info?.('starting change stream');

    // Once this change-streamer acquires "ownership" of the change DB,
    // it is safe to start the storer.
    await this.#storer.assumeOwnership();
    // The storer will, in turn, detect changes to ownership and stop
    // the change-streamer appropriately.
    this.#storer
      .run()
      .then(() => this.stop())
      .catch(e => this.stop(e));

    while (this.#state.shouldRun()) {
      let err: unknown;
      let watermark: string | null = null;
      try {
        const {lastWatermark, backfillRequests} =
          await this.#storer.getStartStreamInitializationParameters();
        const stream = await this.#source.startStream(
          lastWatermark,
          backfillRequests,
        );
        this.#stream = stream;
        this.#state.resetBackoff();
        watermark = null;

        for await (const change of stream.changes) {
          const [type, msg] = change;
          switch (type) {
            case 'status':
              this.#storer.status(change); // storer acks once it gets through its queue
              continue;
            case 'control':
              await this.#handleControlMessage(msg);
              continue; // control messages are not stored/forwarded
            case 'begin':
              watermark = change[2].commitWatermark;
              break;
            case 'commit':
              if (watermark !== change[2].watermark) {
                throw new UnrecoverableError(
                  `commit watermark ${change[2].watermark} does not match 'begin' watermark ${watermark}`,
                );
              }
              this.#txCounter.add(1);
              break;
            default:
              if (watermark === null) {
                throw new UnrecoverableError(
                  `${type} change (${msg.tag}) received before 'begin' message`,
                );
              }
              break;
          }

          this.#storer.store([watermark, change]);
          this.#forwarder.forward([watermark, change]);

          if (type === 'commit' || type === 'rollback') {
            watermark = null;
          }

          // Allow the storer to exert back pressure.
          const readyForMore = this.#storer.readyForMore();
          if (readyForMore) {
            await readyForMore;
          }
        }
      } catch (e) {
        err = e;
      } finally {
        this.#stream?.changes.cancel();
        this.#stream = undefined;
      }

      // When the change stream is interrupted, abort any pending transaction.
      if (watermark) {
        this.#lc.warn?.(`aborting interrupted transaction ${watermark}`);
        this.#storer.abort();
        this.#forwarder.forward([watermark, ['rollback', {tag: 'rollback'}]]);
      }

      await this.#state.backoff(this.#lc, err);
    }
    this.#lc.info?.('ChangeStreamer stopped');
  }

  async #handleControlMessage(msg: ChangeStreamControl[1]) {
    this.#lc.info?.('received control message', msg);
    const {tag} = msg;

    switch (tag) {
      case 'reset-required':
        await markResetRequired(this.#changeDB, this.#shard);
        await publishReplicationError(
          this.#lc,
          'Replicating',
          msg.message ?? 'Resync required',
          msg.errorDetails,
        );
        if (this.#autoReset) {
          this.#lc.warn?.('shutting down for auto-reset');
          await this.stop(new AutoResetSignal());
        }
        break;
      default:
        unreachable(tag);
    }
  }

  subscribe(ctx: SubscriberContext): Promise<Source<Downstream>> {
    const {protocolVersion, id, mode, replicaVersion, watermark, initial} = ctx;
    if (mode === 'serving') {
      this.#serving.resolve();
    }
    const downstream = Subscription.create<Downstream>({
      cleanup: () => this.#forwarder.remove(subscriber),
    });
    const subscriber = new Subscriber(
      protocolVersion,
      id,
      watermark,
      downstream,
    );
    if (replicaVersion !== this.#replicaVersion) {
      this.#lc.warn?.(
        `rejecting subscriber at replica version ${replicaVersion}`,
      );
      subscriber.close(
        ErrorType.WrongReplicaVersion,
        `current replica version is ${
          this.#replicaVersion
        } (requested ${replicaVersion})`,
      );
    } else {
      this.#lc.debug?.(`adding subscriber ${subscriber.id}`);

      this.#forwarder.add(subscriber);
      this.#storer.catchup(subscriber, mode);

      if (initial) {
        this.scheduleCleanup(watermark);
      }
    }
    return Promise.resolve(downstream);
  }

  scheduleCleanup(watermark: string) {
    const origSize = this.#initialWatermarks.size;
    this.#initialWatermarks.add(watermark);

    if (origSize === 0) {
      this.#state.setTimeout(() => this.#purgeOldChanges(), CLEANUP_DELAY_MS);
    }
  }

  async getChangeLogState(): Promise<{
    replicaVersion: string;
    minWatermark: string;
  }> {
    const minWatermark = await this.#storer.getMinWatermarkForCatchup();
    return {
      replicaVersion: this.#replicaVersion,
      minWatermark: minWatermark ?? this.#replicaVersion,
    };
  }

  async #purgeOldChanges(): Promise<void> {
    const initial = [...this.#initialWatermarks];
    if (initial.length === 0) {
      this.#lc.warn?.('No initial watermarks to check for cleanup'); // Not expected.
      return;
    }
    const current = [...this.#forwarder.getAcks()];
    if (current.length === 0) {
      // Also not expected, but possible (e.g. subscriber connects, then disconnects).
      // Bail to be safe.
      this.#lc.warn?.('No subscribers to confirm cleanup');
      return;
    }
    try {
      const earliestInitial = min(...(initial as AtLeastOne<LexiVersion>));
      const earliestCurrent = min(...(current as AtLeastOne<LexiVersion>));
      if (earliestCurrent < earliestInitial) {
        this.#lc.info?.(
          `At least one client is behind backup (${earliestCurrent} < ${earliestInitial})`,
        );
      } else {
        const deleted = await this.#storer.purgeRecordsBefore(earliestInitial);
        this.#lc.info?.(`Purged ${deleted} changes before ${earliestInitial}`);
        this.#initialWatermarks.delete(earliestInitial);
      }
    } finally {
      if (this.#initialWatermarks.size) {
        // If there are unpurged watermarks to check, schedule the next purge.
        this.#state.setTimeout(() => this.#purgeOldChanges(), CLEANUP_DELAY_MS);
      }
    }
  }

  async stop(err?: unknown) {
    this.#state.stop(this.#lc, err);
    this.#stream?.changes.cancel();
    await this.#storer.stop();
  }
}

// The delay between receiving an initial, backup-based watermark
// and performing a check of whether to purge records before it.
// This delay should be long enough to handle situations like the following:
//
// 1. `litestream restore` downloads a backup for the `replication-manager`
// 2. `replication-manager` starts up and runs this `change-streamer`
// 3. `zero-cache`s that are running on a different replica connect to this
//    `change-streamer` after exponential backoff retries.
//
// It is possible for a `zero-cache`[3] to be behind the backup restored [1].
// This cleanup delay (30 seconds) is thus set to be a value comfortably
// longer than the max delay for exponential backoff (10 seconds) in
// `services/running-state.ts`. This allows the `zero-cache` [3] to reconnect
// so that the `change-streamer` can track its progress and know when it has
// surpassed the initial watermark of the backup [1].
const CLEANUP_DELAY_MS = DEFAULT_MAX_RETRY_DELAY_MS * 3;
