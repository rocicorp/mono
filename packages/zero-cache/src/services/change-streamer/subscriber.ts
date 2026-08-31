import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {must} from '../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {RingBuffer} from '../../../../shared/src/ring-buffer.ts';
import {max} from '../../types/lexi-version.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicatorMode} from '../replicator/replicator.ts';
import type {
  ChangeTag,
  Downstream,
  Status,
  WatermarkedChange,
} from './change-streamer.ts';
import type * as ErrorType from './error-type-enum.ts';

type ErrorType = Enum<typeof ErrorType>;

const DEFAULT_BACKLOG_HIGH_WATER_BYTES = 16 * 1024 * 1024;
const DEFAULT_BACKLOG_LOW_WATER_RATIO = 0.8;

export type SubscriberOptions = {
  backlogHighWaterBytes?: number | undefined;
  backlogLowWaterRatio?: number | undefined;

  /**
   * Called whenever the subscriber's acked watermark advances, i.e. when it
   * has confirmed that a commit was durably applied to its replica. The ACK
   * therefore lags the subscriber's replica rather than leading it.
   */
  onAck?: ((watermark: string) => void) | undefined;
};

export type BacklogFullWait = {
  readonly promise: Promise<void>;
  cancel(): void;
};

export type SubscriberStats = {
  processRate: number;
  pending: number;
  backlog: number;
  backlogBytes: number;
  totalBufferedBytes: number;
  missedLastTimeout: boolean;
};

/**
 * Encapsulates a subscriber to changes. All subscribers start in a
 * "catchup" phase in which changes are buffered in a backlog while the
 * storer is queried to send any changes that were committed since the
 * subscriber's watermark. Once the catchup is complete, calls to
 * {@link send()} result in immediately sending the change.
 */
export class Subscriber {
  readonly #protocolVersion: number;
  readonly id: string;
  readonly mode: ReplicatorMode;
  readonly #downstream: Subscription<string>;
  readonly #latestStatus: () => Status;
  #watermark: string;
  #acked: string;
  #backlog: RingBuffer<WatermarkedChange> | null;
  // While catchup is running, live changes are buffered here instead of being
  // pushed downstream. RingBuffer lets drainBacklog consume that backlog without
  // shifting an array, which matters when a subscriber is far behind.
  #backlogBytes = 0;
  #backlogInFlightBytes = 0;
  #backlogDrain: Promise<void> | null = null;
  readonly #backlogBackpressure: ByteBackpressureGate;
  readonly #backlogFullWaiters = new Set<Resolver<void>>();
  readonly #onAck: ((watermark: string) => void) | undefined;

  constructor(
    protocolVersion: number,
    id: string,
    mode: ReplicatorMode,
    watermark: string,
    downstream: Subscription<string>,
    latestStatus: () => Status,
    options: SubscriberOptions = {},
  ) {
    this.#protocolVersion = protocolVersion;
    this.id = id;
    this.mode = mode;
    this.#downstream = downstream;
    this.#latestStatus = latestStatus;
    this.#watermark = watermark;
    this.#acked = watermark;
    this.#backlog = new RingBuffer();
    this.#backlogBackpressure = new ByteBackpressureGate(
      options.backlogHighWaterBytes ?? DEFAULT_BACKLOG_HIGH_WATER_BYTES,
      options.backlogLowWaterRatio ?? DEFAULT_BACKLOG_LOW_WATER_RATIO,
    );
    this.#onAck = options.onAck;
  }

  get watermark() {
    return this.#watermark;
  }

  get acked() {
    return this.#acked;
  }

  /**
   * Whether the backlog of live changes buffered during catchup has reached the
   * point at which {@link send()} stops resolving. Past it the subscriber is no
   * longer free: it holds up every subsequent flush, and with no other
   * subscriber to form a majority it stalls replication outright.
   */
  get backlogFull() {
    return (
      this.#bufferedBacklogBytes >= this.#backlogBackpressure.highWaterBytes
    );
  }

  /**
   * @returns whether the subscriber is currently sending backlogged messages,
   *          vs caught up and sending the "head" of the replication stream.
   */
  isBacklogged() {
    return this.#backlog !== null || this.#bufferedBacklogBytes > 0;
  }

  /**
   * Resolves the first time {@link backlogFull} becomes true, so that a caller
   * holding the subscriber in catchup can give up at the moment the subscriber
   * starts costing replication rather than on a timer. Call cancel() when the
   * wait is no longer needed so the subscriber does not retain it.
   */
  whenBacklogFull(): BacklogFullWait {
    if (this.backlogFull) {
      return {promise: promiseVoid, cancel() {}};
    }
    const r = resolver<void>();
    this.#backlogFullWaiters.add(r);
    return {
      promise: r.promise,
      cancel: () => {
        this.#backlogFullWaiters.delete(r);
      },
    };
  }

  send(change: WatermarkedChange): Promise<void> {
    const [watermark] = change;
    if (watermark > this.#watermark) {
      if (this.#backlog) {
        // During catchup, buffer live changes behind the durable catchup stream.
        // The returned promise applies backpressure if the buffered bytes cross
        // the high water mark.
        this.#pushBacklog(change);
        return this.#maybeWaitForBacklogSpace();
      }
      return this.#sendChange(change);
    }
    return promiseVoid;
  }

  #initialized = false;

  /**
   * Called once the subscriber's watermark has been validated in the initial
   * catchup process.
   */
  #initialize() {
    if (!this.#initialized) {
      this.#initialized = true;
      this.sendStatus(this.#latestStatus());
    }
  }

  sendStatus(status: Status) {
    if (this.#initialized) {
      void this.#sendDownstream(['status', status]);
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  async catchup(change: WatermarkedChange) {
    this.#initialize();
    await this.#sendChange(change);
  }

  /**
   * Marks the Subscribe as "caught up" and flushes any backlog of
   * entries that were received during the catchup.
   */
  setCaughtUp(): Promise<void> {
    this.#initialize();
    if (!this.#backlog) {
      return this.#backlogDrain ?? promiseVoid;
    }
    if (!this.#backlogDrain) {
      // Keep #backlog non-null while queued entries are being handed to
      // downstream. That preserves ordering for sends that race with
      // setCaughtUp(): they append to the same backlog instead of bypassing
      // older buffered changes.
      this.#backlogDrain = this.#drainBacklog();
      void this.#backlogDrain.catch(e => this.fail(e));
    }
    return this.#backlogDrain;
  }

  async #sendChange(change: WatermarkedChange) {
    const [watermark, tag, json] = change;
    if (watermark <= this.watermark) {
      return;
    }
    if (!this.supportsMessage(tag)) {
      return;
    }
    if (tag === 'commit') {
      this.#watermark = watermark;
    }
    const result = await this.#sendStringifiedDownstream(json);
    if (tag === 'commit' && result === 'consumed') {
      // Sends can complete out of order (e.g. the bounded window in
      // #drainBacklog), so the ack only advances monotonically, and listeners
      // are only notified when it does.
      const acked = max(this.#acked, watermark);
      if (acked !== this.#acked) {
        this.#acked = acked;
        this.#onAck?.(acked);
      }
    }
  }

  #sendDownstream(downstream: Downstream) {
    return this.#sendStringifiedDownstream(BigIntJSON.stringify(downstream));
  }

  async #sendStringifiedDownstream(json: string) {
    const size = json.length;
    this.#pending++;
    this.#pendingBytes += size;
    const {result} = this.#downstream.push(json);
    try {
      return await result;
    } finally {
      this.#pending--;
      this.#pendingBytes -= size;
      this.#processed++;
    }
  }

  // `pending` and `processed` stats are tracked by periodically sampling
  // the running totals (by the progress tracker in the Forwarder).
  // This information was originally collected for use in flow control
  // decisions. The final flow control algorithm ended up being simpler
  // than expected and does not actually use this information. However, the
  // stats are still tracked and logged during flow control decisions for
  // debugging, forensics, and potential improvements to the algorithm.

  #pending = 0;
  #pendingBytes = 0;
  #processed = 0;
  #samples: {processed: number; timestamp: number}[] = [
    {processed: 0, timestamp: performance.now()},
  ];

  /**
   * The number of downstream messages that have yet to be acked.
   */
  get numPending() {
    return this.#pending + this.#backlogCount;
  }

  /**
   * The total number of downstream messages that the subscriber has
   * processed (i.e. acked).
   */
  get numProcessed() {
    return this.#processed;
  }

  /**
   * Records a new history entry for the number of messages processed,
   * keeping the number of samples bounded to `maxSamples`.
   */
  sampleProcessRate(now: number, maxSamples = 10): this {
    while (this.#samples.length >= maxSamples) {
      this.#samples.shift();
    }
    this.#samples.push({processed: this.#processed, timestamp: now});
    return this;
  }

  getStats(): SubscriberStats {
    const pending = this.numPending;
    if (this.#samples.length < 2) {
      return {
        processRate: 0,
        pending,
        backlog: this.#backlogCount,
        backlogBytes: this.#bufferedBacklogBytes,
        totalBufferedBytes: this.#totalBufferedBytes,
        missedLastTimeout: this.#missedLastTimeout,
      };
    }
    const from = this.#samples[0];
    const to = must(this.#samples.at(-1));
    const processed = to.processed - from.processed;
    const seconds = (to.timestamp - from.timestamp) / 1000;
    const processRate = seconds === 0 ? 0 : processed / seconds;
    return {
      processRate,
      pending,
      backlog: this.#backlogCount,
      backlogBytes: this.#bufferedBacklogBytes,
      totalBufferedBytes: this.#totalBufferedBytes,
      missedLastTimeout: this.#missedLastTimeout,
    };
  }

  #missedLastTimeout = false;
  #laggingSinceMs: number | undefined;

  trackResponseResult(result: 'on-time' | 'timed-out') {
    this.#missedLastTimeout = result === 'timed-out';
    if (result === 'on-time') {
      this.#laggingSinceMs = undefined;
    }
  }

  /**
   * Reports the change rate of a slow subscriber (i.e. that missed the last
   * timeout) compared to the change rate of the (slowest) subscriber that
   * responded on time.
   *
   * * `lagging` indicates that this subscriber is slower
   * * `catching-up` indicates that it is faster
   *
   * Returns the total duration in which subscriber has been continuously
   * reported as `lagging`.
   */
  reportChangeRate(now: number, status: 'lagging' | 'catching-up') {
    assert(
      this.#missedLastTimeout,
      `reportChangeRate should only be called for slow subscribers`,
    );
    if (status === 'catching-up') {
      this.#laggingSinceMs = undefined;
      return 0;
    }
    this.#laggingSinceMs ??= now;
    return now - this.#laggingSinceMs;
  }

  supportsMessage(tag: ChangeTag) {
    switch (tag) {
      case 'update-table-metadata':
        // update-table-row-key is only understood by subscribers >= protocol v5
        return this.#protocolVersion >= 5;
    }
    return true;
  }

  /**
   * Ends the subscription without sending a downstream `['error', ...]`.
   *
   * This is deliberate, and not the same as reporting the failure to the
   * subscriber: `IncrementalSyncer` treats any `['error', ...]` as terminal and
   * shuts down to restore a fresh replica from litestream, whereas a clean end
   * backs off and re-subscribes. The failures routed here -- storer catchup
   * errors, backlog drain errors, backup-monitor errors -- are transient, so a
   * reconnect is the proportionate response and a fleet-wide restore is not.
   *
   * Callers are responsible for logging `err`; it is not carried downstream.
   */
  fail(_err?: unknown) {
    this.close();
  }

  close(error?: ErrorType, message?: string) {
    // Closing the subscriber must also release producers that are blocked on
    // backlog capacity; there is no future drain that could wake them.
    this.#backlog = null;
    this.#backlogBytes = 0;
    this.#backlogBackpressure.releaseAll();
    // The backlog can no longer grow, so nothing would ever resolve these.
    // Waiters re-check backlogFull, which is now false, and see the close.
    this.#resolveBacklogFullWaiters();

    if (error !== undefined) {
      // Wait for the ACK of the error message before closing the connection.
      void this.#sendDownstream(['error', {type: error, message}]).finally(() =>
        this.#downstream.cancel(),
      );
    } else {
      this.#downstream.cancel();
    }
  }

  get #backlogCount() {
    return this.#backlog?.size ?? 0;
  }

  get #bufferedBacklogBytes() {
    // Include entries already handed to downstream but not yet consumed. Without
    // this, setCaughtUp() could move bytes out of #backlog faster than the
    // downstream Subscription can process them and release producers too early.
    return this.#backlogBytes + this.#backlogInFlightBytes;
  }

  get #totalBufferedBytes() {
    return this.#bufferedBacklogBytes + this.#pendingBytes;
  }

  #pushBacklog(change: WatermarkedChange) {
    assert(this.#backlog, 'cannot push to backlog after catchup completed');
    this.#backlog.push(change);
    this.#backlogBytes += change[2].length;
    if (this.backlogFull) {
      this.#resolveBacklogFullWaiters();
    }
  }

  #resolveBacklogFullWaiters() {
    const waiters = [...this.#backlogFullWaiters];
    this.#backlogFullWaiters.clear();
    for (const waiter of waiters) {
      waiter.resolve();
    }
  }

  #maybeWaitForBacklogSpace(): Promise<void> {
    return this.#backlogBackpressure.waitForSpace(this.#bufferedBacklogBytes);
  }

  async #drainBacklog() {
    const inFlight: {promise: Promise<void>; bytes: number}[] = [];
    let inFlightBytes = 0;

    try {
      for (;;) {
        const change = this.#backlog?.shift();
        if (!change) {
          this.#backlog = null;
          this.#backlogBytes = 0;
          this.#backlogBackpressure.releaseIfUnderLowWater(
            this.#bufferedBacklogBytes,
          );
          break;
        }

        const bytes = change[2].length;
        this.#backlogBytes -= bytes;
        this.#backlogInFlightBytes += bytes;
        this.#backlogBackpressure.releaseIfUnderLowWater(
          this.#bufferedBacklogBytes,
        );

        // Send backlog entries in order, but keep only a bounded byte window in
        // flight. This avoids replacing one unbounded buffer with another inside
        // the downstream Subscription during catchup completion.
        const promise = this.#sendChange(change).finally(() => {
          this.#backlogInFlightBytes -= bytes;
          this.#backlogBackpressure.releaseIfUnderLowWater(
            this.#bufferedBacklogBytes,
          );
        });
        inFlight.push({promise, bytes});
        inFlightBytes += bytes;

        while (inFlightBytes >= this.#backlogBackpressure.highWaterBytes) {
          const next = must(inFlight.shift());
          await next.promise;
          inFlightBytes -= next.bytes;
        }
      }

      for (const {promise} of inFlight) {
        await promise;
      }
    } finally {
      this.#backlogDrain = null;
      this.#backlogBackpressure.releaseIfUnderLowWater(
        this.#bufferedBacklogBytes,
      );
    }
  }
}

class ByteBackpressureGate {
  readonly highWaterBytes: number;
  readonly #lowWaterBytes: number;
  readonly #waiters: Resolver<void>[] = [];

  constructor(highWaterBytes: number, lowWaterRatio: number) {
    this.highWaterBytes = Math.max(1, highWaterBytes);
    this.#lowWaterBytes =
      this.highWaterBytes * Math.min(1, Math.max(0, lowWaterRatio));
  }

  waitForSpace(bufferedBytes: number): Promise<void> {
    if (bufferedBytes < this.highWaterBytes) {
      return promiseVoid;
    }

    // One waiter represents one send() call that has already appended its
    // change. The producer is released when the backlog falls back below the low
    // water mark or the subscriber closes.
    const r = resolver<void>();
    this.#waiters.push(r);
    return r.promise;
  }

  releaseIfUnderLowWater(bufferedBytes: number) {
    if (this.#waiters.length === 0 || bufferedBytes > this.#lowWaterBytes) {
      return;
    }

    // Use a low water mark so waiting producers are released in batches instead
    // of waking one at a time around the high water boundary.
    this.releaseAll();
  }

  releaseAll() {
    const waiters = this.#waiters.splice(0);
    for (const waiter of waiters) {
      waiter.resolve();
    }
  }
}
