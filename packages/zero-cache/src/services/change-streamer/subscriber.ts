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
import type {BackfillState} from './backfill-state.ts';
import type {PreSerializedBatch} from './broadcast.ts';
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
  wsBatched?: boolean | undefined;

  /**
   * Called whenever the subscriber's acked watermark advances, i.e. when it
   * has confirmed that a commit was durably applied to its replica. The ACK
   * therefore lags the subscriber's replica rather than leading it.
   */
  onAck?: ((watermark: string) => void) | undefined;

  /**
   * The subscriber's pending backfills (protocol v8+), which are tracked
   * over the changes sent to the subscriber.
   */
  backfills?: BackfillState | undefined;

  /**
   * Called once the subscriber has been sent all changes up to the head of
   * the stream, i.e. its catchup and backlog have been flushed. From this
   * point on, the subscriber's tracked {@link backfills} reflect the same
   * position in the stream as that of the change-streamer.
   */
  onAligned?: ((subscriber: Subscriber) => void) | undefined;

  /**
   * Called when a `backfill` or `backfill-completed` message is ignored for
   * some of the subscriber's pending backfill columns, after the subscriber
   * is aligned.
   */
  onBackfillIgnored?: ((columns: string[]) => void) | undefined;

  /**
   * Resolves to whether lag reporting is disabled upstream. If so, a
   * {@link LAG_REPORTING_DISABLED} status is sent once the subscriber is
   * aligned, as a stand-in for the lag reports that a subscriber would
   * otherwise use to determine that it is caught up (e.g. to advertise
   * readiness to serve traffic).
   */
  lagReportingDisabled?: Promise<boolean> | undefined;
};

/**
 * A status message queued in the backlog, so that it is sent downstream in
 * the order in which it was received relative to (backlogged) changes.
 */
type QueuedStatus = {readonly status: string};

type BacklogEntry = WatermarkedChange | QueuedStatus;

function isQueuedStatus(entry: BacklogEntry): entry is QueuedStatus {
  return !Array.isArray(entry);
}

function entryBytes(entry: BacklogEntry): number {
  return isQueuedStatus(entry) ? entry.status.length : entry[2].length;
}

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
  readonly #downstream: Subscription<string | PreSerializedBatch>;
  readonly #wsBatched: boolean;
  #watermark: string;
  #acked: string;
  #backlog: RingBuffer<BacklogEntry> | null;
  // While catchup is running, live changes are buffered here instead of being
  // pushed downstream. RingBuffer lets drainBacklog consume that backlog without
  // shifting an array, which matters when a subscriber is far behind.
  #backlogBytes = 0;
  #backlogInFlightBytes = 0;
  #backlogDrain: Promise<void> | null = null;
  readonly #backlogBackpressure: ByteBackpressureGate;
  readonly #backlogFullWaiters = new Set<Resolver<void>>();
  readonly #onAck: ((watermark: string) => void) | undefined;
  readonly #backfills: BackfillState | undefined;
  readonly #onAligned: ((subscriber: Subscriber) => void) | undefined;
  readonly #onBackfillIgnored: ((columns: string[]) => void) | undefined;
  readonly #lagReportingDisabled: Promise<boolean> | undefined;
  #aligned = false;

  constructor(
    protocolVersion: number,
    id: string,
    mode: ReplicatorMode,
    watermark: string,
    downstream: Subscription<string | PreSerializedBatch>,
    options: SubscriberOptions = {},
  ) {
    this.#protocolVersion = protocolVersion;
    this.id = id;
    this.mode = mode;
    this.#downstream = downstream;
    this.#wsBatched = options.wsBatched ?? false;
    this.#watermark = watermark;
    this.#acked = watermark;
    this.#backlog = new RingBuffer();
    this.#backlogBackpressure = new ByteBackpressureGate(
      options.backlogHighWaterBytes ?? DEFAULT_BACKLOG_HIGH_WATER_BYTES,
      options.backlogLowWaterRatio ?? DEFAULT_BACKLOG_LOW_WATER_RATIO,
    );
    this.#onAck = options.onAck;
    this.#backfills = options.backfills;
    this.#onAligned = options.onAligned;
    this.#onBackfillIgnored = options.onBackfillIgnored;
    this.#lagReportingDisabled = options.lagReportingDisabled;
  }

  /**
   * The subscriber's tracked pending backfills, or `undefined` if the
   * subscriber does not report them (i.e. protocol < v8).
   */
  get backfills(): BackfillState | undefined {
    return this.#backfills;
  }

  /**
   * Whether the subscriber has been sent all changes up to the head of the
   * stream (see {@link SubscriberOptions.onAligned}).
   */
  get aligned(): boolean {
    return this.#aligned;
  }

  #trackBackfills(change: WatermarkedChange) {
    if (this.#backfills) {
      const ignored = this.#backfills.applySerialized(change);
      if (ignored.length && this.#aligned) {
        this.#onBackfillIgnored?.(ignored);
      }
    }
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

  sendBatch(
    changes: readonly WatermarkedChange[],
    preSerialized?: PreSerializedBatch | undefined,
  ): Promise<void> {
    if (changes.length === 0) {
      return promiseVoid;
    }

    if (
      this.#wsBatched &&
      preSerialized &&
      !this.#backlog &&
      this.#initialized &&
      changes[0][0] > this.#watermark &&
      (this.#protocolVersion >= 5 ||
        changes.every(c => this.supportsMessage(c[1])))
    ) {
      let commitWatermark: string | undefined;
      for (let i = changes.length - 1; i >= 0; i--) {
        if (changes[i][1] === 'commit') {
          commitWatermark = changes[i][0];
          break;
        }
      }
      if (commitWatermark) {
        this.#watermark = commitWatermark;
      }
      for (const change of changes) {
        this.#trackBackfills(change);
      }
      return this.#sendPreSerializedDownstream(preSerialized, commitWatermark);
    }

    const promises: Promise<void>[] = [];
    for (const change of changes) {
      const p = this.send(change);
      if (p !== promiseVoid) {
        promises.push(p);
      }
    }
    if (promises.length === 0) {
      return promiseVoid;
    }
    if (promises.length === 1) {
      return promises[0];
    }
    return Promise.all(promises).then(() => {});
  }

  #initialized = false;

  /**
   * Called once the subscriber's watermark has been validated in the initial
   * catchup process.
   */
  #initialize() {
    if (!this.#initialized) {
      this.#initialized = true;
      // The initial status precedes the catchup, so it bypasses the backlog.
      void this.#sendDownstream(['status', {tag: 'status'}]);
    }
  }

  /**
   * Sends a status message (e.g. a lag report) downstream. While the
   * subscriber is catching up, the status is queued in the backlog behind
   * the changes that preceded it in the replication stream, so that the
   * subscriber processes it in stream order. This is necessary for lag
   * reports to measure the lag of the subscriber, including its catchup.
   *
   * Status messages received before the subscriber is initialized are
   * dropped.
   */
  sendStatus(status: Status) {
    if (!this.#initialized) {
      return;
    }
    const downstream: Downstream = ['status', status];
    if (this.#backlog) {
      this.#pushBacklog({status: BigIntJSON.stringify(downstream)});
      return;
    }
    void this.#sendDownstream(downstream);
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
    this.#trackBackfills(change);
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

  async #sendPreSerializedDownstream(
    batch: PreSerializedBatch,
    commitWatermark?: string | undefined,
  ): Promise<void> {
    const size = batch.byteLength;
    this.#pending += batch.changes.length;
    this.#pendingBytes += size;
    const {result} = this.#downstream.push(batch);
    try {
      const outcome = await result;
      if (commitWatermark && outcome === 'consumed') {
        const acked = max(this.#acked, commitWatermark);
        if (acked !== this.#acked) {
          this.#acked = acked;
          this.#onAck?.(acked);
        }
      }
    } finally {
      this.#pending -= batch.changes.length;
      this.#pendingBytes -= size;
      this.#processed += batch.changes.length;
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

  #pushBacklog(entry: BacklogEntry) {
    assert(this.#backlog, 'cannot push to backlog after catchup completed');
    this.#backlog.push(entry);
    this.#backlogBytes += entryBytes(entry);
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
        const entry = this.#backlog?.shift();
        if (!entry) {
          const closed = this.#backlog === null;
          this.#backlog = null;
          this.#backlogBytes = 0;
          this.#backlogBackpressure.releaseIfUnderLowWater(
            this.#bufferedBacklogBytes,
          );
          if (!closed && !this.#aligned) {
            // All changes up to the head have been handed to #sendChange()
            // (and thus tracked), and subsequent changes are sent directly.
            this.#aligned = true;
            this.#onAligned?.(this);
            void this.#lagReportingDisabled?.then(disabled => {
              if (disabled) {
                this.sendStatus(LAG_REPORTING_DISABLED);
              }
            });
          }
          break;
        }

        const bytes = entryBytes(entry);
        this.#backlogBytes -= bytes;
        this.#backlogInFlightBytes += bytes;
        this.#backlogBackpressure.releaseIfUnderLowWater(
          this.#bufferedBacklogBytes,
        );

        // Send backlog entries in order, but keep only a bounded byte window in
        // flight. This avoids replacing one unbounded buffer with another inside
        // the downstream Subscription during catchup completion.
        const send = isQueuedStatus(entry)
          ? this.#sendStringifiedDownstream(entry.status).then(() => {})
          : this.#sendChange(entry);
        const promise = send.finally(() => {
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

/**
 * Sent to an aligned subscriber when lag reporting is disabled upstream,
 * signaling that the subscriber is caught up. This is a lag report with a
 * `nextSendTimeMs` of 0 and no `lastTimings`, so that no lag is measured
 * (see `isLagReportingDisabledSignal()` in the replicator's recorder).
 */
export const LAG_REPORTING_DISABLED: Status = {
  tag: 'status',
  lagReport: {nextSendTimeMs: 0},
};
