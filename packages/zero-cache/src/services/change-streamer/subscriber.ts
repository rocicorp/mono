import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {must} from '../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {RingBuffer} from '../../../../shared/src/ring-buffer.ts';
import {max} from '../../types/lexi-version.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer-service.ts';
import {type Downstream, type Status} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';

type ErrorType = Enum<typeof ErrorType>;

const DEFAULT_BACKLOG_HIGH_WATER_BYTES = 16 * 1024 * 1024;
const DEFAULT_BACKLOG_LOW_WATER_RATIO = 0.8;

export type SubscriberOptions = {
  backlogHighWaterBytes?: number | undefined;
  backlogLowWaterRatio?: number | undefined;
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

  constructor(
    protocolVersion: number,
    id: string,
    watermark: string,
    downstream: Subscription<string>,
    latestStatus: () => Status,
    options: SubscriberOptions = {},
  ) {
    this.#protocolVersion = protocolVersion;
    this.id = id;
    this.#downstream = downstream;
    this.#latestStatus = latestStatus;
    this.#watermark = watermark;
    this.#acked = watermark;
    this.#backlog = new RingBuffer();
    this.#backlogBackpressure = new ByteBackpressureGate(
      options.backlogHighWaterBytes ?? DEFAULT_BACKLOG_HIGH_WATER_BYTES,
      options.backlogLowWaterRatio ?? DEFAULT_BACKLOG_LOW_WATER_RATIO,
    );
  }

  get watermark() {
    return this.#watermark;
  }

  get acked() {
    return this.#acked;
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
    if (this.#protocolVersion >= 2 && this.#initialized) {
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
      this.#acked = max(this.#acked, watermark);
    }
  }

  #sendDownstream(downstream: Downstream) {
    return this.#sendStringifiedDownstream(BigIntJSON.stringify(downstream));
  }

  async #sendStringifiedDownstream(json: string) {
    this.#pending++;
    const {result} = this.#downstream.push(json);
    try {
      return await result;
    } finally {
      this.#pending--;
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

  getStats(): {
    processRate: number;
    pending: number;
    backlog: number;
    backlogBytes: number;
  } {
    const pending = this.numPending;
    if (this.#samples.length < 2) {
      return {
        processRate: 0,
        pending,
        backlog: this.#backlogCount,
        backlogBytes: this.#bufferedBacklogBytes,
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
    };
  }

  supportsMessage(tag: ChangeTag) {
    switch (tag) {
      case 'update-table-metadata':
        // update-table-row-key is only understood by subscribers >= protocol v5
        return this.#protocolVersion >= 5;
    }
    return true;
  }

  fail(err?: unknown) {
    this.close(ErrorType.Unknown, String(err));
  }

  close(error?: ErrorType, message?: string) {
    // Closing the subscriber must also release producers that are blocked on
    // backlog capacity; there is no future drain that could wake them.
    this.#backlog = null;
    this.#backlogBytes = 0;
    this.#backlogBackpressure.releaseAll();

    if (error) {
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

  #pushBacklog(change: WatermarkedChange) {
    assert(this.#backlog, 'cannot push to backlog after catchup completed');
    this.#backlog.push(change);
    this.#backlogBytes += change[2].length;
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
