import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {must} from '../../../../shared/src/must.ts';
import {max} from '../../types/lexi-version.ts';
import type {StringifiedStreamPayload} from '../../types/streams.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer-service.ts';
import {type Downstream, type Status} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';

type ErrorType = Enum<typeof ErrorType>;

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
  readonly #downstream: Subscription<StringifiedStreamPayload>;
  readonly #latestStatus: () => Status;
  // #watermark is the latest commit sent to this subscriber. #acked lags until
  // the downstream stream confirms that the commit frame was consumed.
  #watermark: string;
  #acked: string;
  // Non-null while catchup is reading historical changeLog rows. Live forwarded
  // changes are buffered here so the subscriber sees "historical, then live" in
  // one ordered stream.
  #backlog: WatermarkedChange[] | null;

  constructor(
    protocolVersion: number,
    id: string,
    watermark: string,
    downstream: Subscription<StringifiedStreamPayload>,
    latestStatus: () => Status,
  ) {
    this.#protocolVersion = protocolVersion;
    this.id = id;
    this.#downstream = downstream;
    this.#latestStatus = latestStatus;
    this.#watermark = watermark;
    this.#acked = watermark;
    this.#backlog = [];
  }

  get watermark() {
    return this.#watermark;
  }

  get acked() {
    return this.#acked;
  }

  async send(change: WatermarkedChange) {
    const [watermark] = change;
    if (watermark > this.#watermark) {
      if (this.#backlog) {
        // Catchup has not finished; keep live traffic behind the historical
        // replay so the subscriber never sees a gap.
        this.#backlog.push(change);
      } else {
        await this.#sendChanges([change]);
      }
    }
  }

  async sendBatch(changes: readonly WatermarkedChange[]) {
    if (this.#backlog) {
      // Entries at or before the subscriber watermark were already represented
      // in its replica and must not be replayed during catchup handoff.
      for (const change of changes) {
        if (change[0] > this.#watermark) {
          this.#backlog.push(change);
        }
      }
      return;
    }
    await this.#sendChanges(changes);
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
      // Older protocol versions do not know status messages. Before initialize,
      // the subscriber's requested watermark has not been validated yet.
      void this.#sendDownstream(['status', status]);
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  async catchup(change: WatermarkedChange) {
    this.#initialize();
    await this.#sendChanges([change]);
  }

  /** catchupBatch() is called on ChangeEntries loaded from the store. */
  async catchupBatch(changes: readonly WatermarkedChange[]) {
    this.#initialize();
    await this.#sendChanges(changes);
  }

  /**
   * Marks the Subscribe as "caught up" and flushes any backlog of
   * entries that were received during the catchup.
   */
  setCaughtUp() {
    this.#initialize();
    assert(
      this.#backlog,
      'setCaughtUp() called but subscriber is not in catchup mode',
    );
    // Note that this method must be asynchronous in order for send() to
    // interpret the #backlog variable correctly. This is the only place
    // where I/O flow control is not heeded. However, it will be awaited
    // by the next caller to send().
    const backlog = this.#backlog;
    for (let i = 0; i < backlog.length; i += 64) {
      // Send fixed-size ranges from the existing array instead of slice()ing.
      // Catchup handoff can hold thousands of live entries under load, and the
      // range form avoids duplicating those references during the flush.
      void this.#sendChanges(backlog, i, i + 64);
    }
    this.#backlog = null;
  }

  async #sendChanges(
    changes: readonly WatermarkedChange[],
    start = 0,
    end = changes.length,
  ) {
    const json: string[] = [];
    let commitWatermark: string | undefined;
    for (let i = start; i < end && i < changes.length; i++) {
      const change = changes[i];
      const [watermark, tag, payload] = change;
      if (watermark <= this.watermark) {
        // Catchup queries include the row at the requested watermark to prove
        // continuity. The subscriber only needs changes after that point.
        continue;
      }
      if (!this.supportsMessage(tag)) {
        // Protocol-version filtering happens before JSON write so old serving
        // replicas can continue catching up through newer metadata events.
        continue;
      }
      if (tag === 'commit') {
        // Only commit messages advance the public replication position; data
        // messages carry an internal ordering watermark but are not resumable.
        this.#watermark = watermark;
        commitWatermark = watermark;
      }
      json.push(payload);
    }
    if (json.length === 0) {
      // Nothing survived the watermark/protocol filters, so there is no commit
      // ACK to wait for and no downstream flow-control work to track.
      return;
    }
    const payload = json.length === 1 ? json[0] : json;
    const result = await this.#sendStringifiedDownstream(payload, json.length);
    if (commitWatermark !== undefined && result === 'consumed') {
      // Purge logic uses acked watermarks, so advance only after the downstream
      // stream confirms that the commit frame was consumed.
      this.#acked = max(this.#acked, commitWatermark);
    }
  }

  #sendDownstream(downstream: Downstream) {
    return this.#sendStringifiedDownstream(BigIntJSON.stringify(downstream));
  }

  async #sendStringifiedDownstream(
    payload: StringifiedStreamPayload,
    messageCount = typeof payload === 'string' ? 1 : payload.length,
  ) {
    // pending/processed track logical downstream messages, not websocket frames;
    // a stringified array still represents several change-stream messages.
    this.#pending += messageCount;
    const {result} = this.#downstream.push(payload);
    try {
      return await result;
    } finally {
      this.#pending -= messageCount;
      this.#processed += messageCount;
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
    return this.#pending;
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

  getStats(): {processRate: number; pending: number} {
    const pending = this.#pending;
    if (this.#samples.length < 2) {
      return {processRate: 0, pending};
    }
    const from = this.#samples[0];
    const to = must(this.#samples.at(-1));
    const processed = to.processed - from.processed;
    const seconds = (to.timestamp - from.timestamp) / 1000;
    const processRate = seconds === 0 ? 0 : processed / seconds;
    return {processRate, pending};
  }

  supportsMessage(tag: ChangeTag) {
    switch (tag) {
      case 'update-table-metadata':
        // Protocol v5 introduced relation row-key/table-metadata updates.
        // Earlier replicas must skip this message instead of failing catchup.
        return this.#protocolVersion >= 5;
    }
    return true;
  }

  fail(err?: unknown) {
    this.close(ErrorType.Unknown, String(err));
  }

  close(error?: ErrorType, message?: string) {
    if (error) {
      // Wait for the ACK of the error message before closing the connection.
      void this.#sendDownstream(['error', {type: error, message}]).finally(() =>
        this.#downstream.cancel(),
      );
    } else {
      this.#downstream.cancel();
    }
  }
}
