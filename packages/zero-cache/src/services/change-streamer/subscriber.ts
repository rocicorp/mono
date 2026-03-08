import {assert} from '../../../../shared/src/asserts.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {must} from '../../../../shared/src/must.ts';
import {max} from '../../types/lexi-version.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ChangeStreamData} from '../change-source/protocol/current.ts';
import type {WatermarkedChange} from './change-streamer-service.ts';
import {type Downstream} from './change-streamer.ts';
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
  readonly #downstream: Subscription<Downstream>;
  #watermark: string;
  #acked: string;
  #backlog: WatermarkedChange[] | null;

  constructor(
    protocolVersion: number,
    id: string,
    watermark: string,
    downstream: Subscription<Downstream>,
  ) {
    this.#protocolVersion = protocolVersion;
    this.id = id;
    this.#downstream = downstream;
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
        this.#backlog.push(change);
      } else {
        await this.#sendChange(change);
      }
    }
  }

  #initialStatusSent = false;

  #ensureInitialStatusSent() {
    if (this.#protocolVersion >= 2 && !this.#initialStatusSent) {
      void this.#sendDownstream(['status', {tag: 'status'}]);
      this.#initialStatusSent = true;
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  async catchup(change: WatermarkedChange) {
    this.#ensureInitialStatusSent();
    await this.#sendChange(change);
  }

  /**
   * Marks the Subscribe as "caught up" and flushes any backlog of
   * entries that were received during the catchup.
   */
  setCaughtUp() {
    this.#ensureInitialStatusSent();
    assert(
      this.#backlog,
      'setCaughtUp() called but subscriber is not in catchup mode',
    );
    // Note that this method must be asynchronous in order for send() to
    // interpret the #backlog variable correctly. This is the only place
    // where I/O flow control is not heeded. However, it will be awaited
    // by the next caller to send().
    for (const change of this.#backlog) {
      void this.#sendChange(change);
    }
    this.#backlog = null;
  }

  async #sendChange(change: WatermarkedChange) {
    const [watermark, downstream] = change;
    if (watermark <= this.watermark) {
      return;
    }
    if (!this.supportsMessage(downstream[1])) {
      return;
    }
    if (downstream[0] === 'commit') {
      this.#watermark = watermark;
    }
    const result = await this.#sendDownstream(downstream);
    if (downstream[0] === 'commit' && result === 'consumed') {
      this.#acked = max(this.#acked, watermark);
    }
  }

  async #sendDownstream(downstream: Downstream) {
    this.#pending++;
    const {result} = this.#downstream.push(downstream);
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

  supportsMessage(change: ChangeStreamData[1]) {
    switch (change.tag) {
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
    if (error) {
      const {result} = this.#downstream.push(['error', {type: error, message}]);
      // Wait for the ACK of the error message before closing the connection.
      void result.then(() => this.#downstream.cancel());
    } else {
      this.#downstream.cancel();
    }
  }
}
