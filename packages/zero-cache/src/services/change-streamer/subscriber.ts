import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {must} from '../../../../shared/src/must.ts';
import {max} from '../../types/lexi-version.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer-service.ts';
import {type Downstream, type Status} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';

type ErrorType = Enum<typeof ErrorType>;

type BacklogEntry = {
  change: WatermarkedChange;
  done: Resolver<void>;
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
  #backlog: BacklogEntry[] | null;
  #backlogFlush: Promise<void> | undefined;

  constructor(
    protocolVersion: number,
    id: string,
    watermark: string,
    downstream: Subscription<string>,
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
        const done = resolver<void>();
        this.#backlog.push({change, done});
        await done.promise;
      } else {
        await this.#sendChange(change);
      }
    }
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
      const json = BigIntJSON.stringify([
        'status',
        status,
      ] satisfies Downstream);
      if (this.#backlogFlush) {
        void this.#backlogFlush
          .then(() => this.#sendStringifiedDownstream(json))
          .catch(err => this.fail(err));
      } else {
        void this.#sendStringifiedDownstream(json);
      }
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
  setCaughtUp() {
    this.#initialize();
    assert(
      this.#backlog,
      'setCaughtUp() called but subscriber is not in catchup mode',
    );
    if (this.#backlogFlush) {
      return this.#backlogFlush;
    }

    const backlog = this.#backlog;
    const flush = this.#flushBacklog(backlog).finally(() => {
      if (this.#backlogFlush === flush) {
        this.#backlogFlush = undefined;
      }
    });
    this.#backlogFlush = flush;
    void flush.catch(err => this.fail(err));
    return flush;
  }

  async #flushBacklog(backlog: BacklogEntry[]) {
    // #5970: https://github.com/rocicorp/mono/pull/5970
    // Keep catchup handoff flow-controlled so completion means the downstream
    // subscriber consumed the buffered live messages. Previously, fire-and-
    // forget Promise fanout could move a large backlog into downstream pending
    // state and let catchup report success before that work was actually done.
    //
    //   catchup query -> backlog[0] -> downstream ACK
    //                  -> backlog[1] -> downstream ACK
    //                  -> ...
    //                  -> live forwarding
    let next = 0;
    try {
      while (next < backlog.length) {
        const entry = backlog[next++];
        try {
          await this.#sendChange(entry.change);
          entry.done.resolve();
        } catch (err) {
          entry.done.reject(err);
          throw err;
        }
      }
    } catch (err) {
      while (next < backlog.length) {
        backlog[next++].done.reject(err);
      }
      throw err;
    } finally {
      if (this.#backlog === backlog) {
        this.#backlog = null;
      }
    }
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
      // Wait for the ACK of the error message before closing the connection.
      void this.#sendDownstream(['error', {type: error, message}]).finally(() =>
        this.#downstream.cancel(),
      );
    } else {
      this.#downstream.cancel();
    }
  }
}
