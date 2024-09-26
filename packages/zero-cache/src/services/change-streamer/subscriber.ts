import {assert} from 'shared/src/asserts.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import type {WatermarkedChange} from './change-streamer-service.js';
import {type Downstream, ErrorType} from './change-streamer.js';

/**
 * Encapsulates a subscriber to changes. All subscribers start in a
 * "catchup" phase in which changes are buffered in a backlog while the
 * storer is queried to send any changes that were committed since the
 * subscriber's watermark. Once the catchup is complete, calls to
 * {@link send()} result in immediately sending the change.
 */
export class Subscriber {
  readonly id: string;
  readonly #downstream: Subscription<Downstream>;
  #watermark: string;
  #backlog: WatermarkedChange[] | null;

  constructor(
    id: string,
    watermark: string,
    downstream: Subscription<Downstream>,
  ) {
    this.id = id;
    this.#downstream = downstream;
    this.#watermark = watermark;
    this.#backlog = [];
  }

  get watermark() {
    return this.#watermark;
  }

  send(change: WatermarkedChange) {
    const [watermark] = change;
    if (watermark > this.#watermark) {
      if (this.#backlog) {
        this.#backlog.push(change);
      } else {
        this.#send(change);
      }
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  catchup(change: WatermarkedChange) {
    this.#send(change);
  }

  /**
   * Marks the Subscribe as "caught up" and flushes any backlog of
   * entries that were received during the catchup.
   */
  setCaughtUp() {
    assert(this.#backlog);
    for (const change of this.#backlog) {
      this.#send(change);
    }
    this.#backlog = null;
  }

  #send(change: WatermarkedChange) {
    const [watermark, downstream] = change;
    if (watermark > this.watermark) {
      this.#downstream.push(downstream);
      if (downstream[0] === 'commit') {
        this.#watermark = watermark;
      }
    }
  }

  fail(err?: unknown) {
    this.close(ErrorType.Unknown, String(err));
  }

  close(error?: ErrorType, message?: string) {
    if (error) {
      this.#downstream.push(['error', {type: error, message}]);
    }
    this.#downstream.cancel();
  }
}
