import {assert} from 'shared/src/asserts.js';
import {Subscription} from 'zero-cache/src/types/subscription.js';
import {ChangeEntry, Downstream, ErrorType} from './change-streamer.js';

/**
 * Encapsulates a subscriber to changes. All subscribers start in a
 * "catchup" phase in which changes are buffered in a backlog while the
 * storer is queried to send any changes that were committed since the
 * subscriber's watermark. Once the catchup is complete, calls to
 * {@link send()} result in immediately sending the change.
 */
export class Subscriber {
  readonly id: string;
  readonly watermark: string;
  readonly #downstream: Subscription<Downstream>;
  #backlog: ChangeEntry[] | null;

  constructor(
    id: string,
    watermark: string,
    downstream: Subscription<Downstream>,
  ) {
    this.id = id;
    this.watermark = watermark;
    this.#downstream = downstream;
    this.#backlog = [];
  }

  send(change: ChangeEntry) {
    const {watermark} = change;
    if (watermark > this.watermark) {
      if (this.#backlog) {
        this.#backlog.push(change);
      } else {
        this.#send(change);
      }
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  catchup(change: ChangeEntry) {
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

  #send(change: ChangeEntry) {
    const {watermark} = change;
    if (watermark > this.watermark) {
      this.#downstream.push(['change', change]);
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
