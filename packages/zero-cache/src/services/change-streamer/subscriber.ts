import {assert} from '../../../../shared/src/asserts.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {max} from '../../types/lexi-version.ts';
import type {Subscription} from '../../types/subscription.ts';
import {type PendingResult} from '../../types/subscription.ts';
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

  #initialStatusSent = false;

  #ensureInitialStatusSent() {
    if (this.#protocolVersion >= 2 && !this.#initialStatusSent) {
      this.#downstream.push(['status', {tag: 'status'}]);
      this.#initialStatusSent = true;
    }
  }

  /** catchup() is called on ChangeEntries loaded from the store. */
  catchup(change: WatermarkedChange): PendingResult {
    this.#ensureInitialStatusSent();
    return this.#send(change);
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
    for (const change of this.#backlog) {
      this.#send(change);
    }
    this.#backlog = null;
  }

  #send(change: WatermarkedChange): PendingResult {
    const [watermark, downstream] = change;
    if (watermark <= this.watermark) {
      return ALREADY_CONSUMED_RESULT;
    }
    if (!this.supportsMessage(downstream[1])) {
      return ALREADY_CONSUMED_RESULT;
    }
    const pending = this.#downstream.push(downstream);
    if (downstream[0] === 'commit') {
      this.#watermark = watermark;
      void pending.result.then(val => {
        if (val === 'consumed') {
          this.#acked = max(this.#acked, watermark);
        }
      });
    }
    return pending;
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

const ALREADY_CONSUMED_RESULT: PendingResult = {
  result: Promise.resolve('consumed'),
};
