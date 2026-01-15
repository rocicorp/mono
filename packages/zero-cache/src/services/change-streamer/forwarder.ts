import {joinIterables, wrapIterable} from '../../../../shared/src/iterables.ts';
import type {WatermarkedChange} from './change-streamer-service.ts';
import type {Subscriber} from './subscriber.ts';

export class Forwarder {
  readonly #active = new Set<Subscriber>();
  readonly #queued = new Set<Subscriber>();
  #inTransaction = false;

  /**
   * `add()` is called in lock step with `Storer.catchup()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  add(sub: Subscriber) {
    if (this.#inTransaction) {
      this.#queued.add(sub);
    } else {
      this.#active.add(sub);
    }
  }

  remove(sub: Subscriber) {
    this.#active.delete(sub);
    this.#queued.delete(sub);
    sub.close();
  }

  /**
   * `forward()` is called in lockstep with `Storer.store()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  forward(entry: WatermarkedChange) {
    const [type] = entry[1];
    for (const active of this.#active.values()) {
      active.send(entry);
    }
    switch (type) {
      case 'begin':
        // While in a Transaction, all added subscribers are "queued" so that no
        // messages are forwarded to them. This state corresponds to being queued
        // for catchup in the Storer, which will retrieve historic changes
        // and call catchup() once the current transaction is committed.
        this.#inTransaction = true;
        break;
      case 'commit':
      case 'rollback':
        // Upon commit or rollback, all queued subscribers are transferred to
        // the active set. This means that they can receive messages starting
        // from the next transaction.
        //
        // Note that if catchup is still in progress (in the Storer), these messages
        // will be buffered in the backlog until catchup completes.
        this.#inTransaction = false;
        for (const sub of this.#queued.values()) {
          this.#active.add(sub);
        }
        this.#queued.clear();
        break;
    }
  }

  getAcks(): Set<string> {
    return new Set(
      joinIterables(
        wrapIterable(this.#active).map(s => s.acked),
        wrapIterable(this.#queued).map(s => s.acked),
      ),
    );
  }
}
