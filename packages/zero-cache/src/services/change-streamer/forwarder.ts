import {ChangeEntry} from './change-streamer.js';
import {Subscriber} from './subscriber.js';

export class Forwarder {
  readonly #active = new Map<string, Subscriber>();
  readonly #queued = new Map<string, Subscriber>();
  #inTransaction = false;

  /**
   * `add()` is called in lock step with `Storer.catchup()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  add(sub: Subscriber) {
    this.remove(sub.id);
    if (this.#inTransaction) {
      this.#queued.set(sub.id, sub);
    } else {
      this.#active.set(sub.id, sub);
    }
  }

  remove(id: string, instance?: Subscriber) {
    const sub = this.#active.get(id) ?? this.#queued.get(id);
    if (sub && (instance ?? sub) === sub) {
      this.#active.delete(id);
      this.#queued.delete(id);
      sub.close();
    }
  }

  /**
   * `forward()` is called in lockstep with `Storer.store()` so that the
   * two components have an equivalent interpretation of whether a Transaction is
   * currently being streamed.
   */
  forward(entry: ChangeEntry) {
    const {change} = entry;
    for (const active of this.#active.values()) {
      active.send(entry);
    }
    switch (change.tag) {
      case 'begin':
        // While in a Transaction, all added subscribers are "queued" so that no
        // messages are forwarded to them. This state corresponds to being queued
        // for catchup in the Storer, which will retrieve historic changes
        // and call catchup() once the current transaction is committed.
        this.#inTransaction = true;
        break;
      case 'commit':
        // Upon commit, all queued subscribers are transferred to the active set.
        // This means that they can receive messages starting from the next transaction.
        // Note that if catchup is still in progress (in the Storer), these messages
        // will be buffered in the backlog until catchup completes.
        this.#inTransaction = false;
        for (const sub of this.#queued.values()) {
          this.#active.set(sub.id, sub);
        }
        this.#queued.clear();
        break;
    }
  }
}