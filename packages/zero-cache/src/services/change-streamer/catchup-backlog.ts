import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import type {MaybePromise} from '../../../../shared/src/types.ts';

type Entry<T> = {
  change: T;
  consumed: Resolver<void>;
};

type Closed = {state: 'closed'} | {state: 'failed'; cause: unknown};

/**
 * Buffers live changes that arrive while a subscriber is still replaying
 * historical catchup entries.
 */
export class CatchupBacklog<T> {
  readonly #entries: (Entry<T> | undefined)[] = [];
  #flush: Promise<void> | undefined;
  #closed: Closed | undefined;

  get empty() {
    return this.#entries.length === 0;
  }

  /**
   * #5970: https://github.com/rocicorp/mono/pull/5970
   * Give each buffered live change a completion receipt so outage catchup keeps
   * RM backpressure tied to VS consumption instead of hiding the recovery burst
   * in downstream pending work.
   */
  enqueue(entry: T) {
    if (this.#closed) {
      return Promise.reject(this.#rejectionCause(this.#closed));
    }
    const consumed = resolver<void>();
    this.#entries.push({change: entry, consumed});
    return consumed.promise;
  }

  flushWith(consume: (entry: T) => MaybePromise<void>) {
    if (this.#closed) {
      return this.#closed.state === 'failed'
        ? Promise.reject(this.#closed.cause)
        : Promise.resolve();
    }
    if (!this.#flush) {
      this.#flush = this.#drain(consume);
    }
    return this.#flush;
  }

  close() {
    this.#settle({state: 'closed'});
  }

  fail(cause: unknown) {
    this.#settle({state: 'failed', cause});
  }

  async #drain(consume: (entry: T) => MaybePromise<void>) {
    let next = 0;
    try {
      while (next < this.#entries.length) {
        const entry = this.#entries[next];
        assert(entry, 'missing catchup backlog entry');
        this.#entries[next++] = undefined;
        try {
          await consume(entry.change);
          if (this.#closed) {
            this.#settleEntry(entry, this.#closed);
            if (this.#closed.state === 'failed') {
              throw this.#closed.cause;
            }
            return;
          }
          entry.consumed.resolve();
        } catch (err) {
          entry.consumed.reject(err);
          throw err;
        }
      }
    } catch (err) {
      while (next < this.#entries.length) {
        const entry = this.#entries[next];
        this.#entries[next++] = undefined;
        entry?.consumed.reject(err);
      }
      throw err;
    } finally {
      this.#closed ??= {state: 'closed'};
      this.#entries.length = 0;
    }
  }

  #settle(closed: Closed) {
    if (this.#closed) {
      return;
    }
    this.#closed = closed;
    for (const entry of this.#entries) {
      if (entry) {
        this.#settleEntry(entry, closed);
      }
    }
    this.#entries.length = 0;
  }

  #settleEntry(entry: Entry<T>, closed: Closed) {
    if (closed.state === 'failed') {
      entry.consumed.reject(closed.cause);
    } else {
      entry.consumed.resolve();
    }
  }

  #rejectionCause(closed: Closed) {
    return closed.state === 'failed'
      ? closed.cause
      : new Error('cannot enqueue into a closed catchup backlog');
  }
}
