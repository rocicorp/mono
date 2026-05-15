import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import type {MaybePromise} from '../../../../shared/src/types.ts';

type Entry<T> = {
  change: T;
  consumed: Resolver<void>;
};

/**
 * Buffers live changes that arrive while a subscriber is still replaying
 * historical catchup entries.
 */
export class CatchupBacklog<T> {
  readonly #entries: (Entry<T> | undefined)[] = [];
  #flush: Promise<void> | undefined;
  #closed = false;

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
    assert(!this.#closed, 'cannot enqueue into a closed catchup backlog');
    const consumed = resolver<void>();
    this.#entries.push({change: entry, consumed});
    return consumed.promise;
  }

  flushWith(consume: (entry: T) => MaybePromise<void>) {
    if (!this.#flush) {
      this.#flush = this.#drain(consume);
    }
    return this.#flush;
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
      this.#closed = true;
      this.#entries.length = 0;
    }
  }
}
