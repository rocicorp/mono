import type {Node} from './data.js';

// streams are lazy forward-only iterables.
// once they reach the end they can't be restarted.
// they are iterable, not iterator, so that they can be used in for-each,
// and so that we know when consumer has stopped iterator. this allows us
// to clean up resources like sql statements.
export type Stream = Iterable<Node>;

export function makeStream(iterable: Iterable<Node>): Stream {
  return new OneTimeIterable(iterable);
}

class OneTimeIterable<T> implements Iterable<T> {
  readonly #iterator: Iterator<T>;

  constructor(iterable: Iterable<T>) {
    this.#iterator = iterable[Symbol.iterator]();
  }

  [Symbol.iterator]() {
    return this;
  }

  next() {
    return this.#iterator.next();
  }

  throw() {
    if (this.#iterator.throw) {
      return this.#iterator.throw();
    }
    return {done: true, value: undefined} as const;
  }

  return() {
    this.#iterator.return?.();
    return {done: true, value: undefined} as const;
  }
}
