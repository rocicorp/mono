// streams are lazy forward-only iterables.
// once they reach the end they can't be restarted.
// they are iterable, not iterator, so that they can be used in for-each,
// and so that we know when consumer has stopped iterator. this allows us
// to clean up resources like sql statements.
export type Stream<T> = Iterable<T>;

export function makeStream<T>(iterable: Iterable<T> | Iterator<T>): Stream<T> {
  return new OneTimeIterable(iterable);
}

class OneTimeIterable<T> implements Iterable<T> {
  readonly #iterator: Iterator<T>;

  constructor(iterable: Iterable<T> | Iterator<T>) {
    this.#iterator = (iterable as Iterable<T>)[Symbol.iterator]
      ? (iterable as Iterable<T>)[Symbol.iterator]()
      : (iterable as Iterator<T>);
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
