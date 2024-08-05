export type Mode = 'normal' | 'needy';

/**
 * ChangeStream wraps another iterator and enforces some special semantics:
 *
 * - The iterator is only callable once. Our change streams can only be used
 *   once because they are coupled to the state of the datastore, which would
 *   need to be reset if we wanted to iterate again. We want to enforce that
 *   they can only be iterated once.
 * - If the iterator is representing some data that was pushed, it must be
 *   fully consumed. Otherwise, the resulting state of the query will be
 *   incomplete.
 */
export class ChangeStream<T> implements Iterable<T> {
  readonly #iterator: Iterator<T>;
  readonly #mode: Mode;

  #done: boolean;

  constructor(iterable: Iterable<T>, mode: Mode = 'normal') {
    this.#iterator = iterable[Symbol.iterator]();
    this.#mode = mode;
    this.#done = false;
  }

  [Symbol.iterator]() {
    return this;
  }

  next() {
    if (this.#done) {
      return {done: true, value: undefined} as const;
    }
    const result = this.#iterator.next();
    if (result.done) {
      this.#done = true;
    }
    return result;
  }

  return() {
    if (!this.#done && this.#mode === 'needy') {
      throw new Error('NeedyIterator was not fully consumed!');
    }
    this.#done = true;
    return {done: true, value: undefined} as const;
  }
}
