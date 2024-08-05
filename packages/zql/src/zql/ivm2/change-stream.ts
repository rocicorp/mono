export type Mode = 'normal' | 'needy';

/**
 * ChangeStream wraps another iterator and enforces some special semantics:
 *
 * 1. The iterator is only callable once. Our change streams can only be used
 *    once because they are coupled to the state of the datastore, which would
 *    need to be reset if we wanted to iterate again. We want to enforce that
 *    they can only be iterated once.
 * 2. If the iterator contains data that was push()'d, it must be fully
 *    consumed. Otherwise, the resulting state of the query will be incomplete.
 *
 * For an example of (2), consider a query like:
 *
 * z.issue.select().orderBy('id').limit(10);
 *
 * On the first pull, the ChangeStream we receive will be `normal`. We can stop
 * consuming it at any point because it is sorted. When we've received 10 rows,
 * we can stop consuming it and the query will be complete.
 *
 * Now consider that someone pushes two changes into the pipeline. These changes
 * will not be sorted and so we must consume them all to ensure that the query
 * results are correct.
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
    const result = this.#iterator.next();
    this.#done = result.done ?? false;
    return result;
  }

  throw() {
    if (this.#iterator.throw) {
      return this.#iterator.throw();
    }
    return {done: true, value: undefined} as const;
  }

  return() {
    this.#iterator.return?.();

    if (!this.#done && this.#mode === 'needy') {
      throw new Error('NeedyIterator was not fully consumed!');
    }

    this.#done = true;
    return {done: true, value: undefined} as const;
  }
}
