/**
 * A LookaheadIterator is an iterator that reads ahead one value eagerly and
 * provides access to both the current and next value without having to advance.
 */
export class LookaheadIterator<T>
  implements Iterator<[T, ...Array<T | undefined>]>
{
  readonly #iter: Iterator<T>;
  readonly #buffer: Array<T | undefined>;

  #iteratedOnce = false;

  constructor(iter: Iterator<T>, size: number = 2) {
    this.#iter = iter;
    this.#buffer = new Array(size);
    for (let i = 0; i < size; i++) {
      const r = iter.next();
      this.#buffer[i] = r.done ? undefined : r.value;
    }
  }

  [Symbol.iterator](): Iterator<[T, ...Array<T | undefined>]> {
    return this;
  }

  next(): IteratorResult<[T, ...Array<T | undefined>]> {
    if (!this.#iteratedOnce) {
      this.#iteratedOnce = true;
    } else {
      for (let i = 0; i < this.#buffer.length - 1; i++) {
        this.#buffer[i] = this.#buffer[i + 1];
      }
      const r = this.#iter.next();
      this.#buffer[this.#buffer.length - 1] = r.done ? undefined : r.value;
    }
    if (this.#buffer[0] === undefined) {
      return {done: true, value: undefined};
    }
    return {done: false, value: this.#buffer} as IteratorResult<
      [T, ...Array<T | undefined>]
    >;
  }
}
