export class PeekIterator<T> implements IterableIterator<T> {
  #peeked: IteratorResult<T> | undefined = undefined;
  readonly #iter: Iterator<T>;

  constructor(iter: Iterator<T>) {
    this.#iter = iter;
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this;
  }

  next(): IteratorResult<T> {
    if (this.#peeked !== undefined) {
      const p = this.#peeked;
      this.#peeked = undefined;
      return p;
    }
    return this.#iter.next();
  }

  peek(): IteratorResult<T> {
    if (this.#peeked !== undefined) {
      return this.#peeked;
    }
    return (this.#peeked = this.#iter.next());
  }
}
