export class PeekIterator<T> implements IterableIterator<T> {
  #peeked: IteratorResult<T, void> | undefined = undefined;
  readonly #iter: Iterator<T, void>;

  constructor(iter: Iterator<T, void>) {
    this.#iter = iter;
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this;
  }

  next(): IteratorResult<T, void> {
    if (this.#peeked !== undefined) {
      const p = this.#peeked;
      this.#peeked = undefined;
      return p;
    }
    return this.#iter.next();
  }

  peek(): IteratorResult<T, void> {
    if (this.#peeked !== undefined) {
      return this.#peeked;
    }
    return (this.#peeked = this.#iter.next());
  }
}
