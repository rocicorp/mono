export class PeekIterator<T> implements IterableIterator<T> {
  private _peeked: IteratorResult<T> | undefined = undefined;
  private readonly _iter: Iterator<T>;

  constructor(iter: Iterator<T>) {
    this._iter = iter;
  }

  [Symbol.iterator](): IterableIterator<T> {
    return this;
  }

  next(): IteratorResult<T> {
    if (this._peeked !== undefined) {
      const p = this._peeked;
      this._peeked = undefined;
      return p;
    }
    return this._iter.next();
  }

  peek(): IteratorResult<T> {
    if (this._peeked !== undefined) {
      return this._peeked;
    }
    return (this._peeked = this._iter.next());
  }
}
