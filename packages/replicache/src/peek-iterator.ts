/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
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
