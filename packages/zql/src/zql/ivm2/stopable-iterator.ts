/**
 * An iterator that can be stopped. Useful if the data backing the iterator
 * changes and you don't want clients to be able to keep iterating.
 */
export class StoppableIterator<T> {
  #iterator: Iterator<T>;
  #stopped = false;

  constructor(iterator: Iterator<T>) {
    this.#iterator = iterator;
  }

  next() {
    if (this.#stopped) {
      throw new Error('Iterator has been stopped');
    }
    return this.#iterator.next();
  }

  stop() {
    this.#stopped = true;
  }
}
