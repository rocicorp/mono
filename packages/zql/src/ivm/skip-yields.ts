import type {Node} from './data.ts';
import type {Stream} from './stream.ts';

// Implemented as a custom IterableIterator rather than a generator function to
// reduce allocations. A generator creates a new state-machine object each time
// it is called. By returning `this` from [Symbol.iterator](), the same object
// acts as both the Iterable and the Iterator, so iterating the stream incurs
// only one allocation (the SkipYieldsStream instance itself) instead of two.
// Additionally, the IteratorResult objects ({value, done}) from the inner
// iterator are returned directly rather than being recreated, avoiding further
// per-item allocations.
class SkipYieldsStream implements IterableIterator<Node> {
  readonly #stream: Stream<Node | 'yield'>;
  #it: Iterator<Node | 'yield'> | undefined = undefined;

  constructor(stream: Stream<Node | 'yield'>) {
    this.#stream = stream;
  }

  [Symbol.iterator](): IterableIterator<Node> {
    this.#it = this.#stream[Symbol.iterator]();
    return this;
  }

  next(): IteratorResult<Node> {
    // #it is always set before next() is called, as [Symbol.iterator]() must
    // be called first (e.g. by a for-of loop).
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const it = this.#it!;
    for (;;) {
      const r = it.next();
      if (r.done || r.value !== 'yield') {
        return r as IteratorResult<Node>;
      }
    }
  }

  return(value?: undefined): IteratorResult<Node> {
    this.#it?.return?.(value);
    return {done: true, value: undefined};
  }
}

export function skipYields(stream: Stream<Node | 'yield'>): Stream<Node> {
  return new SkipYieldsStream(stream);
}
