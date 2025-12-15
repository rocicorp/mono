export function* joinIterables<T>(...iters: Iterable<T>[]) {
  for (const iter of iters) {
    yield* iter;
  }
}

function* filterIter<T>(
  iter: Iterable<T>,
  p: (t: T, index: number) => boolean,
): Iterable<T> {
  let index = 0;
  for (const t of iter) {
    if (p(t, index++)) {
      yield t;
    }
  }
}

function* mapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => U,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield f(t, index++);
  }
}

export function first<T>(stream: Iterable<T>): T | undefined {
  const it = stream[Symbol.iterator]();
  const {value} = it.next();
  it.return?.();
  return value;
}

export function* once<T>(stream: Iterable<T>): Iterable<T> {
  const it = stream[Symbol.iterator]();
  const {value} = it.next();
  if (value !== undefined) {
    yield value;
  }
  it.return?.();
}

// ES2024 Iterator helpers are available in Node 22+
// https://github.com/tc39/proposal-iterator-helpers

type IteratorWithHelpers<T> = Iterator<T> & {
  map<U>(f: (t: T, index: number) => U): IteratorWithHelpers<U>;
  filter(p: (t: T, index: number) => boolean): IteratorWithHelpers<T>;
  [Symbol.iterator](): IteratorWithHelpers<T>;
};

type IteratorConstructor = {
  from<T>(this: void, iter: Iterable<T>): IteratorWithHelpers<T>;
};

// Check if native Iterator.from is available and bind it once at startup
// We use globalThis to access the runtime value safely
const iteratorCtor = (globalThis as {Iterator?: IteratorConstructor}).Iterator;
const nativeIteratorFrom = iteratorCtor?.from as
  | (<T>(iter: Iterable<T>) => IteratorWithHelpers<T>)
  | undefined;

const iteratorFrom: <T>(e: Iterable<T>) => IteratorWithHelpers<T> =
  nativeIteratorFrom ?? (e => new IterWrapper(e));

// Fallback implementation for environments without ES2024 Iterator helpers
class IterWrapper<T> implements IteratorWithHelpers<T>, IterableIterator<T> {
  readonly #iterator: Iterator<T>;

  constructor(iterable: Iterable<T>) {
    this.#iterator = iterable[Symbol.iterator]();
  }

  next(): IteratorResult<T> {
    return this.#iterator.next();
  }

  [Symbol.iterator](): IteratorWithHelpers<T> {
    return this;
  }

  map<U>(f: (t: T, index: number) => U): IterWrapper<U> {
    return new IterWrapper(mapIter(this, f));
  }

  filter(p: (t: T, index: number) => boolean): IterWrapper<T> {
    return new IterWrapper(filterIter(this, p));
  }
}

export function wrapIterable<T>(iter: Iterable<T>): IteratorWithHelpers<T> {
  return iteratorFrom(iter);
}
