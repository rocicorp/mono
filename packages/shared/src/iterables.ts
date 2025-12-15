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
  from<T>(iter: Iterable<T>): IteratorWithHelpers<T>;
};

// Check if native Iterator.from is available
// We use globalThis to access the runtime value safely
const hasNativeIteratorFrom = (() => {
  try {
    return (
      typeof (globalThis as {Iterator?: unknown}).Iterator !== 'undefined' &&
      typeof (
        (globalThis as {Iterator?: {from?: unknown}}).Iterator as {
          from?: unknown;
        }
      ).from === 'function'
    );
  } catch {
    return false;
  }
})();

// Fallback implementation for environments without ES2024 Iterator helpers
class IterWrapper<T> implements Iterable<T> {
  iter: Iterable<T>;
  constructor(iter: Iterable<T>) {
    this.iter = iter;
  }

  [Symbol.iterator]() {
    return this.iter[Symbol.iterator]();
  }

  map<U>(f: (t: T, index: number) => U): IterWrapper<U> {
    return new IterWrapper(mapIter(this.iter, f));
  }

  filter(p: (t: T, index: number) => boolean): IterWrapper<T> {
    return new IterWrapper(filterIter(this.iter, p));
  }
}

export function wrapIterable<T>(
  iter: Iterable<T>,
): IterWrapper<T> | IteratorWithHelpers<T> {
  if (hasNativeIteratorFrom) {
    // Use native ES2024 Iterator.from
    const IteratorCtor = (globalThis as {Iterator?: IteratorConstructor})
      .Iterator as IteratorConstructor;
    return IteratorCtor.from(iter);
  }
  // Fallback to custom implementation
  return new IterWrapper(iter);
}
