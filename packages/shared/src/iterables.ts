import {assert} from './asserts.js';

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

// TODO(arv): Use ES2024 Iterable.from when available
// https://github.com/tc39/proposal-iterator-helpers

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

export function wrapIterable<T>(iter: Iterable<T>): IterWrapper<T> {
  return new IterWrapper(iter);
}

export function* mergeIterables<T>(
  iterables: Iterable<T>[],
  comparator: (l: T, r: T) => number,
  distinct = false,
): IterableIterator<T> {
  const iterators = iterables.map(i => i[Symbol.iterator]());
  try {
    const current = iterators.map(i => i.next());
    let lastYielded: T | undefined;
    while (current.some(c => !c.done)) {
      const min = current.reduce(
        (acc: [T, number] | undefined, c, i): [T, number] | undefined => {
          if (c.done) {
            return acc;
          }
          if (acc === undefined || comparator(c.value, acc[0]) < 0) {
            return [c.value, i];
          }
          return acc;
        },
        undefined,
      );

      assert(min !== undefined, 'min is undefined');
      current[min[1]] = iterators[min[1]].next();
      if (
        lastYielded !== undefined &&
        distinct &&
        comparator(lastYielded, min[0]) === 0
      ) {
        continue;
      }
      lastYielded = min[0];
      yield min[0];
    }
  } finally {
    for (const it of iterators) {
      it.return?.();
    }
  }
}
