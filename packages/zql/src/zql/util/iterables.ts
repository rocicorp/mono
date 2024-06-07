import {assert} from 'shared/src/asserts.js';

export function gen<T>(generator: () => Iterator<T>): Iterable<T> {
  return {
    [Symbol.iterator]() {
      return generator();
    },
  };
}

export function genConcat<T>(iterables: Iterable<T>[]): Iterable<T> {
  return {
    *[Symbol.iterator]() {
      for (const iter of iterables) {
        yield* iter;
      }
    },
  };
}

export function genMap<T, U>(
  iterable: Iterable<T>,
  cb: (x: T) => U,
): Iterable<U> {
  return {
    *[Symbol.iterator]() {
      for (const x of iterable) {
        yield cb(x);
      }
    },
  };
}

export function genCached<T>(s: Iterable<T>): Iterable<T> {
  const cache: T[] = [];

  // we have to start it outside so it doesn't get re-started
  // on later calls to the cache which have a cache-miss.
  const innerIterator = s[Symbol.iterator]();
  let lastIteratorResult: IteratorResult<T> | undefined;

  return {
    *[Symbol.iterator]() {
      try {
        let i = 0;

        for (;;) {
          if (i < cache.length) {
            yield cache[i];
            ++i;
            continue;
          }

          lastIteratorResult = innerIterator.next();
          if (lastIteratorResult.done) {
            return;
          }

          cache.push(lastIteratorResult.value);
        }
      } finally {
        if (!lastIteratorResult?.done) {
          innerIterator.return?.();
        }
      }
    },
  };
}

export function genFilter<S extends T, T>(
  s: Iterable<T>,
  f: (x: T) => x is S,
): Iterable<S>;
export function genFilter<T>(s: Iterable<T>, f: (x: T) => boolean): Iterable<T>;
export function genFilter<S extends T, T>(
  s: Iterable<T>,
  cb: (x: T) => boolean,
): Iterable<S> {
  return {
    *[Symbol.iterator]() {
      for (const x of s) {
        if (cb(x)) {
          yield x as S;
        }
      }
    },
  };
}

export function genFlatMap<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
): Iterable<U> {
  return {
    *[Symbol.iterator]() {
      let index = 0;
      for (const t of iter) {
        yield* f(t, index++);
      }
    },
  };
}

export function* mapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => U,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield f(t, index++);
  }
}

export function* iterInOrder<T>(
  iterables: Iterable<T>[],
  comparator: (l: T, r: T) => number,
): IterableIterator<T> {
  const iterators = iterables.map(i => i[Symbol.iterator]());
  try {
    const current = iterators.map(i => i.next());
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
      yield min[0];
      current[min[1]] = iterators[min[1]].next();
    }
  } finally {
    for (const it of iterators) {
      it.return?.();
    }
  }
}
