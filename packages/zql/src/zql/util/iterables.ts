export function genMap<T, U>(
  s: Iterable<T>,
  cb: (x: T) => U,
  finallyCb?: () => void | undefined,
) {
  return {
    *[Symbol.iterator]() {
      try {
        for (const x of s) {
          yield cb(x);
        }
      } finally {
        finallyCb?.();
      }
    },
  };
}

export function genCached<T>(
  s: Iterable<T>,
  finallyCb?: () => void | undefined,
) {
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
          if (lastIteratorResult?.done) {
            return;
          }

          cache.push(lastIteratorResult.value);
        }
      } finally {
        if (!lastIteratorResult?.done) {
          innerIterator.return?.();
        }
        finallyCb?.();
      }
    },
  };
}

export function genFilter<S extends T, T>(
  s: Iterable<T>,
  f: (x: T) => x is S,
  finallyCb?: () => void | undefined,
): {
  [Symbol.iterator](): Generator<S, void, unknown>;
};
export function genFilter<T>(
  s: Iterable<T>,
  f: (x: T) => boolean,
  finallyCb?: () => void | undefined,
): {
  [Symbol.iterator](): Generator<T, void, unknown>;
};
export function genFilter<S extends T, T>(
  s: Iterable<T>,
  cb: (x: T) => boolean,
  finallyCb?: () => void | undefined,
): {
  [Symbol.iterator](): Generator<S, void, unknown>;
} {
  return {
    *[Symbol.iterator]() {
      try {
        for (const x of s) {
          if (cb(x)) {
            yield x as S;
          }
        }
      } finally {
        finallyCb?.();
      }
    },
  };
}

export function genFlatMap<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
  finallyCb?: () => void | undefined,
) {
  return {
    *[Symbol.iterator]() {
      try {
        let index = 0;
        for (const t of iter) {
          yield* f(t, index++);
        }
      } finally {
        finallyCb?.();
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
