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

export function genMapCached<T, U>(
  s: Iterable<T>,
  cb: (x: T) => U,
  finallyCb?: () => void | undefined,
) {
  const cache: U[] = [];
  return {
    *[Symbol.iterator]() {
      try {
        let i = 0;
        for (const x of s) {
          if (cache.length > i) {
            yield cache[i];
          } else {
            const value = cb(x);
            cache.push(value);
            yield value;
          }
          i++;
        }
      } finally {
        finallyCb?.();
      }
    },
  };
}

export function genFilterCached<S extends T, T>(
  s: Iterable<T>,
  f: (x: T) => x is S,
  finallyCb?: () => void | undefined,
) {
  const cache: boolean[] = [];
  return {
    *[Symbol.iterator]() {
      try {
        let i = 0;
        for (const x of s) {
          if (cache.length > i) {
            if (cache[i]) {
              yield x as S;
            }
          } else {
            const value = f(x);
            cache.push(value);
            if (value) {
              yield x as S;
            }
          }
          i++;
        }
      } finally {
        finallyCb?.();
      }
    },
  };
}

export function genFlatMapCached<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
  finallyCb?: () => void | undefined,
) {
  const cache: Iterable<U>[] = [];
  return {
    *[Symbol.iterator]() {
      try {
        let i = 0;
        for (const t of iter) {
          if (cache.length > i) {
            yield* cache[i];
          } else {
            const values = f(t, i);
            cache.push(values);
            yield* values;
          }
          i++;
        }
      } finally {
        finallyCb?.();
      }
    },
  };
}
