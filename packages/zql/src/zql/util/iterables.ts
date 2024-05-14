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
        if (finallyCb !== undefined) {
          finallyCb();
        }
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
        if (finallyCb !== undefined) {
          finallyCb();
        }
      }
    },
  };
}

/**
 * idk why `iter` needs to be returned from a lambda below.
 * Something breaks in `reduce-operator` if we change this.
 * Seems like something is wrong in `reduce` then.
 */
export function genFlatMap<T, U>(
  iter: () => Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
  finallyCb?: () => void | undefined,
) {
  return {
    *[Symbol.iterator]() {
      try {
        let index = 0;
        for (const t of iter()) {
          yield* f(t, index++);
        }
      } finally {
        if (finallyCb !== undefined) {
          finallyCb();
        }
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
