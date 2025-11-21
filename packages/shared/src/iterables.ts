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
