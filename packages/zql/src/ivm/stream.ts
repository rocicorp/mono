/**
 * streams are lazy forward-only iterables.
 * Once a stream reaches the end it can't be restarted.
 * They are iterable, not iterator, so that they can be used in for-each,
 * and so that we know when consumer has stopped iterating the stream. This allows us
 * to clean up resources like sql statements.
 */
export type Stream<T> = Iterable<T>;

export function* take<T>(stream: Stream<T>, limit: number): Stream<T> {
  if (limit < 1) {
    return;
  }
  let count = 0;
  for (const v of stream) {
    yield v;
    if (++count === limit) {
      break;
    }
  }
}

/**
 * Returns the first element of `stream`, or `undefined` if it is empty.
 *
 * NOTE: operator `fetch` streams are `Stream<Node | 'yield'>` -- they
 * interleave 'yield' sentinels for cooperative multitasking. Calling this on
 * one answers "was the stream non-empty?", NOT "did it produce a row": a
 * sentinel is returned as if it were a `Node`. Wrap such a stream in
 * `skipYields()` first.
 */
export function first<T>(stream: Stream<T>): T | undefined {
  const it = stream[Symbol.iterator]();
  const {value} = it.next();
  it.return?.();
  return value;
}

export function consume<T>(stream: Stream<T>): void {
  // Required to prevent some minifiers (e.g. Terser) from removing this empty loop
  for (const _ of stream);
}

export function drainGenerator<Yield, Return>(
  gen: Generator<Yield, Return, unknown>,
): Return {
  let result = gen.next();
  while (!result.done) {
    result = gen.next();
  }
  return result.value;
}
