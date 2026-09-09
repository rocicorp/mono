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

/**
 * The pull-function protocol.
 *
 * `next()` returns the next value directly, or `undefined` once the stream is
 * exhausted; `close()` releases resources if the consumer stops early. This is
 * the same pull model as {@link Stream} -- the consumer drives, so 'yield' and
 * stream merging work unchanged -- minus the iterator protocol's costs: no
 * `{done, value}` result object per value, and one object per stream rather
 * than per stage. A four-deep pipeline measured ~26% faster than hand-written
 * iterators on both Hermes and V8, within a few percent of push callbacks.
 *
 * `undefined` is the end marker, so a PullStream cannot carry `undefined` as a
 * value. Nodes are objects and 'yield' is a string; neither can be.
 */
export interface PullStream<T> {
  next(): T | undefined;
  close(): void;
}

/**
 * Base for pull streams.
 *
 * Deliberately NOT `Iterable`. If a pull stream could be `for...of`'d, every
 * unconverted consumer would keep silently paying the iterator protocol -- a
 * `{done, value}` object per row -- which is the cost this protocol exists to
 * remove. There is deliberately no adapter back to an iterable: a consumer
 * that wants values calls `next()`.
 */
export abstract class PullStreamBase<T> implements PullStream<T> {
  abstract next(): T | undefined;
  abstract close(): void;
}

class EmptyPullStream<T> extends PullStreamBase<T> {
  next(): T | undefined {
    return undefined;
  }
  close(): void {}
}
const EMPTY: PullStream<never> = new EmptyPullStream<never>();
/** A pull stream over a fixed list; for producers that already have an array. */
class ArrayPull<T> extends PullStreamBase<T> {
  readonly #items: readonly T[];
  #i = 0;
  constructor(items: readonly T[]) {
    super();
    this.#items = items;
  }
  next(): T | undefined {
    return this.#i < this.#items.length ? this.#items[this.#i++] : undefined;
  }
  close(): void {
    this.#i = this.#items.length;
  }
}

/**
 * Reads a pull stream to completion, applying `map` to each value as it
 * arrives.
 *
 * The interleaving matters: `Array.from(iter, fn)` called `fn` between pulls,
 * and callers such as `Catch` rely on that -- expanding a node's
 * relationships triggers child fetches, so draining first and mapping second
 * reorders those fetches relative to the parent scan.
 */
export function drainPullMap<T, U>(
  stream: PullStream<T>,
  map: (value: T) => U,
): U[] {
  const out: U[] = [];
  for (let v = stream.next(); v !== undefined; v = stream.next()) {
    out.push(map(v));
  }
  return out;
}

/** Reads a pull stream to completion. For tests and for `Catch`. */
export function drainPull<T>(stream: PullStream<T>): T[] {
  const out: T[] = [];
  for (let v = stream.next(); v !== undefined; v = stream.next()) {
    out.push(v);
  }
  return out;
}

export function pullOf<T>(items: readonly T[]): PullStream<T> {
  return items.length === 0 ? emptyPullStream<T>() : new ArrayPull(items);
}

export function emptyPullStream<T>(): PullStream<T> {
  return EMPTY;
}

/**
 * A pull stream whose work starts on the first `next()`, as a generator body
 * does. Lets a source defer reading mutable state until iteration actually
 * begins.
 */
export class LazyPullStream<T> extends PullStreamBase<T> {
  #start: (() => PullStream<T>) | undefined;
  #inner: PullStream<T> | undefined;

  constructor(start: () => PullStream<T>) {
    super();
    this.#start = start;
  }

  next(): T | undefined {
    let inner = this.#inner;
    if (inner === undefined) {
      const start = this.#start;
      if (start === undefined) {
        return undefined;
      }
      this.#start = undefined;
      inner = this.#inner = start();
    }
    return inner.next();
  }

  close(): void {
    this.#start = undefined;
    const inner = this.#inner;
    this.#inner = undefined;
    inner?.close();
  }
}
