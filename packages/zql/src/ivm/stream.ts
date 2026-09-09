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
/**
 * A stream read by calling `next()` until it returns `undefined`; `close()`
 * releases resources if the consumer stops early.
 *
 * Deliberately NOT `Iterable`. If a pull stream could be `for...of`'d, every
 * unconverted consumer would keep silently paying the iterator protocol -- a
 * `{done, value}` object per row -- which is the cost this protocol exists to
 * remove. There is no adapter back to an iterable: a consumer that wants
 * values calls `next()`.
 */
export interface PullStream<T> {
  next(): T | undefined;
  close(): void;
}

const EMPTY: PullStream<never> = {
  next: () => undefined,
  close: () => {},
};
/**
 * Keeps the values `keep` accepts.
 *
 * These three cover most of what operators do to a stream, replacing a
 * per-operator class each -- all the same pull/check/return shape. A node
 * stream carrying 'yield' markers passes them through by accepting them in
 * the predicate.
 */
export function filterPull<T>(
  stream: PullStream<T>,
  keep: (value: T) => boolean,
): PullStream<T> {
  return {
    next() {
      for (;;) {
        const v = stream.next();
        if (v === undefined || keep(v)) {
          return v;
        }
      }
    },
    close: () => stream.close(),
  };
}

/** Ends the stream at the first value `keep` rejects, closing the source. */
export function takeWhilePull<T>(
  stream: PullStream<T>,
  keep: (value: T) => boolean,
): PullStream<T> {
  let done = false;
  return {
    next() {
      if (done) {
        return undefined;
      }
      const v = stream.next();
      if (v === undefined) {
        done = true;
        return undefined;
      }
      if (!keep(v)) {
        done = true;
        stream.close();
        return undefined;
      }
      return v;
    },
    close() {
      if (!done) {
        done = true;
        stream.close();
      }
    },
  };
}

/** Applies `map` to each value. */
export function mapPull<T, U>(
  stream: PullStream<T>,
  map: (value: T) => U,
): PullStream<U> {
  return {
    next() {
      const v = stream.next();
      return v === undefined ? undefined : map(v);
    },
    close: () => stream.close(),
  };
}

/**
 * Emits at most `limit` values, calling `onValue` for each and `onComplete`
 * once the scan finishes.
 *
 * `Take` and `Cap` both hydrate this way: read up to a limit, record what was
 * seen, and treat a consumer that stops early as a bug -- their initial fetch
 * must run to completion or the state they persist is wrong. Closing early
 * still records, then raises, exactly as their generators' `finally` did.
 */
export function limitedScan<T>(
  stream: PullStream<T>,
  limit: number,
  isValue: (v: T) => boolean,
  onValue: (v: T) => void,
  onComplete: () => void,
  onEarlyClose: () => void,
): PullStream<T> {
  let seen = 0;
  let done = false;
  const complete = () => {
    done = true;
    stream.close();
    onComplete();
  };
  return {
    next() {
      if (done) {
        return undefined;
      }
      if (seen === limit) {
        complete();
        return undefined;
      }
      let v: T | undefined;
      try {
        v = stream.next();
      } catch (e) {
        // As the generators did: an exception records no state.
        done = true;
        throw e;
      }
      if (v === undefined) {
        complete();
        return undefined;
      }
      if (isValue(v)) {
        onValue(v);
        seen++;
      }
      return v;
    },
    close() {
      if (!done) {
        complete();
        onEarlyClose();
      }
    },
  };
}

/** A pull stream over a fixed list; for producers that already have an array. */
class ArrayPull<T> implements PullStream<T> {
  readonly #items: readonly T[];
  #i = 0;
  constructor(items: readonly T[]) {
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
export class LazyPullStream<T> implements PullStream<T> {
  #start: (() => PullStream<T>) | undefined;
  #inner: PullStream<T> | undefined;

  constructor(start: () => PullStream<T>) {
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
