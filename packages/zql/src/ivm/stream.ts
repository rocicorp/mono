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
/**
 * Runs `fn` with `stream` and closes it afterwards, on the normal path and on
 * a throw.
 *
 * `close()` is what `for...of` used to do implicitly on abrupt completion, so
 * anything that takes ownership of a stream for the length of a scope should
 * go through here rather than hand-writing try/finally. If `fn` throws, that
 * error is the one raised: a failure to close is a symptom, not the cause.
 */
export function withPull<T, R>(
  stream: PullStream<T>,
  fn: (stream: PullStream<T>) => R,
): R {
  let result: R;
  try {
    result = fn(stream);
  } catch (e) {
    try {
      stream.close();
    } catch {
      // Preserve the original failure.
    }
    throw e;
  }
  stream.close();
  return result;
}

/**
 * Calls `fn` for each value, closing the stream when the scan ends, when `fn`
 * returns `'break'`, or when `fn` throws.
 *
 * The `'break'` sentinel is the point: a `break` out of a hand-written loop is
 * exactly the abrupt completion that `for...of` used to close for, and the
 * case most likely to be written without a `finally`.
 */
export function forEachPull<T>(
  stream: PullStream<T>,
  fn: (value: T) => void | 'break',
): void {
  withPull(stream, s => {
    for (let v = s.next(); v !== undefined; v = s.next()) {
      if (fn(v) === 'break') {
        return;
      }
    }
  });
}

export function filterPull<T>(
  stream: PullStream<T>,
  keep: (value: T) => boolean,
): PullStream<T> {
  return {
    next() {
      for (;;) {
        const v = stream.next();
        if (v === undefined) {
          return v;
        }
        if (callOrClose(stream, keep, v)) {
          return v;
        }
      }
    },
    close: () => stream.close(),
  };
}

/**
 * Applies a caller-supplied callback, closing `stream` if it throws. A
 * combinator has no scope to close in, and the exception propagates past the
 * caller's own `close()`, so the release has to happen here.
 */
function callOrClose<T, R>(
  stream: PullStream<unknown>,
  fn: (value: T) => R,
  value: T,
): R {
  try {
    return fn(value);
  } catch (e) {
    stream.close();
    throw e;
  }
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
      if (!callOrClose(stream, keep, v)) {
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
      return v === undefined ? undefined : callOrClose(stream, map, v);
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
        // As the generators did: an exception records no state. The source
        // still has to be released -- setting `done` makes the caller's
        // close() skip it, which is the bug FilterStartPull had.
        done = true;
        stream.close();
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

/**
 * At most the first value, then closes the source.
 *
 * The pull-protocol form of `once()`. A primary-key equality can match one
 * row, so the index walk must stop there rather than run to the end of the
 * scan rejecting rows one at a time.
 */
export function firstPull<T>(stream: PullStream<T>): PullStream<T> {
  let done = false;
  return {
    next() {
      if (done) {
        return undefined;
      }
      done = true;
      const v = stream.next();
      stream.close();
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
  forEachPull(stream, v => {
    out.push(map(v));
  });
  return out;
}

/** Reads a pull stream to completion. For tests and for `Catch`. */
export function drainPull<T>(stream: PullStream<T>): T[] {
  const out: T[] = [];
  forEachPull(stream, v => {
    out.push(v);
  });
  return out;
}

export function pullOf<T>(items: readonly T[]): PullStream<T> {
  return items.length === 0 ? emptyPullStream<T>() : new ArrayPull(items);
}

/**
 * The empty push stream. `push` returns `Stream<'yield'>` purely so a slow push
 * can suspend; the operators that never suspend on their own share this one
 * frozen array rather than each allocating a generator.
 */
export const EMPTY_YIELDS: Stream<'yield'> = Object.freeze([]);

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
