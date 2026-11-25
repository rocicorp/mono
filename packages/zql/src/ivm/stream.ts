import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {Node} from './data.ts';

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
/**
 * `stream`, then the stream `rest` returns.
 *
 * A push through a chain of generators costs a generator per operator, though
 * a push rarely yields. So an operator does its push right away and returns
 * `emptyArray` when all of it is done. It returns a stream only when a push it
 * makes downstream, or a fetch it reads, yields. That stream holds the yields
 * and, behind them, the rest of the operator's work.
 *
 * So nothing may follow a downstream push directly. What follows it goes in
 * `rest`:
 *
 *     return andThen(output.push(a, this), () => output.push(b, this));
 *
 * `rest` runs right away when the first push is done, and otherwise once the
 * stream of the first push has been read. Only `emptyArray` says that a push
 * is done: any other stream is read first, an empty one too, which is correct
 * and only slower.
 *
 * The stream a push returns must be read: by the time it is returned, the
 * operator may have done part of its work and hold state that only the rest
 * of the stream clears.
 *
 * This is for a fixed number of steps. A loop goes through `eachStream`.
 */
export function andThen(
  stream: Stream<'yield'>,
  rest: () => Stream<'yield'>,
): Stream<'yield'> {
  return stream === emptyArray ? rest() : chain(stream, rest);
}

function* chain(
  stream: Stream<'yield'>,
  rest: () => Stream<'yield'>,
): Stream<'yield'> {
  yield* stream;
  const s = rest();
  if (s !== emptyArray) {
    yield* s;
  }
}

/**
 * Runs `cleanup` once `stream` is done, however it ends: `try { yield* stream
 * } finally { cleanup() }` without the generator when there is nothing to
 * wait for. The caller runs `cleanup` itself if getting the stream throws.
 */
export function andFinally(
  stream: Stream<'yield'>,
  cleanup: () => void,
): Stream<'yield'> {
  if (stream === emptyArray) {
    cleanup();
    return emptyArray;
  }
  return finallyAfter(stream, cleanup);
}

function* finallyAfter(
  stream: Stream<'yield'>,
  cleanup: () => void,
): Stream<'yield'> {
  try {
    yield* stream;
  } finally {
    cleanup();
  }
}

/**
 * The streams `next` returns, one after the other, until it returns
 * `undefined`: a loop whose body makes one push per round, without a generator
 * for as long as every push is done.
 *
 * Written with `andThen`, such a loop would nest one generator per round that
 * is not done, and a push to several thousand parents through an output that
 * returns a generator would overflow the stack. Here, the first stream that is
 * not done hands the rest of the loop to a single generator.
 *
 * `open` is an iterator that `next` reads. It is closed if the loop stops
 * before `next` returns `undefined`, because `next` or a stream threw or the
 * consumer stopped reading, as a `for...of` over that iterator would have
 * closed it.
 */
export function eachStream(
  next: () => Stream<'yield'> | undefined,
  open?: Iterator<unknown>,
): Stream<'yield'> {
  for (;;) {
    let s: Stream<'yield'> | undefined;
    try {
      s = next();
    } catch (e) {
      closeAfterError(open);
      throw e;
    }
    if (s === undefined) {
      return emptyArray;
    }
    if (s !== emptyArray) {
      return restOfEach(s, next, open);
    }
  }
}

function* restOfEach(
  first: Stream<'yield'>,
  next: () => Stream<'yield'> | undefined,
  open: Iterator<unknown> | undefined,
): Stream<'yield'> {
  let ended = false;
  try {
    yield* first;
    for (;;) {
      const s = next();
      if (s === undefined) {
        break;
      }
      if (s !== emptyArray) {
        yield* s;
      }
    }
    ended = true;
  } catch (e) {
    ended = true;
    closeAfterError(open);
    throw e;
  } finally {
    // The consumer stopped reading.
    if (!ended) {
      open?.return?.();
    }
  }
}

/**
 * Closes `it` on the way out with an error. An error from closing it is
 * dropped, as a `for...of` drops it: the first error is the one to report.
 */
function closeAfterError(it: Iterator<unknown> | undefined): void {
  try {
    it?.return?.();
  } catch {
    // The first error wins.
  }
}

/** One 'yield'. Returned for a round of `eachStream` that was reached by one. */
export const yieldOnce: Stream<'yield'> = Object.freeze(['yield'] as const);

// The per-row steps of a fetch are iterators rather than generators, as
// `SkipYieldsStream` is: resuming a generator costs several times what a call
// to `next()` does, and a fetch pays it per row for each operator the row
// passes through. Each one is both the iterable and the iterator, passes
// 'yield' through, hands on the inner iterator's result object where the value
// is unchanged, and closes the inner iterator when it stops early or its
// callback throws, as a generator's `for...of` would.

type NodeResult = IteratorResult<Node | 'yield'>;

export const doneResult: IteratorReturnResult<undefined> = Object.freeze({
  value: undefined,
  done: true,
});

class MappedNodes implements IterableIterator<Node | 'yield'> {
  readonly #it: Iterator<Node | 'yield'>;
  readonly #fn: (node: Node) => Node;
  #done = false;

  constructor(it: Iterator<Node | 'yield'>, fn: (node: Node) => Node) {
    this.#it = it;
    this.#fn = fn;
  }

  [Symbol.iterator](): this {
    return this;
  }

  next(): NodeResult {
    if (this.#done) {
      return doneResult;
    }
    let r: NodeResult;
    try {
      r = this.#it.next();
    } catch (e) {
      // A fetch that threw is over, as a generator's is.
      this.#done = true;
      throw e;
    }
    if (r.done) {
      this.#done = true;
      return r;
    }
    if (r.value === 'yield') {
      return r;
    }
    let node: Node;
    try {
      node = this.#fn(r.value);
    } catch (e) {
      this.#done = true;
      closeAfterError(this.#it);
      throw e;
    }
    return {value: node, done: false};
  }

  return(): NodeResult {
    if (!this.#done) {
      this.#done = true;
      this.#it.return?.();
    }
    return doneResult;
  }
}

/** Yields `fn(node)` for each node of `stream`. */
export function mapNodes(
  stream: Stream<Node | 'yield'>,
  fn: (node: Node) => Node,
): Stream<Node | 'yield'> {
  return new MappedNodes(stream[Symbol.iterator](), fn);
}

class TappedNodes implements IterableIterator<Node | 'yield'> {
  readonly #it: Iterator<Node | 'yield'>;
  readonly #fn: (node: Node) => void;
  #done = false;

  constructor(it: Iterator<Node | 'yield'>, fn: (node: Node) => void) {
    this.#it = it;
    this.#fn = fn;
  }

  [Symbol.iterator](): this {
    return this;
  }

  next(): NodeResult {
    if (this.#done) {
      return doneResult;
    }
    let r: NodeResult;
    try {
      r = this.#it.next();
    } catch (e) {
      // A fetch that threw is over, as a generator's is.
      this.#done = true;
      throw e;
    }
    if (r.done) {
      this.#done = true;
    } else if (r.value !== 'yield') {
      try {
        this.#fn(r.value);
      } catch (e) {
        this.#done = true;
        closeAfterError(this.#it);
        throw e;
      }
    }
    return r;
  }

  return(): NodeResult {
    if (!this.#done) {
      this.#done = true;
      this.#it.return?.();
    }
    return doneResult;
  }
}

/** Yields the nodes of `stream`, calling `fn` on each as it goes by. */
export function tapNodes(
  stream: Stream<Node | 'yield'>,
  fn: (node: Node) => void,
): Stream<Node | 'yield'> {
  return new TappedNodes(stream[Symbol.iterator](), fn);
}

class NodesWhile implements IterableIterator<Node | 'yield'> {
  readonly #it: Iterator<Node | 'yield'>;
  readonly #fn: (node: Node) => boolean;
  #done = false;

  constructor(it: Iterator<Node | 'yield'>, fn: (node: Node) => boolean) {
    this.#it = it;
    this.#fn = fn;
  }

  [Symbol.iterator](): this {
    return this;
  }

  next(): NodeResult {
    if (this.#done) {
      return doneResult;
    }
    let r: NodeResult;
    try {
      r = this.#it.next();
    } catch (e) {
      // A fetch that threw is over, as a generator's is.
      this.#done = true;
      throw e;
    }
    if (r.done) {
      this.#done = true;
      return r;
    }
    if (r.value !== 'yield') {
      let passes: boolean;
      try {
        passes = this.#fn(r.value);
      } catch (e) {
        this.#done = true;
        closeAfterError(this.#it);
        throw e;
      }
      if (!passes) {
        return this.return();
      }
    }
    return r;
  }

  return(): NodeResult {
    if (!this.#done) {
      this.#done = true;
      this.#it.return?.();
    }
    return doneResult;
  }
}

/** Yields the nodes of `stream` up to the first for which `fn` is false. */
export function nodesWhile(
  stream: Stream<Node | 'yield'>,
  fn: (node: Node) => boolean,
): Stream<Node | 'yield'> {
  return new NodesWhile(stream[Symbol.iterator](), fn);
}
