import {compareUTF8} from 'compare-utf8';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {forEachPull} from './stream.ts';
import type {PullStream} from './stream.ts';

/**
 * What a relationship closure returns. The pull protocol only -- a relationship
 * read is the hottest path in hydration, and accepting an iterable here would
 * let a producer put the per-row result object back.
 */
export type RelationshipStream = PullStream<Node | 'yield'>;

/**
 * A row flowing through the pipeline, plus its relationships.
 * Relationships are generated lazily as read.
 */
export type Node = {
  row: Row;
  /**
   * Relationships are generated lazily as read.
   * The stream may contain 'yield' to indicate the operator has yielded control.
   * See {@linkcode Operator.fetch} for more details about yields.
   */
  relationships: Record<string, () => RelationshipStream>;
};

/**
 * Compare two values. The values must be of the same type. This function
 * throws at runtime if the types differ.
 *
 * Note, this function considers `null === null` and
 * `undefined === undefined`. This is different than SQL. In join code,
 * null must be treated separately.
 *
 * See: https://github.com/rocicorp/mono/pull/2116/files#r1704811479
 *
 * @returns < 0 if a < b, 0 if a === b, > 0 if a > b
 */
export function compareValues(a: Value, b: Value): number {
  a = a ?? null;
  b = b ?? null;

  if (a === b) {
    return 0;
  }

  if (typeof a === 'string' && typeof b === 'string') {
    // We compare all strings in Zero as UTF-8. This is the default on SQLite
    // and we need to match it. See:
    // https://blog.replicache.dev/blog/replicache-11-adventures-in-text-encoding.
    //
    // TODO: We could change this since SQLite supports UTF-16. Microbenchmark
    // to see if there's a big win.
    //
    // https://www.sqlite.org/c3ref/create_collation.html
    return compareUTF8(a, b);
  }

  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  if (typeof a === 'boolean' && typeof b === 'boolean') {
    return a ? 1 : -1;
  }

  // check with null after since it is less likely to be the common case and we
  // want to avoid the extra checks in that case
  if (a === null) {
    return -1;
  }
  if (b === null) {
    return 1;
  }

  if (typeof a !== typeof b) {
    throw new Error(
      `Cannot compare values of different types: ${typeof a} and ${typeof b}`,
    );
  }

  throw new Error(`Unsupported type: ${typeof a}`);
}

export type NormalizedValue = Exclude<Value, undefined>;

/**
 * We allow undefined to be passed for the convenience of developers, but we
 * treat it equivalently to null. It's better for perf to not create an copy
 * of input values, so we just normalize at use when necessary.
 */
export function normalizeUndefined(v: Value): NormalizedValue {
  return v ?? null;
}

export type Comparator = (r1: Row, r2: Row) => number;

export function makeComparator(order: Ordering, reverse?: boolean): Comparator {
  // A single ascending field is the common shape -- it is what every source
  // ordered by a single-column primary key gets -- and specializing it drops
  // the loop, the direction test and the reverse test from the hottest
  // function in hydration.
  if (order.length === 1 && order[0][1] === 'asc' && !reverse) {
    const field = order[0][0];
    return (a, b) => compareValues(a[field], b[field]);
  }

  const length = order.length;
  return (a, b) => {
    // Skip destructuring here since it is hot code. An indexed loop rather
    // than `for...of` for the same reason: Hermes allocates an iterator and
    // calls `next()` per element, which costs more than the comparison for an
    // ordering this short.
    for (let i = 0; i < length; i++) {
      const ord = order[i];
      const comp = compareValues(a[ord[0]], b[ord[0]]);
      if (comp !== 0) {
        const result = ord[1] === 'asc' ? comp : -comp;
        return reverse ? -result : result;
      }
    }
    return 0;
  };
}

/**
 * Determine if two values are equal. Note that unlike compareValues() above,
 * this function treats `null` as unequal to itself (and same for `undefined`).
 * This is required to make joins work correctly, but may not be the right
 * semantic for your application.
 */
export function valuesEqual(a: Value, b: Value): boolean {
  // oxlint-disable-next-line eqeqeq
  if (a == null || b == null) {
    return false;
  }
  return a === b;
}

export function drainStreams(node: Node | 'yield') {
  if (node === 'yield') {
    return;
  }
  for (const stream of Object.values(node.relationships)) {
    const children = stream();
    forEachPull(children, node => {
      drainStreams(node);
    });
  }
}
