import {compareUTF8} from 'compare-utf8';
import {assertBoolean, assertNumber, assertString} from 'shared/src/asserts.js';
import type {Ordering} from '../ast2/ast.js';

/**
 * The data types that Zero can represent are limited by two things:
 *
 * 1. The underlying Replicache sync layer currently can only represent JSON
 *    types. This could possibly be expanded in the future, but we do want to be
 *    careful of adding encoding overhead. By using JSON, we are taking
 *    advantage of IndexedDB’s fast native JSValue [de]serialization which has
 *    historically been a perf advantage for us.
 *
 * 2. IDs in Zero need to be comparable because we use them for sorting and row
 *    identity. We could expand the set of allowed value types (to include,
 *    i.e., Objects) but we would then need to restrict IDs to only comparable
 *    types.
 *
 * These two facts leave us with the following allowed types. Zero's replication
 * layer must convert other types into these for tables to be used with Zero.
 *
 * For developer convenience we also allow `undefined`, which we treat
 * equivalently to `null`.
 *
 * TODO: This file needs unit tests.
 */
export type Value = undefined | null | boolean | number | string;

/**
 * A Row is represented as a JS Object.
 *
 * We do everything in IVM as loosely typed values because these pipelines are
 * going to be constructed at runtime by other code, so type-safety can't buy us
 * anything.
 *
 * Also since the calling code on the client ultimately wants objects to work
 * with we end up with a lot less copies by using objects throughout.
 */
export type Row = Record<string, Value>;

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
  a = normalizeUndefined(a);
  b = normalizeUndefined(b);

  if (a === b) {
    return 0;
  }
  if (a === null) {
    return -1;
  }
  if (b === null) {
    return 1;
  }
  if (typeof a === 'boolean') {
    assertBoolean(b);
    return a ? 1 : -1;
  }
  if (typeof a === 'number') {
    assertNumber(b);
    return a - b;
  }
  if (typeof a === 'string') {
    assertString(b);
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
  throw new Error(`Unsupported type: ${a}`);
}

/**
 * We allow undefined to be passed for the convenience of developers, but we
 * treat it equivalently to null. It's better for perf to not create an copy
 * of input values, so we just normalize at use when necessary.
 */
export function normalizeUndefined(v: Value): Exclude<Value, undefined> {
  if (v === undefined) {
    return null;
  }
  return v;
}

export type Comparator = (r1: Row, r2: Row) => number;

export function makeComparator(order: Ordering): Comparator {
  return (a, b) => {
    for (const [field, direction] of order) {
      const comp = compareValues(a[field], b[field]);
      if (comp !== 0) {
        return direction === 'asc' ? comp : -comp;
      }
    }
    return 0;
  };
}
