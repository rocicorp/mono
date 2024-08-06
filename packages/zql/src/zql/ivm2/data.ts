import {compareUTF8} from 'compare-utf8';
import {
  assert,
  assertBoolean,
  assertNumber,
  assertString,
} from 'shared/src/asserts.js';
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
 * TODO: Add support for undefined to support optimistic mutations on client
 * that omit fields.
 * TODO: This file needs unit tests.
 */
export type Value = null | boolean | number | string;

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
 * Zero requires that all synced tables have a unique primary key. Primary keys
 * composed of multiple columns are supported (and required, due to junction
 * tables). Rows from the same source having the same ID are considered to be
 * the same row, without comparing other fields.
 *
 * The code that vends these IDs must return the columns in some consistent
 * order over the lifetime of the process. This avoid the sort having to be done
 * at the time of comparison.
 *
 * TODO: Microbenchmark this approach against the version where we put an ID
 * symbol on each object. Benchmark maintaining some sorted list of rows.
 */
export type ID = Value[];

/**
 * For performance reasons (to avoid expanding every single row with a new
 * object/array having the ID fields) we provide access to the identity of a row
 * externally, with a separate function when necessary.
 */
export type GetID = (row: Row) => ID;

/**
 * Check two IDs for equality. This function considers any two IDs with the same
 * components equal, even if they are from different tables.
 */
export function idEquals(id1: ID, id2: ID): boolean {
  if (id1.length !== id2.length) {
    return false;
  }
  for (let i = 0; i < id1.length; i++) {
    if (id1[i] !== id2[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Compare two values. The values must be of the same type. This function
 * throws at runtime if the types differ.
 * @returns < 0 if a < b, 0 if a === b, > 0 if a > b
 */
export function compareValues(a: Value, b: Value): number {
  // TODO: should `null === null`? Should `undefined === undefined`?
  // See: https://github.com/rocicorp/mono/pull/2116/files#r1704811479
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
 * Compare two IDs. This function throws at runtime if the IDs have different
 * lengths, or if the types of the components don't match.
 * @returns < 0 if a < b, 0 if a === b, > 0 if a > b
 */
export function compareIDs(a: ID, b: ID): number {
  assert(a.length === b.length, 'Mismatched ID lengths');
  for (let i = 0; i < a.length; i++) {
    const cmp = compareValues(a[i], b[i]);
    if (cmp !== 0) {
      return cmp;
    }
  }
  return 0;
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
