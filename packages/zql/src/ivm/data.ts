import {compareUTF8} from 'compare-utf8';
import {
  assertBoolean,
  assertNumber,
  assertString,
} from '../../../shared/src/asserts.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {Stream} from './stream.ts';

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
  relationships: Record<string, () => Stream<Node | 'yield'>>;
};

/**
 * Fast-path string comparison that handles the common ASCII case
 * without calling into compareUTF8. Falls back to compareUTF8 for
 * non-ASCII characters.
 *
 * Returns a sign-only contract: negative if a < b, 0 if equal, positive
 * if a > b. Callers must NOT rely on the magnitude of the return value.
 */
export function compareStringUTF8Fast(a: string, b: string): number {
  if (a === b) return 0;
  const len = a.length < b.length ? a.length : b.length;
  for (let i = 0; i < len; i++) {
    const ac = a.charCodeAt(i);
    const bc = b.charCodeAt(i);
    if (ac !== bc) {
      if (ac < 128 && bc < 128) return ac - bc;
      return compareUTF8(a, b);
    }
  }
  return a.length - b.length;
}

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
  // String check before null: strings are the most common value type in
  // practice, so testing them first reduces branch mispredictions. The
  // null sub-check inside handles the string-vs-null comparison without
  // falling through to the generic null checks below.
  if (typeof a === 'string') {
    if (b === null) return 1;
    assertString(b);
    return compareStringUTF8Fast(a, b);
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
  throw new Error(`Unsupported type: ${a}`);
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
  if (order.length === 1) {
    const key = order[0][0];
    const dir = order[0][1];
    if (dir === 'asc') {
      return reverse
        ? (a, b) => -compareValues(a[key], b[key])
        : (a, b) => compareValues(a[key], b[key]);
    }
    return reverse
      ? (a, b) => compareValues(a[key], b[key])
      : (a, b) => -compareValues(a[key], b[key]);
  }
  return (a, b) => {
    // Skip destructuring here since it is hot code.
    for (const ord of order) {
      const field = ord[0];
      const comp = compareValues(a[field], b[field]);
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
    for (const node of stream()) {
      drainStreams(node);
    }
  }
}
