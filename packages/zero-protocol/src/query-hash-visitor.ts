import {unreachable} from '../../shared/src/asserts.ts';
// These live in shared/src/xxhash32.ts, imported via hash.ts's re-export
// rather than directly: the direct zero-protocol -> shared/xxhash32.ts edge
// makes tsc 7.0.2 mis-evaluate Query-type relations in other programs --
// phantom TS2345s in zql's runnable-query-impl.ts under the packages/zero
// declaration build, and zero-client silently losing the type error that
// custom.test.ts pins with a ts-expect-error directive. Bisected cold-cache
// to that one import statement; the hash.ts route checks clean everywhere.
// Collapse to a direct import when TypeScript is upgraded past this.
import {
  avalanche32,
  PRIME32_1,
  PRIME32_5,
  round32,
} from '../../shared/src/hash.ts';
import type {Format} from '../../zero-types/src/format.ts';

/**
 * Hashes a normalized AST by visiting it, rather than by rendering it to JSON
 * and hashing the bytes. `hashOfAST` and `hashOfNameAndArgs` in
 * `query-hash.ts` are both built on this, so these digests are the query IDs
 * the client registers and the transformation hashes the server stores.
 *
 * The pipeline this replaced was
 * `h64(JSON.stringify(normalizeAST(ast))).toString(36)`, which materialized
 * the whole AST as a string, UTF-8 encoded it, and only then hashed it — three
 * passes over the data, two of which existed purely to produce bytes to hash.
 *
 * A visitor can fold values into the hash state as it walks, so nothing is
 * materialized at all. The primitives that make that pay off:
 *
 * - Two 32-bit accumulators held in module-level `let`s, so the "state" is
 *   plain numbers in registers rather than an object being threaded through
 *   the walk.
 * - xxHash32's own round and final mix, shared with `shared/src/hash.ts` via
 *   `shared/src/xxhash32.ts`. Only the ingestion differs: that one feeds them
 *   UTF-8 bytes, this one feeds them words straight out of the structure.
 * - Strings folded straight from `charCodeAt`, two UTF-16 units at a time, into
 *   one 32-bit word. No UTF-8 encoding, and half the loop iterations a
 *   byte-oriented hash would need over ASCII.
 *
 * These digests do not agree with xxHash32 over the old JSON encoding, and are
 * not meant to: nothing recomputes a hash produced elsewhere and compares it,
 * so the encoding is free to change as long as every writer moves together.
 * What it does have to give is distinct words for structurally distinct
 * queries — a 64-bit digest cannot promise that absolutely, but every field
 * and node kind must at least reach the mixer, and no two of them may be
 * written so as to be confusable. `query-hash-visitor.test.ts` holds that
 * line with a corpus and a per-field mutation test.
 *
 * The walk is deliberately specialized to the AST shape rather than written as
 * a generic JSON-value walk. That is not premature: a generic version was
 * measured alongside this one and came out ~2.4x slower, no better than the
 * `JSON.stringify` it was meant to replace, because `for...in` and the
 * megamorphic property loads cost about what the native stringify costs. Only
 * `visitValue` below stays generic, for the two places an AST holds arbitrary
 * JSON: a literal's value and a bound's row.
 */
import type {
  AST,
  Condition,
  CorrelatedSubquery,
  Ordering,
  System,
  ValuePosition,
} from './ast.ts';

// Two 32-bit lanes, combined into 64 bits at the end. Module-level so the walk
// mutates registers rather than allocating or threading state through it.
let h1 = 0;
let h2 = 0;

/**
 * Fold one 32-bit word into both lanes.
 *
 * Both lanes run xxHash32's own round; only their starting values differ, so
 * they diverge immediately and stay diverged. That is the same construction
 * `h64` uses to widen xxHash32 past 32 bits, just fed words taken from a
 * structure rather than bytes taken from a string.
 */
function mix(w: number): void {
  h1 = round32(h1, w);
  h2 = round32(h2, w);
}

/**
 * Fold a string, two UTF-16 units per word.
 *
 * The leading length keeps `['ab','c']` and `['a','bc']` apart. STR_MARK is
 * folded into that same word rather than mixed separately: it costs nothing,
 * and since every other count written by this file is a small array length, a
 * string can never fold like one.
 */
function mixString(s: string): void {
  const n = s.length;
  mix(n | STR_MARK);
  let i = 0;
  for (; i + 1 < n; i += 2) {
    mix(s.charCodeAt(i) | (s.charCodeAt(i + 1) << 16));
  }
  if (i < n) {
    mix(s.charCodeAt(i));
  }
}

// A DataView with an explicit byte order, not a Float64Array/Int32Array
// aliasing pair: typed arrays read the buffer in host byte order, and these
// digests are persisted, so a big-endian runtime would fold a float's words
// swapped and disagree with every little-endian peer.
const numView = new DataView(new ArrayBuffer(8));

/** Fold a number: one word for an int32, two for anything else. */
function mixNumber(n: number): void {
  if ((n | 0) === n) {
    mix(n);
  } else {
    numView.setFloat64(0, n, true);
    mix(numView.getInt32(0, true));
    mix(numView.getInt32(4, true));
  }
}

// Structural tags. Distinct constants so that, say, a string "1" and the
// number 1 cannot fold to the same state.
const TAG_NULL = 0x1001;
const TAG_FALSE = 0x1002;
const TAG_TRUE = 0x1003;
const TAG_INT = 0x1004;
const TAG_FLOAT = 0x1005;
const TAG_ARR = 0x1007;
const TAG_OBJ = 0x1008;
const TAG_END = 0x1009;
const TAG_UNDEF = 0x100a;

// Discriminators for the AST's own unions, kept distinct from the value tags
// above so a node kind can never fold like a value of some other type.
const TAG_LITERAL = 0x2001;
const TAG_COLUMN = 0x2002;
const TAG_STATIC = 0x2003;
const TAG_SIMPLE = 0x2004;
const TAG_SUBQUERY = 0x2005;
const TAG_AND = 0x2006;
const TAG_OR = 0x2007;
const TAG_FORMAT = 0x2008;
const TAG_NAME_ARGS = 0x2009;
// `System` is a closed set of three, so it folds as one word rather than as a
// string -- on an AST it appears once per correlated subquery, so this is on
// the walk's hot path. The switch below is exhaustive, so adding a member is a
// type error rather than a silent collision with an existing one.
const TAG_SYSTEM_PERMISSIONS = 0x200a;
const TAG_SYSTEM_CLIENT = 0x200b;
const TAG_SYSTEM_TEST = 0x200c;

// Set on a string's length word. High enough that no array length reaches it.
const STR_MARK = 0x40000000;

function reset(): void {
  h1 = PRIME32_5;
  h2 = PRIME32_1;
}

/** Collapse the two lanes into one 64-bit value, avalanching each first. */
function finalize(): string {
  return (
    avalanche32(h1).toString(36).padStart(7, '0') +
    avalanche32(h2).toString(36).padStart(7, '0')
  );
}

// ---------------------------------------------------------------------------
// The generic part: arbitrary JSON, for literal values and bound rows.
// ---------------------------------------------------------------------------

function visitValue(v: unknown): void {
  switch (typeof v) {
    case 'string':
      mixString(v);
      return;
    case 'number':
      // NaN and Infinity are null by the time they reach the wire, because
      // that is what JSON.stringify writes for them. Two queries identical on
      // the wire must not get different IDs.
      if (!Number.isFinite(v)) {
        mix(TAG_NULL);
        return;
      }
      mix((v | 0) === v ? TAG_INT : TAG_FLOAT);
      mixNumber(v);
      return;
    case 'boolean':
      mix(v ? TAG_TRUE : TAG_FALSE);
      return;
    case 'object': {
      if (v === null) {
        mix(TAG_NULL);
        return;
      }
      if (Array.isArray(v)) {
        mix(TAG_ARR);
        for (let i = 0; i < v.length; i++) {
          // An `undefined` element is `null` by the time it reaches the wire,
          // because that is what JSON.stringify makes of it. Dropping it
          // instead would fold `[undefined]` onto `[]`.
          const e: unknown = v[i];
          visitValue(e === undefined ? null : e);
        }
        mix(TAG_END);
        return;
      }
      mix(TAG_OBJ);
      // Sorted, so the digest is a function of the object's contents and not
      // of its key order. Rows and custom-query args round-trip through
      // Postgres JSONB, which reorders keys; an order-sensitive digest made
      // reloaded query records look changed. (The AST's own fields don't need
      // this: visitAST reads them in a fixed order by name.)
      const keys = Object.keys(v).sort();
      for (const k of keys) {
        const val = (v as Record<string, unknown>)[k];
        if (val === undefined) {
          continue; // as JSON.stringify does
        }
        mixString(k);
        visitValue(val);
      }
      mix(TAG_END);
      return;
    }
    default:
      // `undefined` and functions as an object's value: dropped, as
      // JSON.stringify drops them. Array elements are handled above.
      return;
  }
}

// ---------------------------------------------------------------------------
// The specialized part: the AST's own shape.
// ---------------------------------------------------------------------------

function visitOptionalString(s: string | undefined): void {
  if (s === undefined) {
    mix(TAG_UNDEF);
  } else {
    mixString(s);
  }
}

function mixOptionalSystem(system: System | undefined): void {
  if (system === undefined) {
    mix(TAG_UNDEF);
  } else {
    mixSystem(system);
  }
}

function mixSystem(system: System): void {
  switch (system) {
    case 'permissions':
      mix(TAG_SYSTEM_PERMISSIONS);
      break;
    case 'client':
      mix(TAG_SYSTEM_CLIENT);
      break;
    case 'test':
      mix(TAG_SYSTEM_TEST);
      break;
    default:
      unreachable(system);
  }
}

function visitCompoundKey(k: readonly string[]): void {
  mix(k.length);
  for (let i = 0; i < k.length; i++) {
    mixString(k[i]);
  }
}

function visitOrdering(o: Ordering | undefined): void {
  if (o === undefined) {
    mix(TAG_UNDEF);
    return;
  }
  mix(o.length);
  for (let i = 0; i < o.length; i++) {
    const [field, dir] = o[i];
    mixString(field);
    mix(dir === 'asc' ? TAG_TRUE : TAG_FALSE);
  }
}

function visitValuePosition(v: ValuePosition): void {
  switch (v.type) {
    case 'literal':
      mix(TAG_LITERAL);
      visitValue(v.value);
      return;
    case 'column':
      mix(TAG_COLUMN);
      mixString(v.name);
      return;
    case 'static': {
      mix(TAG_STATIC);
      mixString(v.anchor);
      if (typeof v.field === 'string') {
        mixString(v.field);
      } else {
        visitCompoundKey(v.field);
      }
      return;
    }
    default:
      unreachable(v);
  }
}

function visitCondition(c: Condition): void {
  switch (c.type) {
    case 'simple':
      mix(TAG_SIMPLE);
      mixString(c.op);
      visitValuePosition(c.left);
      visitValuePosition(c.right);
      return;
    case 'correlatedSubquery':
      mix(TAG_SUBQUERY);
      mixString(c.op);
      mix(c.flip === undefined ? TAG_UNDEF : c.flip ? TAG_TRUE : TAG_FALSE);
      mix(c.scalar === undefined ? TAG_UNDEF : c.scalar ? TAG_TRUE : TAG_FALSE);
      visitCorrelatedSubquery(c.related);
      return;
    default: {
      c.type satisfies 'and' | 'or';
      mix(c.type === 'and' ? TAG_AND : TAG_OR);
      const conds = c.conditions;
      mix(conds.length);
      for (let i = 0; i < conds.length; i++) {
        visitCondition(conds[i]);
      }
      return;
    }
  }
}

function visitCorrelatedSubquery(csq: CorrelatedSubquery): void {
  visitCompoundKey(csq.correlation.parentField);
  visitCompoundKey(csq.correlation.childField);
  mix(csq.hidden === undefined ? TAG_UNDEF : csq.hidden ? TAG_TRUE : TAG_FALSE);
  mixOptionalSystem(csq.system);
  visitAST(csq.subquery);
}

function visitAST(ast: AST): void {
  mix(TAG_OBJ);
  visitOptionalString(ast.schema);
  mixString(ast.table);
  visitOptionalString(ast.alias);

  if (ast.where === undefined) {
    mix(TAG_UNDEF);
  } else {
    visitCondition(ast.where);
  }

  const related = ast.related;
  if (related === undefined) {
    mix(TAG_UNDEF);
  } else {
    mix(related.length);
    for (let i = 0; i < related.length; i++) {
      visitCorrelatedSubquery(related[i]);
    }
  }

  const start = ast.start;
  if (start === undefined) {
    mix(TAG_UNDEF);
  } else {
    mix(start.exclusive ? TAG_TRUE : TAG_FALSE);
    visitValue(start.row);
  }

  if (ast.limit === undefined) {
    mix(TAG_UNDEF);
  } else {
    // Tagged, because this is the one optional field whose present and absent
    // forms would otherwise both write a single bare word: an untagged
    // `limit: 4106` is the same word as TAG_UNDEF. The other optionals are
    // safe -- a present string carries STR_MARK, a condition or bound leads
    // with its own kind tag, and `related`/`orderBy` write a count followed by
    // that many items, so their streams diverge straight after.
    mix(TAG_INT);
    mixNumber(ast.limit);
  }

  visitOrdering(ast.orderBy);
  mix(TAG_END);
}

export function hashAST(ast: AST): string {
  // Save and restore the lanes: `visitValue` reads properties of arbitrary
  // runtime objects, and a getter is free to ask for another hash mid-walk.
  // Without this, the inner call's reset() would corrupt the outer digest.
  const s1 = h1;
  const s2 = h2;
  try {
    reset();
    visitAST(ast);
    return finalize();
  } finally {
    h1 = s1;
    h2 = s2;
  }
}

/**
 * The identity of a custom query: its name and its arguments.
 *
 * Same walk, entered at a different node. `args` is arbitrary JSON, which is
 * what `visitValue` already exists to handle. The leading tag keeps this hash
 * space disjoint from `hashAST`'s -- both produce query IDs that live in the
 * same namespace, so an AST must not be able to fold to the same words as some
 * name and args.
 */
export function hashNameAndArgs(
  name: string,
  args: readonly unknown[],
): string {
  const s1 = h1;
  const s2 = h2;
  try {
    reset();
    mix(TAG_NAME_ARGS);
    mixString(name);
    visitValue(args);
    return finalize();
  } finally {
    h1 = s1;
    h2 = s2;
  }
}

/**
 * The identity of a query as the client sees it: its AST, the shape it returns,
 * the system it was built for, and -- for a custom query -- the name and
 * arguments it was declared with.
 *
 * Everything past the AST is here because it is part of what makes two queries
 * different while leaving no trace, or an incomplete one, in the AST itself:
 *
 * - **name and args**: `nameAndArgs` reuses the AST it is given and only
 *   attaches a `CustomQueryID`, so without these two different named queries
 *   over the same table would be indistinguishable.
 * - **system**: a query stamps it onto every `related` entry and every `exists`
 *   correlation it builds, so it is already hashed via
 *   {@link visitCorrelatedSubquery} -- but a query with neither has nowhere to
 *   put it, and would otherwise hash the same whether it was built for the
 *   client or for permissions.
 *
 * The name and args are taken apart rather than as a `CustomQueryID` because
 * that type lives in `zql`, which is above this package; `hashNameAndArgs`
 * splits them the same way.
 */
export function hashOfQueryInternals(
  ast: AST,
  format: Format,
  system: System,
  customQueryName: string | undefined,
  customQueryArgs: readonly unknown[] | undefined,
): string {
  const s1 = h1;
  const s2 = h2;
  try {
    reset();
    visitAST(ast);
    mix(TAG_FORMAT);
    visitValue(format);
    mixSystem(system);
    if (customQueryName === undefined) {
      mix(TAG_UNDEF);
    } else {
      mix(TAG_NAME_ARGS);
      mixString(customQueryName);
      visitValue(customQueryArgs);
    }
    return finalize();
  } finally {
    h1 = s1;
    h2 = s2;
  }
}
