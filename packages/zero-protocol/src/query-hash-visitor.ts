/**
 * Prototype: hash a normalized AST by visiting it, instead of by rendering it
 * to JSON and hashing the bytes.
 *
 * The shipped pipeline is `h64(JSON.stringify(normalizeAST(ast))).toString(36)`.
 * That materializes the whole AST as a string, UTF-8 encodes it, and only then
 * hashes — three passes over the data, two of which exist purely to produce
 * bytes to hash.
 *
 * A visitor can fold values into the hash state as it walks, so nothing is
 * materialized at all. The primitives that make that pay off:
 *
 * - Two 32-bit accumulators held in module-level `let`s, so the "state" is
 *   plain numbers in registers rather than an object being threaded through
 *   the walk.
 * - `Math.imul` for the mixing step. js-xxhash avoids it (splitting each
 *   multiply into 16-bit halves) to support ancient engines; we don't need to.
 * - Strings folded straight from `charCodeAt`, two UTF-16 units at a time, into
 *   one 32-bit word. No UTF-8 encoding, and half the loop iterations a
 *   byte-oriented hash would need over ASCII.
 *
 * The hash need not agree with UTF-8 xxHash32 — it only has to be injective
 * over distinct ASTs. It does NOT match `hashOfAST`, so adopting it is a wire
 * format change.
 *
 * The walk is deliberately specialized to the AST shape rather than written as
 * a generic JSON-value walk. That is not premature: a generic version was
 * measured alongside this one and came out ~2.4x slower, no better than the
 * `JSON.stringify` it was meant to replace, because `for...in` and the
 * megamorphic property loads cost about what the native stringify costs. Only
 * `visitValue` below stays generic, for the two places an AST holds arbitrary
 * JSON: a literal's value and a bound's row.
 */
import type {AST, Condition, CorrelatedSubquery, Ordering} from './ast.ts';

// xxHash32's primes, reused because they're well-tested odd constants with
// good bit distribution. The construction below is not xxHash.
const P1 = 0x9e3779b1 | 0;
const P2 = 0x85ebca77 | 0;
const P3 = 0xc2b2ae3d | 0;
const P4 = 0x27d4eb2f | 0;
const P5 = 0x165667b1 | 0;

// Two independent 32-bit lanes, combined into 64 bits at the end. Module-level
// so the walk mutates registers rather than allocating or threading state.
let h1 = 0;
let h2 = 0;

/** Fold one 32-bit word into both lanes. */
function mix(w: number): void {
  let a = (h1 + Math.imul(w, P2)) | 0;
  a = (a << 13) | (a >>> 19);
  h1 = Math.imul(a, P1);

  let b = (h2 ^ Math.imul(w, P3)) | 0;
  b = (b << 17) | (b >>> 15);
  h2 = Math.imul(b, P4);
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

const f64 = new Float64Array(1);
const i32 = new Int32Array(f64.buffer);

/** Fold a number: one word for an int32, two for anything else. */
function mixNumber(n: number): void {
  if ((n | 0) === n) {
    mix(n);
  } else {
    f64[0] = n;
    mix(i32[0]);
    mix(i32[1]);
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

// Set on a string's length word. High enough that no array length reaches it.
const STR_MARK = 0x40000000;

function reset(): void {
  h1 = P5;
  h2 = P1;
}

/** Collapse the two lanes into one 64-bit value, avalanching each first. */
function finalize(): string {
  let a = h1;
  a ^= a >>> 15;
  a = Math.imul(a, P2);
  a ^= a >>> 13;
  a = Math.imul(a, P3);
  a ^= a >>> 16;

  let b = h2;
  b ^= b >>> 16;
  b = Math.imul(b, P3);
  b ^= b >>> 13;
  b = Math.imul(b, P2);
  b ^= b >>> 15;

  return (
    (a >>> 0).toString(36).padStart(7, '0') +
    (b >>> 0).toString(36).padStart(7, '0')
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
          visitValue(v[i]);
        }
        mix(TAG_END);
        return;
      }
      mix(TAG_OBJ);
      for (const k in v) {
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
      return; // undefined, functions: dropped, as JSON.stringify does
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

function visitValuePosition(v: unknown): void {
  const t = (v as {type: string}).type;
  switch (t) {
    case 'literal':
      mix(TAG_LITERAL);
      visitValue((v as {value: unknown}).value);
      return;
    case 'column':
      mix(TAG_COLUMN);
      mixString((v as {name: string}).name);
      return;
    default: {
      // 'static' parameter reference.
      mix(TAG_STATIC);
      const p = v as {anchor: string; field: string | string[]};
      mixString(p.anchor);
      if (typeof p.field === 'string') {
        mixString(p.field);
      } else {
        visitCompoundKey(p.field);
      }
      return;
    }
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
      // 'and' / 'or'
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
  visitOptionalString(csq.system);
  visitAST_(csq.subquery);
}

function visitAST_(ast: AST): void {
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
    mixNumber(ast.limit);
  }

  visitOrdering(ast.orderBy);
  mix(TAG_END);
}

export function hashAST(ast: AST): string {
  reset();
  visitAST_(ast);
  return finalize();
}
