/**
 * **L1 — a t-wise covering array of decorations** (ported from rusty-ivm
 * `rindle-fuzz/src/cover.rs`, design §2 L1).
 *
 * The decorations (`filter / exists / order / limit`) form a combinatorial
 * space we do **not** cross-product (that explodes). Instead {@link greedyCover} builds
 * a *t-wise covering array*: a small set of assignments such that every combination of
 * `t` `(axis, value)` pairs appears in at least one row (`t = 2`, pairwise, by default).
 * {@link decorate} lowers one assignment onto a root table into a concrete `Query`.
 *
 * The array covers all pairwise tuples **by construction** (each round seeds from a
 * still-uncovered tuple), so {@link Coverage} over the realized assignments is the
 * *check* that every row was actually realized on some table — the backbone's "100%
 * pairwise" gate.
 */

import type {Condition} from '../../../../zero-protocol/src/ast.ts';
import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {newStaticQuery} from '../../../../zql/src/query/static-query.ts';
import {schema} from '../schema.ts';
import {
  AXES,
  axisIndex,
  EXISTS_VALS,
  type ExistsVal,
  FILTER_VALS,
  type FilterVal,
  filterRealizable,
  LIMIT_VALS,
  type LimitVal,
  N_AXES,
  ORDER_VALS,
  type OrderVal,
  type Rel,
  relsOf,
  rolesOf,
} from './axes.ts';
import {axisCombinations} from './coverage.ts';
import {filterCondition, simple} from './literals.ts';

// ── the greedy covering-array builder ─────────────────────────────────────────────────

type Tuple = ReadonlyArray<readonly [number, number]>;

function tupleKey(tuple: Tuple): string {
  return tuple.map(([a, v]) => `${a}:${v}`).join(',');
}

/** All t-tuples over `domains` (every value combination is coverable — no constraints). */
function allTuples(domains: readonly number[], t: number): Map<string, Tuple> {
  const out = new Map<string, Tuple>();
  for (const combo of axisCombinations(domains.length, t)) {
    const sizes = combo.map(a => domains[a]);
    for (const values of cartesianLocal(sizes)) {
      const tuple = combo.map((a, i) => [a, values[i]] as const);
      out.set(tupleKey(tuple), tuple);
    }
  }
  return out;
}

function cartesianLocal(domains: readonly number[]): number[][] {
  let out: number[][] = [[]];
  for (const d of domains) {
    const next: number[][] = [];
    for (const prefix of out) {
      for (let v = 0; v < d; v++) {
        next.push([...prefix, v]);
      }
    }
    out = next;
  }
  return out;
}

/** The t-tuples a full assignment covers. */
function rowTuples(row: readonly number[], t: number): Tuple[] {
  return axisCombinations(row.length, t).map(combo =>
    combo.map(a => [a, row[a]] as const),
  );
}

/** How many still-uncovered tuples are fully matched by the assigned axes of a row. */
function coveredNow(
  row: ReadonlyArray<number | null>,
  uncovered: Map<string, Tuple>,
): number {
  let n = 0;
  for (const tuple of uncovered.values()) {
    if (tuple.every(([a, v]) => row[a] === v)) {
      n += 1;
    }
  }
  return n;
}

/**
 * Build a `t`-wise covering array over `AXES`: full assignments (a value index per axis)
 * covering **every** t-tuple. Greedy AETG-style: each round seeds from a still-uncovered
 * tuple and fills the remaining axes to cover the most additional tuples. Deterministic.
 */
export function greedyCover(t: number): number[][] {
  const domains = AXES.map(a => a.values.length);
  const uncovered = allTuples(domains, t);
  const rows: number[][] = [];

  while (uncovered.size > 0) {
    const seed = uncovered.values().next().value as Tuple;
    const row: Array<number | null> = new Array(N_AXES).fill(null);
    for (const [a, v] of seed) {
      row[a] = v;
    }
    // Fill the remaining axes greedily.
    for (let axis = 0; axis < N_AXES; axis++) {
      if (row[axis] !== null) {
        continue;
      }
      let bestV = 0;
      let bestGain = -1;
      for (let v = 0; v < domains[axis]; v++) {
        row[axis] = v;
        const gain = coveredNow(row, uncovered);
        if (gain > bestGain) {
          bestGain = gain;
          bestV = v;
        }
      }
      row[axis] = bestV; // value 0 (a "none") is always a valid fallback
    }
    const full = row.map(x => x as number);
    for (const tuple of rowTuples(full, t)) {
      uncovered.delete(tupleKey(tuple));
    }
    rows.push(full);
  }
  return rows;
}

/** A compact label of a covering-array row (`filter=like exists=none order=desc1 …`). */
export function rowLabel(row: readonly number[]): string {
  return row.map((v, ax) => `${AXES[ax].name}=${AXES[ax].values[v]}`).join(' ');
}

// ── decoration lowering (an assignment → a concrete root `Query`) ─────────────────────

/**
 * The set of root tables L1 decorates. `track` is *universal* (text + numeric +
 * nullable columns + relationships), so every covering-array row is realizable on it
 * (guaranteeing 100% coverage); the rest add parity diversity (self-join `employee`,
 * null-country `customer`, text/numeric `album`/`genre`/`invoice`).
 */
export function decoratableRoots(): readonly string[] {
  return ['track', 'album', 'customer', 'employee', 'genre', 'invoice'];
}

/**
 * `(parent, relationship)` pairs whose child is a **plural collection with several rows
 * per parent**, so decorating the *child* exercises `order` / `limit` / `start` /
 * nested `exists` on a NESTED collection. All chosen children are non-junction (junction
 * order/limit is unsupported by the builder), spanning text-rich + numeric tables.
 */
export function childDecorationPairs(): ReadonlyArray<
  readonly [string, string]
> {
  return [
    ['album', 'tracks'],
    ['artist', 'albums'],
    ['customer', 'invoices'],
    ['genre', 'tracks'],
    ['invoice', 'lines'],
  ];
}

/** Apply the `order` axis to a query (root or child). */
export function applyOrder(q: AnyQuery, table: string, ov: OrderVal): AnyQuery {
  const r = rolesOf(table);
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const qq = q as any;
  switch (ov) {
    case 'none':
      return q;
    case 'asc1':
      return qq.orderBy(r.orderCol, 'asc');
    case 'desc1':
      return qq.orderBy(r.orderCol, 'desc');
    case 'mixed2':
      return qq.orderBy(r.orderCol, 'asc').orderBy(r.num, 'desc');
  }
}

/** Apply the `limit` axis. */
export function applyLimit(q: AnyQuery, lv: LimitVal): AnyQuery {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const qq = q as any;
  switch (lv) {
    case 'none':
      return q;
    case 'small':
      return qq.limit(2); // ≤ a typical child/root group
    case 'large':
      return qq.limit(10_000); // ≥ the whole fixture (effectively unbounded)
  }
}

/**
 * Build the EXISTS/NOT-EXISTS `where` condition for an {@link ExistsVal} on `table`:
 * gate on `rel`, at the requested boolean position. The AND/OR positions pair the gate
 * with a companion simple condition (an always-true `num IS NOT NULL` for AND —
 * structure without changing the answer; a partitioning `num = mid` for OR — a genuine
 * disjunction). `eb` is the fluent expression builder (it fills the gate's correlation
 * keys + junction shape).
 */
export function existsCondition(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  eb: any,
  table: string,
  rel: Rel,
  ev: ExistsVal,
): Condition {
  const r = rolesOf(table);
  const notExists = ev.startsWith('not_exists');
  const gate: Condition = notExists
    ? eb.not(eb.exists(rel.name))
    : eb.exists(rel.name);
  switch (ev) {
    case 'exists_top':
    case 'not_exists_top':
      return gate;
    case 'exists_and':
    case 'not_exists_and':
      return eb.and(gate, simple(r.num, 'IS NOT', null));
    case 'exists_or':
    case 'not_exists_or':
      return eb.or(gate, simple(r.num, '=', r.numMid));
    default:
      throw new Error(`existsCondition called with ${ev}`);
  }
}

/** Combine an optional filter condition + optional exists gate into one `where`. */
function applyWhere(
  q: AnyQuery,
  table: string,
  filterCond: Condition | null,
  gateRel: Rel | undefined,
  ev: ExistsVal,
): AnyQuery {
  if (!filterCond && !gateRel) {
    return q;
  }
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  return (q as any).where((eb: any) => {
    const parts: Condition[] = [];
    if (filterCond) {
      parts.push(filterCond);
    }
    if (gateRel) {
      parts.push(existsCondition(eb, table, gateRel, ev));
    }
    return parts.length === 1 ? parts[0] : eb.and(...parts);
  });
}

/**
 * Apply covering-array assignment `a`'s decorations (`filter` + `exists` gate, `order`,
 * `limit`) to `q` (a query already rooted at `table` — a fresh root or a builder-provided
 * child subquery). `null` if **unrealizable** on `table` (a text filter on a textless
 * table, or an EXISTS on a table without a relationship).
 */
function applyDecorations(
  q: AnyQuery,
  table: string,
  a: readonly number[],
): AnyQuery | null {
  const fv = FILTER_VALS[a[axisIndex('filter')]] as FilterVal;
  const ev = EXISTS_VALS[a[axisIndex('exists')]] as ExistsVal;
  const ov = ORDER_VALS[a[axisIndex('order')]] as OrderVal;
  const lv = LIMIT_VALS[a[axisIndex('limit')]] as LimitVal;

  if (!filterRealizable(table, fv)) {
    return null;
  }

  const filterCond = filterCondition(table, fv);
  let gateRel: Rel | undefined;
  if (ev !== 'none') {
    // The node has no materialized `related` of its own here, so the first relationship
    // is a safe (collision-free) gate.
    gateRel = relsOf(table)[0];
    if (!gateRel) {
      return null; // no relationship to gate on
    }
  }

  let out = applyWhere(q, table, filterCond, gateRel, ev);
  out = applyOrder(out, table, ov);
  out = applyLimit(out, lv);
  return out;
}

/**
 * Lower covering-array assignment `a` onto root `table` into a `Query`, with `true` iff
 * it bears an EXISTS. `null` if the assignment is **unrealizable** on `table`.
 */
export function decorate(
  table: string,
  a: readonly number[],
): [AnyQuery, boolean] | null {
  const ev = EXISTS_VALS[a[axisIndex('exists')]] as ExistsVal;
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const root = newStaticQuery(schema, table as any) as AnyQuery;
  const q = applyDecorations(root, table, a);
  return q ? [q, ev !== 'none'] : null;
}

/**
 * Lower assignment `a` onto the **child** of `parent.rel`: a bare `parent` root with one
 * decorated materialized `related` child (the relationship name). Exercises the
 * decoration axes on a NESTED collection — `order` / `limit` / nested `exists` on the
 * child — which the root-only {@link decorate} never reaches. `true` iff the child bears
 * an EXISTS. `null` if unrealizable on the child table.
 */
export function decorateChild(
  parent: string,
  rel: string,
  a: readonly number[],
): [AnyQuery, boolean] | null {
  const relInfo = relsOf(parent).find(r => r.name === rel);
  if (!relInfo) {
    return null;
  }
  // Realizability precheck (so we return null before building the `.related` wrapper).
  const fv = FILTER_VALS[a[axisIndex('filter')]] as FilterVal;
  if (!filterRealizable(relInfo.child, fv)) {
    return null;
  }
  const ev = EXISTS_VALS[a[axisIndex('exists')]] as ExistsVal;
  if (ev !== 'none' && relsOf(relInfo.child).length === 0) {
    return null;
  }
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const root = (newStaticQuery(schema, parent as any) as any).related(
    rel,
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    (sub: any) => applyDecorations(sub as AnyQuery, relInfo.child, a) ?? sub,
  );
  return [root as AnyQuery, ev !== 'none'];
}
