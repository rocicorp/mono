/**
 * **L0 — bounded-exhaustive structural skeletons** (ported from rusty-ivm
 * `rindle-fuzz/src/skeleton.rs`, design §2 L0, the backbone).
 *
 * The *small-scope hypothesis*: most bugs manifest on small inputs, so enumerate every
 * query *structure* up to a size bound, smallest-first — the first failing skeleton
 * found is then already near-minimal. A **skeleton** is the structural tree only (the
 * nesting of `related` / EXISTS), with **no decorations** (`where` / `order` / `limit` /
 * `start` are added by L1). Each node picks a schema relationship (so every correlation
 * the builder lowers is buildable), bounded by `(depth D, #exists E, #related R)`.
 *
 * `E`/`R` are **global** caps (counted across the whole tree); `D` is a per-path depth
 * bound. Enumeration is canonical (a node's children are taken in relationship-
 * declaration order, each relationship at most once per node), so no two enumerated
 * skeletons differ only by sibling order. A `related` subtree may nest `related` or
 * EXISTS; an EXISTS subtree may nest only further EXISTS (materialized children are
 * inert under existence — design §8 — so they are not generated there).
 */

import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {newStaticQuery} from '../../../../zql/src/query/static-query.ts';
import {schema} from '../schema.ts';
import {relsOf, tables} from './axes.ts';

/** Whether a structural child is a materialized relationship or an existence gate. */
export type ChildKind = 'related' | 'exists' | 'notExists';

function isExists(kind: ChildKind): boolean {
  return kind === 'exists' || kind === 'notExists';
}

/** `[relatedCost, existsCost]` against the global budget. */
function cost(kind: ChildKind): [number, number] {
  return kind === 'related' ? [1, 0] : [0, 1];
}

/** One structural child: a relationship used as `related` / EXISTS, + its sub-skeleton. */
export type SkelChild = {
  readonly rel: string;
  readonly kind: ChildKind;
  readonly sub: Skeleton;
};

/** A decoration-free query structure: a root table plus structural children. */
export type Skeleton = {
  readonly table: string;
  readonly children: readonly SkelChild[];
};

/** The size bound on enumerated skeletons (design caps: `D ≤ 3, E ≤ 2, R ≤ 2`). */
export type Bounds = {
  readonly depth: number;
  readonly related: number;
  readonly exists: number;
};

/** The per-PR backbone bound: exhaustive at depth ≤ 2. */
export function backboneBounds(): Bounds {
  return {depth: 2, related: 2, exists: 2};
}

// ── skeleton measures ─────────────────────────────────────────────────────────────────

export function nExists(s: Skeleton): number {
  return s.children.reduce(
    (acc, c) => acc + (isExists(c.kind) ? 1 : 0) + nExists(c.sub),
    0,
  );
}

export function nRelated(s: Skeleton): number {
  return s.children.reduce(
    (acc, c) => acc + (c.kind === 'related' ? 1 : 0) + nRelated(c.sub),
    0,
  );
}

export function depthOf(s: Skeleton): number {
  return s.children.reduce((m, c) => Math.max(m, 1 + depthOf(c.sub)), 0);
}

/** Whether the skeleton bears any EXISTS (so it routes through flip-invariance later). */
export function isExistsBearing(s: Skeleton): boolean {
  return nExists(s) > 0;
}

/**
 * The **deepest** table reached (a leaf of the structure) — the most propagation-rich
 * mutation target. For a bare root, the root itself.
 */
export function deepestTable(s: Skeleton): string {
  let best: {d: number; t: string} = {d: 0, t: s.table};
  for (const c of s.children) {
    const d = 1 + depthOf(c.sub);
    if (d > best.d) {
      best = {d, t: deepestTable(c.sub)};
    }
  }
  return best.t;
}

/** A compact structural label (`album(rel:track,ex:artist)`) for failure reports. */
export function label(s: Skeleton): string {
  if (s.children.length === 0) {
    return s.table;
  }
  const kids = s.children.map(c => {
    const k = c.kind === 'related' ? 'rel' : c.kind === 'exists' ? 'ex' : 'nx';
    return `${k}:${label(c.sub)}`;
  });
  return `${s.table}(${kids.join(',')})`;
}

// ── lowering (skeleton → a runnable Query via the fluent builder) ─────────────────────

/**
 * Lower a skeleton to a bare `Query` (no decorations): materialized children become
 * `.related(...)`; EXISTS children become `.whereExists(...)` / `not(exists(...))`
 * gates, ANDed at the top. The fluent builder fills correlation keys (incl. junctions)
 * from the schema.
 */
export function lower(s: Skeleton): AnyQuery {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  const q = newStaticQuery(schema, s.table as any) as AnyQuery;
  return applySkeleton(q, s);
}

function applySkeleton(q: AnyQuery, s: Skeleton): AnyQuery {
  let out = q;
  for (const child of s.children) {
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    const sub = (cq: any) => applySkeleton(cq as AnyQuery, child.sub);
    if (child.kind === 'related') {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      out = (out as any).related(child.rel, sub);
    } else if (child.kind === 'exists') {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      out = (out as any).whereExists(child.rel, sub);
    } else {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      out = (out as any).where(({not, exists}: any) =>
        not(exists(child.rel, sub)),
      );
    }
  }
  return out;
}

// ── bounded-exhaustive enumeration ────────────────────────────────────────────────────

function allowedKinds(underExists: boolean): readonly ChildKind[] {
  return underExists
    ? ['exists', 'notExists']
    : ['related', 'exists', 'notExists'];
}

/**
 * All canonical child-forests for `table`, choosing relationships from index `start`
 * onward, within `(r, e, depth)` budget. Returns each forest with the budget
 * **remaining** after it, so a sibling group correctly shares the global `r`/`e` budget
 * with each child's subtree while `depth` stays per-path.
 */
function forests(
  table: string,
  start: number,
  r: number,
  e: number,
  depth: number,
  underExists: boolean,
): Array<[SkelChild[], number, number]> {
  // Always an option to add no (more) children: the full budget remains.
  const out: Array<[SkelChild[], number, number]> = [[[], r, e]];
  if (depth === 0) {
    return out;
  }
  const rels = relsOf(table);
  for (let j = start; j < rels.length; j++) {
    const rel = rels[j];
    for (const kind of allowedKinds(underExists)) {
      const [dr, de] = cost(kind);
      if (dr > r || de > e) {
        continue;
      }
      const subforests = forests(
        rel.child,
        0,
        r - dr,
        e - de,
        depth - 1,
        isExists(kind),
      );
      for (const [subc, r2, e2] of subforests) {
        const child: SkelChild = {
          rel: rel.name,
          kind,
          sub: {table: rel.child, children: subc},
        };
        for (const [rest, r3, e3] of forests(
          table,
          j + 1,
          r2,
          e2,
          depth,
          underExists,
        )) {
          out.push([[child, ...rest], r3, e3]);
        }
      }
    }
  }
  return out;
}

/**
 * Enumerate **every** skeleton within `bounds`, smallest-first by construction — every
 * root table, every canonical child configuration. Deterministic.
 */
export function enumerate(bounds: Bounds): Skeleton[] {
  const out: Skeleton[] = [];
  for (const t of tables()) {
    for (const [children] of forests(
      t,
      0,
      bounds.related,
      bounds.exists,
      bounds.depth,
      false,
    )) {
      out.push({table: t, children});
    }
  }
  return out;
}
