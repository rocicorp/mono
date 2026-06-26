/**
 * **L4 — the random tail** (ported from rusty-ivm `rindle-fuzz/src/tail.rs`, design §2
 * L4). The faithful pure-random generator, but run **only** for shapes the bounded-
 * exhaustive enumerator can't reach (deeper nesting, larger `where` trees), and **only**
 * under the static cost gate of {@link CostModel} (applied by the driver). It is the
 * long-tail safety net, not the backbone.
 *
 * A random query is a random **skeleton** (deeper than the enumerator's caps — depth up
 * to 4, same "no `related` under EXISTS" invariant), lowered via the fluent builder and
 * given an optional random root filter + `order` / `limit`. (The Rust `start`/`select`
 * decorations are dropped — both are inert here; see `axes.ts`.)
 */

import type {AnyQuery} from '../../../../zql/src/query/query.ts';
import {
  FILTER_VALS,
  filterRealizable,
  type LimitVal,
  type OrderVal,
  relsOf,
  tables,
} from './axes.ts';
import {applyLimit, applyOrder} from './cover.ts';
import {filterCondition} from './literals.ts';
import type {Rng} from './rng.ts';
import {
  type ChildKind,
  isExistsBearing,
  lower,
  nExists,
  nRelated,
  type SkelChild,
  type Skeleton,
} from './skeleton.ts';

/** Bounds for the random tail (beyond the enumerator's `D ≤ 2`). */
export type DeepBounds = {
  readonly depth: number;
  readonly related: number;
  readonly exists: number;
};

/** The default tail shape: depth ≤ 4, up to 3 related + 2 exists. */
export function tailBounds(): DeepBounds {
  return {depth: 4, related: 3, exists: 2};
}

/**
 * Generate one random deep query. `true` iff EXISTS-bearing. `null` if the random root
 * pick is empty (never, for chinook).
 */
export function tailGen(
  rng: Rng,
  bounds: DeepBounds,
): [AnyQuery, boolean] | null {
  const root = rng.choose(tables());
  if (!root) {
    return null;
  }
  const skel = randomSkeleton(
    rng,
    root,
    bounds.related,
    bounds.exists,
    bounds.depth,
    false,
  );
  let q = lower(skel);

  // Optionally add a random root filter (the "larger where tree" the tail targets).
  if (rng.bool(0.6)) {
    const realizable = FILTER_VALS.filter(
      v => v !== 'none' && filterRealizable(root, v),
    );
    const fv = rng.choose(realizable);
    const cond = fv ? filterCondition(root, fv) : null;
    if (cond) {
      // oxlint-disable-next-line @typescript-eslint/no-explicit-any
      q = (q as any).where(() => cond);
    }
  }

  // Random root decorations. A root EXISTS gate may freely combine with order+limit (a
  // top-N-with-a-gate query); plain hydrate parity validates it either way.
  if (rng.bool(0.5)) {
    const ov = rng.choose<OrderVal>(['asc1', 'desc1', 'mixed2']);
    if (ov) {
      q = applyOrder(q, root, ov);
    }
  }
  if (rng.bool(0.4)) {
    const lv = rng.choose<LimitVal>(['small', 'large']);
    if (lv) {
      q = applyLimit(q, lv);
    }
  }

  return [q, isExistsBearing(skel)];
}

/**
 * Build ONE random skeleton (not the full enumeration), threading the global
 * related/exists budget approximately and respecting the per-path depth cap and the "no
 * `related` under EXISTS" invariant.
 */
function randomSkeleton(
  rng: Rng,
  table: string,
  rLeft: number,
  eLeft: number,
  depthLeft: number,
  underExists: boolean,
): Skeleton {
  const children: SkelChild[] = [];
  if (depthLeft === 0) {
    return {table, children};
  }
  const rels = relsOf(table);
  const idxs = rng.shuffle(rels.map((_, i) => i));

  let r = rLeft;
  let e = eLeft;
  for (const j of idxs) {
    if (children.length >= 3 || (r === 0 && e === 0)) {
      break;
    }
    const rel = rels[j];
    const addRelated = !underExists && r > 0 && rng.bool();
    const addExists = !addRelated && e > 0 && rng.bool();

    if (addRelated) {
      const sub = randomSkeleton(
        rng,
        rel.child,
        Math.max(0, r - 1),
        e,
        depthLeft - 1,
        false,
      );
      r = Math.max(0, r - (1 + nRelated(sub)));
      e = Math.max(0, e - nExists(sub));
      children.push({rel: rel.name, kind: 'related', sub});
    } else if (addExists) {
      const kind: ChildKind = rng.bool(0.7) ? 'exists' : 'notExists';
      const sub = randomSkeleton(
        rng,
        rel.child,
        0,
        Math.max(0, e - 1),
        depthLeft - 1,
        true,
      );
      e = Math.max(0, e - (1 + nExists(sub)));
      children.push({rel: rel.name, kind, sub});
    }
  }
  return {table, children};
}
