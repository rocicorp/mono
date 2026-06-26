/**
 * **Structure-aware shrinking** (ported from rusty-ivm `rindle-fuzz/src/shrink.rs`,
 * design §7) — the minimal-repro engine. On *any* failure (from any layer), delta-debug
 * to a minimal repro by greedily applying the first one-step `AST` simplification that
 * still reproduces, to a fixpoint:
 *
 * - drop a `related` child / a `where` conjunct / `order` terms / `limit` / `start`;
 * - collapse an EXISTS to bare existence; unset `flip`; recurse into children.
 *
 * The shrinks are **hand-written** (a generic shrinker doesn't understand correlation
 * validity): every candidate is a structurally-smaller but still-buildable `AST` (each
 * rule only removes/simplifies, never rewrites a correlation). The search keeps a
 * candidate iff the caller's `fails` predicate still holds, so the result is a local
 * minimum that still reproduces.
 *
 * (Data/push shrinking — the Rust `shrink_data`/`shrink_pushes` — lands with the push
 * layer; the mini fixture is fixed and tiny, so query shrinking is the readability win
 * the backbone needs.)
 */

import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';

/**
 * A rough **complexity measure**: every relationship, condition node, order term, and
 * the `limit`/`start` flags count one. Used to report repro size and to assert the
 * shrinker actually minimized.
 */
export function constructCount(ast: AST): number {
  let n =
    (ast.related?.length ?? 0) +
    (ast.orderBy?.length ?? 0) +
    (ast.limit !== undefined ? 1 : 0) +
    (ast.start !== undefined ? 1 : 0);
  if (ast.where) {
    n += condCount(ast.where);
  }
  for (const r of ast.related ?? []) {
    n += constructCount(r.subquery);
  }
  return n;
}

function condCount(c: Condition): number {
  switch (c.type) {
    case 'simple':
      return 1;
    case 'and':
    case 'or':
      return 1 + c.conditions.reduce((s, cc) => s + condCount(cc), 0);
    case 'correlatedSubquery':
      return 1 + constructCount(c.related.subquery);
  }
}

/**
 * Greedily apply the first one-step `AST` simplification that still reproduces (per
 * `fails`), to a fixpoint. Terminates because every candidate strictly reduces
 * {@link constructCount} (or removes a `flip`), bounded below by zero.
 */
export async function shrinkAst(
  ast: AST,
  fails: (ast: AST) => boolean | Promise<boolean>,
): Promise<AST> {
  let current = ast;
  for (;;) {
    let next: AST | null = null;
    for (const cand of oneStepShrinks(current)) {
      if (await fails(cand)) {
        next = cand;
        break;
      }
    }
    if (next === null) {
      return current;
    }
    current = next;
  }
}

// ── one-step AST shrinks ──────────────────────────────────────────────────────────────

/**
 * Every `AST` reachable from `ast` by a single structural simplification. Each is
 * strictly smaller (or drops a flip), so the greedy search terminates.
 */
export function oneStepShrinks(ast: AST): AST[] {
  const out: AST[] = [];
  const related = ast.related ?? [];

  // Drop decorations.
  if (ast.limit !== undefined) {
    out.push({...ast, limit: undefined});
  }
  if (ast.start !== undefined) {
    out.push({...ast, start: undefined});
  }
  if (ast.orderBy && ast.orderBy.length > 0) {
    out.push({...ast, orderBy: undefined});
    if (ast.orderBy.length > 1) {
      out.push({...ast, orderBy: ast.orderBy.slice(0, -1)});
    }
  }

  // Drop each materialized child.
  for (let i = 0; i < related.length; i++) {
    out.push({...ast, related: related.filter((_, j) => j !== i)});
  }

  // Simplify the `where` tree: drop it whole, or simplify within.
  if (ast.where) {
    out.push({...ast, where: undefined});
    for (const w2 of condShrinks(ast.where)) {
      out.push({...ast, where: w2});
    }
  }

  // Recurse into each materialized child.
  for (let i = 0; i < related.length; i++) {
    for (const sub of oneStepShrinks(related[i].subquery)) {
      const newRelated = related.map((r, j) =>
        j === i ? {...r, subquery: sub} : r,
      );
      out.push({...ast, related: newRelated});
    }
  }

  return out;
}

/** One-step simplifications of a condition tree. */
function condShrinks(c: Condition): Condition[] {
  switch (c.type) {
    case 'simple':
      return [];
    case 'and':
    case 'or': {
      const out: Condition[] = [];
      // Drop a child (keeping ≥1; a lone child unwraps the AND/OR).
      if (c.conditions.length >= 2) {
        for (let i = 0; i < c.conditions.length; i++) {
          out.push(
            rebuild(
              c,
              c.conditions.filter((_, j) => j !== i),
            ),
          );
        }
      }
      // Simplify a child in place.
      for (let i = 0; i < c.conditions.length; i++) {
        for (const s of condShrinks(c.conditions[i])) {
          out.push(
            rebuild(
              c,
              c.conditions.map((cc, j) => (j === i ? s : cc)),
            ),
          );
        }
      }
      return out;
    }
    case 'correlatedSubquery': {
      const out: Condition[] = [];
      // Unset a flip annotation.
      if (c.flip !== undefined) {
        out.push({...c, flip: undefined});
      }
      // Collapse the subquery's own `where` to bare existence, or simplify within.
      const sub = c.related.subquery;
      if (sub.where) {
        out.push({
          ...c,
          related: {...c.related, subquery: {...sub, where: undefined}},
        });
        for (const w2 of condShrinks(sub.where)) {
          out.push({
            ...c,
            related: {...c.related, subquery: {...sub, where: w2}},
          });
        }
      }
      return out;
    }
  }
}

/** Reconstruct an AND/OR with new children (a lone child unwraps to itself). */
function rebuild(orig: {type: 'and' | 'or'}, conds: Condition[]): Condition {
  if (conds.length === 1) {
    return conds[0];
  }
  return {type: orig.type, conditions: conds};
}
