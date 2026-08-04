/**
 * **Scalar-invariance** (design §6, shaped like {@link flippableExistsCount flip.ts}).
 *
 * `{scalar: true}` on an EXISTS gate is a **plan hint, not a semantic one**: it asserts the
 * subquery matches at most one row, so an engine *may* decorrelate the gate into a literal
 * comparison. z2s ignores it (Postgres decorrelates EXISTS on its own) and the base IVM
 * ignores it; only zero-cache's pipeline-driver acts on it, pre-resolving a *simple*
 * (unique-key-pinned) subquery through `resolveSimpleScalarSubqueries` — a transform whose
 * own soundness is unit-tested in `zqlite/src/resolve-scalar-subqueries.test.ts`.
 *
 * So the property is: **marking gates scalar must not change results.** Every
 * `{false, true}^k` scalar assignment of an EXISTS-bearing query MUST hydrate to the same
 * rows — and the differential pins each one to the same Postgres oracle, so if any
 * assignment diverges it surfaces here.
 *
 * This lane used to generate only PK-pinned (hence provably decorrelatable) gates, and ran
 * the *resolved* AST through the IVM against the *original* through z2s. That mirrored
 * production, but it assumed the rewrite valid on both sides and so could not observe a
 * wrong one: it missed a z2s bug that dropped rows for every scalar gate not pinned to a
 * unique key (repro: `src/scalar-nested-exists.pg.test.ts`). Generating the unpinned space
 * is the whole point — an undecorated skeleton gate is already unpinned.
 *
 * Both `EXISTS` and `NOT EXISTS` gates are marked (`not()` carries the flag through), at
 * any nesting depth, so the shapes the old one-hop candidate list could not express — a
 * scalar gate whose body nests a further EXISTS — are now in range.
 *
 * Junction (two-hop) wrappers are skipped: the builder lands `{scalar: true}` on the
 * *inner* hop of a junction, so marking the wrapper would generate an AST no builder call
 * can produce.
 */

import {
  SUBQ_PREFIX,
  type AST,
  type Condition,
  type CorrelatedSubqueryCondition,
} from '../../../../zero-protocol/src/ast.ts';

/**
 * Whether `c` is the outer wrapper the builder emits for a junction (two-hop)
 * relationship — recognized, as in `ast-to-zql`, by the hidden alias on the second hop.
 */
function isJunctionWrapper(c: CorrelatedSubqueryCondition): boolean {
  const secondHop = c.related.subquery.where;
  return (
    secondHop?.type === 'correlatedSubquery' &&
    (secondHop.related.subquery.alias?.includes(SUBQ_PREFIX + 'zhidden_') ??
      false)
  );
}

/** The number of gates in `ast` that {@link setScalars} will mark. */
export function scalarizableExistsCount(ast: AST): number {
  let n = 0;
  const countCond = (c: Condition): void => {
    switch (c.type) {
      case 'simple':
        return;
      case 'and':
      case 'or':
        c.conditions.forEach(countCond);
        return;
      case 'correlatedSubquery':
        if (!isJunctionWrapper(c)) {
          n += 1;
        }
        countAst(c.related.subquery);
    }
  };
  const countAst = (a: AST): void => {
    if (a.where) {
      countCond(a.where);
    }
    for (const r of a.related ?? []) {
      countAst(r.subquery);
    }
  };
  countAst(ast);
  return n;
}

/**
 * Set the `scalar` flag of each markable gate from `bits` (one boolean per gate, in a fixed
 * pre-order). `bits.length` must equal {@link scalarizableExistsCount}. Pure.
 */
export function setScalars(ast: AST, bits: readonly boolean[]): AST {
  let i = 0;
  const visitCond = (c: Condition): Condition => {
    switch (c.type) {
      case 'simple':
        return c;
      case 'and':
      case 'or':
        return {...c, conditions: c.conditions.map(visitCond)};
      case 'correlatedSubquery': {
        const mark = !isJunctionWrapper(c);
        // Consume this gate's bit before descending, so the pre-order matches the count.
        const scalar = mark ? bits[i++] : undefined;
        const subquery = visitAst(c.related.subquery);
        const withSub: Condition = {...c, related: {...c.related, subquery}};
        return scalar === undefined ? withSub : {...withSub, scalar};
      }
    }
  };
  const visitAst = (a: AST): AST => ({
    ...a,
    where: a.where ? visitCond(a.where) : undefined,
    related: a.related?.map(r => ({...r, subquery: visitAst(r.subquery)})),
  });
  return visitAst(ast);
}

/** Whether `ast` carries any `scalar: true` correlated subquery. */
export function hasScalarSubquery(ast: AST): boolean {
  const inCond = (c: AST['where']): boolean => {
    if (!c) {
      return false;
    }
    switch (c.type) {
      case 'correlatedSubquery':
        return !!c.scalar || inAst(c.related.subquery);
      case 'and':
      case 'or':
        return c.conditions.some(inCond);
      default:
        return false;
    }
  };
  const inAst = (a: AST): boolean =>
    inCond(a.where) || (a.related ?? []).some(r => inAst(r.subquery));
  return inAst(ast);
}
