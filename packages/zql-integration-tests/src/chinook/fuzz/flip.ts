/**
 * **Flip-invariance** (ported from rusty-ivm `rindle-fuzz` `parity::assert_flip_invariant`,
 * design §6). `flip` is a **plan choice**, not a semantic one: an `EXISTS` gate can be
 * lowered as a semi-join (`flip: false`) or as a `FlippedJoin` (`flip: true`) — the IVM
 * honors the flag (`planner-builder.ts` picks the join strategy from it) while z2s ignores
 * it (it always emits semi-join-compatible SQL). So **every** flip assignment of an
 * EXISTS-bearing query MUST hydrate to the same rows — and the differential check pins
 * each one to the same Postgres oracle, so if any flip plan diverges it surfaces here.
 *
 * Only **positive** `EXISTS` nodes are flipped: a flipped `NOT EXISTS` (anti-join) is not
 * a supported plan, so we leave those gates as the builder lowered them.
 */

import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';

/** The number of positive `EXISTS` nodes in `ast` (the flippable gates). */
export function flippableExistsCount(ast: AST): number {
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
        if (c.op === 'EXISTS') {
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
 * Set the `flip` flag of each positive `EXISTS` node from `bits` (one boolean per node, in
 * a fixed pre-order). `bits.length` must equal {@link flippableExistsCount}. Pure.
 */
export function setFlips(ast: AST, bits: readonly boolean[]): AST {
  let i = 0;
  const visitCond = (c: Condition): Condition => {
    switch (c.type) {
      case 'simple':
        return c;
      case 'and':
      case 'or':
        return {...c, conditions: c.conditions.map(visitCond)};
      case 'correlatedSubquery': {
        const subquery = visitAst(c.related.subquery);
        const withSub: Condition = {...c, related: {...c.related, subquery}};
        return c.op === 'EXISTS' ? {...withSub, flip: bits[i++]} : withSub;
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

/** Every `{false, true}^k` assignment for `k` flippable gates (`2^k` rows). */
export function flipAssignments(k: number): boolean[][] {
  const out: boolean[][] = [];
  for (let mask = 0; mask < 1 << k; mask++) {
    out.push(Array.from({length: k}, (_, b) => (mask & (1 << b)) !== 0));
  }
  return out;
}
