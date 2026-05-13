import {assert} from '../../../../shared/src/asserts.ts';
import type {
  AST,
  Bound,
  Condition,
  CorrelatedSubquery,
  Ordering,
  ValuePosition,
} from '../../../../zero-protocol/src/ast.ts';
import {normalizeAST} from '../../../../zero-protocol/src/ast.ts';

/**
 * Static (syntactic) covering check.
 *
 * `covers(a, b)` returns true if, for every possible database state, the rows
 * (and joined related sub-trees) that `b` would return are derivable from
 * `a`'s result tree — i.e., a client holding `a`'s results can satisfy `b`
 * locally without consulting the server.
 *
 * v1 is syntactic: predicate implication is detected only as "every conjunct
 * of A appears as a conjunct of B (after normalization)." Range subsumption
 * (`x > 5` covers `x > 10`) and disjunction reasoning (`(x=1 OR x=2)` covers
 * `x=1`) are deliberately not detected. Future versions can layer those on.
 */
export function covers(a: AST, b: AST): boolean {
  return coversNormalized(normalizeAST(a), normalizeAST(b));
}

function coversNormalized(a: AST, b: AST): boolean {
  if (a.table !== b.table) return false;
  if (a.schema !== b.schema) return false;

  // If A drops rows via a window, A's window must exactly match B's. Otherwise
  // a row inside B's window could fall outside A's. orderBy alone (no
  // limit/start) is presentational and doesn't restrict the row set.
  if (a.limit !== undefined || a.start !== undefined) {
    if (a.limit !== b.limit) return false;
    if (!boundEqual(a.start, b.start)) return false;
    if (!orderingEqual(a.orderBy, b.orderBy)) return false;
  }

  if (!conjunctsImply(a.where, b.where)) return false;
  if (!relatedCovers(a.related, b.related)) return false;
  return true;
}

function conjunctsImply(
  aWhere: Condition | undefined,
  bWhere: Condition | undefined,
): boolean {
  const aConjs = topLevelConjuncts(aWhere);
  if (aConjs.length === 0) return true;
  const bConjs = topLevelConjuncts(bWhere);
  for (const ac of aConjs) {
    if (!bConjs.some(bc => conditionEqual(ac, bc))) return false;
  }
  return true;
}

function topLevelConjuncts(where: Condition | undefined): readonly Condition[] {
  if (!where) return [];
  if (where.type === 'and') return where.conditions;
  return [where];
}

function relatedCovers(
  aRelated: readonly CorrelatedSubquery[] | undefined,
  bRelated: readonly CorrelatedSubquery[] | undefined,
): boolean {
  if (!bRelated || bRelated.length === 0) return true;
  if (!aRelated || aRelated.length === 0) return false;
  for (const rb of bRelated) {
    const match = aRelated.find(
      ra =>
        relatedShapeEqual(ra, rb) && coversNormalized(ra.subquery, rb.subquery),
    );
    if (!match) return false;
  }
  return true;
}

function relatedShapeEqual(
  a: CorrelatedSubquery,
  b: CorrelatedSubquery,
): boolean {
  return (
    a.subquery.alias === b.subquery.alias &&
    a.hidden === b.hidden &&
    a.system === b.system &&
    compoundKeyEqual(a.correlation.parentField, b.correlation.parentField) &&
    compoundKeyEqual(a.correlation.childField, b.correlation.childField)
  );
}

function compoundKeyEqual(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function boundEqual(a: Bound | undefined, b: Bound | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.exclusive !== b.exclusive) return false;
  const aKeys = Object.keys(a.row);
  const bKeys = Object.keys(b.row);
  if (aKeys.length !== bKeys.length) return false;
  for (const k of aKeys) {
    if (!Object.hasOwn(b.row, k)) return false;
    if (a.row[k] !== b.row[k]) return false;
  }
  return true;
}

function orderingEqual(
  a: Ordering | undefined,
  b: Ordering | undefined,
): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i][0] !== b[i][0] || a[i][1] !== b[i][1]) return false;
  }
  return true;
}

function conditionEqual(a: Condition, b: Condition): boolean {
  if (a.type !== b.type) return false;
  switch (a.type) {
    case 'simple':
      assert(b.type === 'simple', 'type-checked above');
      return (
        a.op === b.op &&
        valuePositionEqual(a.left, b.left) &&
        valuePositionEqual(a.right, b.right)
      );
    case 'and':
    case 'or': {
      assert(b.type === 'and' || b.type === 'or', 'type-checked above');
      if (a.conditions.length !== b.conditions.length) return false;
      // Inner conditions are sorted by normalizeAST → position-equal works.
      for (let i = 0; i < a.conditions.length; i++) {
        if (!conditionEqual(a.conditions[i], b.conditions[i])) return false;
      }
      return true;
    }
    case 'correlatedSubquery':
      assert(b.type === 'correlatedSubquery', 'type-checked above');
      return (
        a.op === b.op &&
        !!a.flip === !!b.flip &&
        !!a.scalar === !!b.scalar &&
        relatedShapeEqual(a.related, b.related) &&
        astStructurallyEqual(a.related.subquery, b.related.subquery)
      );
  }
}

function valuePositionEqual(a: ValuePosition, b: ValuePosition): boolean {
  if (a.type !== b.type) return false;
  switch (a.type) {
    case 'column':
      assert(b.type === 'column', 'type-checked above');
      return a.name === b.name;
    case 'literal': {
      assert(b.type === 'literal', 'type-checked above');
      const av = a.value;
      const bv = b.value;
      if (Array.isArray(av) || Array.isArray(bv)) {
        if (!Array.isArray(av) || !Array.isArray(bv)) return false;
        if (av.length !== bv.length) return false;
        for (let i = 0; i < av.length; i++) {
          if (av[i] !== bv[i]) return false;
        }
        return true;
      }
      return av === bv;
    }
    case 'static':
      // Static parameters (authData/preMutationRow) are not used now that all
      // queries are custom queries — params resolve before reaching the AST.
      throw new Error('Static parameters are not supported in covers()');
  }
}

function astStructurallyEqual(a: AST, b: AST): boolean {
  if (a.table !== b.table) return false;
  if (a.schema !== b.schema) return false;
  if (a.alias !== b.alias) return false;
  if (a.limit !== b.limit) return false;
  if (!boundEqual(a.start, b.start)) return false;
  if (!orderingEqual(a.orderBy, b.orderBy)) return false;
  if ((a.where === undefined) !== (b.where === undefined)) return false;
  if (a.where && b.where && !conditionEqual(a.where, b.where)) return false;
  const ar = a.related ?? [];
  const br = b.related ?? [];
  if (ar.length !== br.length) return false;
  // normalizeAST sorts `related` by alias, so position-equal works.
  for (let i = 0; i < ar.length; i++) {
    if (!relatedShapeEqual(ar[i], br[i])) return false;
    if (!astStructurallyEqual(ar[i].subquery, br[i].subquery)) return false;
  }
  return true;
}
