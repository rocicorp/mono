import {deepEqual} from '../../../shared/src/json.js';
import {must} from '../../../shared/src/must.js';
import type {AST, Condition} from '../../../zero-protocol/src/ast.js';
import {gatherCorrelatedSubqueryQueriesFromCondition} from '../builder/builder.js';
import {SUBQ_PREFIX} from '../query/query-impl.js';

/**
 * We have a much more complete and complex covering algorithm sketched out in the design docs.
 * This is an implementation that only handles the simplest of cases.
 *
 * The designed algorithm can determine if many existing queries taken together
 * cover a new query.
 *
 * It can also handle limits, where clauses, divergent subqueries, and more.
 */
type Table = string;
type QueryHash = string;

export function findCover(
  existingQueries: Map<Table, Map<QueryHash, {normalized: AST}>>,
  query: AST,
): {hash: QueryHash; query: AST} | undefined {
  for (const [hash, candidate] of existingQueries.get(query.table) ?? []) {
    if (covers(candidate.normalized, query)) {
      return {hash, query: candidate.normalized};
    }
  }

  return undefined;
}

export function covers(cover: AST, covered: AST) {
  return (
    cover.table === covered.table &&
    whereCovers(cover.where, covered.where) &&
    limitCovers(cover, covered) &&
    coversRelationships(cover, covered)
  );
}

function limitCovers(cover: AST, covered: AST) {
  if (cover.limit === undefined) {
    return true;
  }

  if (covered.limit === undefined) {
    return false;
  }

  return cover.limit >= covered.limit && sameOrder(cover, covered);
}

function sameOrder(cover: AST, covered: AST) {
  return deepEqual(cover.orderBy, covered.orderBy);
}

function whereCovers(
  cover: Condition | undefined,
  covered: Condition | undefined,
): boolean {
  if (cover === undefined) {
    return true;
  }

  if (cover === covered) {
    return true;
  }

  if (cover === undefined || covered === undefined) {
    return false;
  }

  return conditionCovers(cover, covered);
}

function conditionCovers(cover: Condition, covered: Condition): boolean {
  if (cover.type === 'simple' && covered.type === 'simple') {
    // we can be smarter here like checking if an inequality is wider than the other.
    return deepEqual(cover, covered);
  }

  if (cover.type === 'simple' && covered.type === 'or') {
    return covered.conditions.every(c => conditionCovers(cover, c));
  }

  if (cover.type === 'simple' && covered.type === 'and') {
    return covered.conditions.some(c => conditionCovers(cover, c));
  }

  if (cover.type === 'and' && covered.type === 'and') {
    return cover.conditions.every(c =>
      covered.conditions.some(cc => conditionCovers(c, cc)),
    );
  }

  if (cover.type === 'or' && covered.type === 'or') {
    if (cover.conditions.length !== covered.conditions.length) {
      return false;
    }

    for (let i = 0; i < cover.conditions.length; i++) {
      if (!conditionCovers(cover.conditions[i], covered.conditions[i])) {
        return false;
      }
    }
  }

  if (cover.type === 'or' && covered.type === 'and') {
    // if any branch covers we're good.
    return cover.conditions.some(cover => conditionCovers(cover, covered));
  }

  if (cover.type === 'or' && covered.type === 'simple') {
    return cover.conditions.some(cover => conditionCovers(cover, covered));
  }

  return false;
}

function coversRelationships(cover: AST, query: AST): boolean {
  const coverSubqueries = pullSubqueries(cover);
  const subqueries = pullSubqueries(query);

  function getCoversSubquery(alias: string) {
    return (
      coverSubqueries.get(alias) ??
      coverSubqueries.get(SUBQ_PREFIX + alias) ??
      coverSubqueries.get(alias.substring(SUBQ_PREFIX.length))
    );
  }

  // Make sure the cover has all the same (or more) subqueries.
  for (const alias of subqueries.keys()) {
    if (getCoversSubquery(alias) === undefined) {
      return false;
    }
  }

  // Make sure each subquery covers the corresponding subquery.
  for (const [alias, subquery] of subqueries) {
    const coverSubquery = must(getCoversSubquery(alias));
    if (!covers(coverSubquery, subquery)) {
      return false;
    }
  }

  return true;
}

function pullSubqueries(query: AST) {
  return new Map([
    ...(query.related?.map(
      r => [must(r.subquery.alias), r.subquery] as const,
    ) ?? []),
    ...gatherCorrelatedSubqueryQueriesFromCondition(query.where).map(
      csq => [must(csq.subquery.alias), csq.subquery] as const,
    ),
  ]);
}
