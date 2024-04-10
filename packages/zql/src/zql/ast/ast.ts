// Going for a subset of the SQL `SELECT` grammar
// https://www.sqlite.org/lang_select.html

import {compareUTF8} from 'compare-utf8';

// TODO: the chosen operator needs to constrain the allowed values for the value
// input to the query builder.
export type Ordering = readonly [readonly string[], 'asc' | 'desc'];
export type Primitive = string | number | boolean | null;

// I think letting users provide their own lambda functions
// to perform the aggregation would make the most sense.
// We should should extend that to let users provide `filter`, `map`, and `reduce` lambdas
// to do things not available in the query language itself.
export type Aggregate = 'sum' | 'avg' | 'min' | 'max' | 'array' | 'count';
export type Aggregation = {
  readonly field?: string | undefined;
  readonly alias: string;
  readonly aggregate: Aggregate;
};

// type Ref = `${string}.${string}`;

/**
 * Note: We'll eventually need to start ordering conditions
 * in the dataflow graph so we get the maximum amount
 * of sharing between queries.
 */
export type AST = {
  readonly table: string;
  readonly alias?: string | undefined;
  readonly select?: [selector: string, alias: string][] | undefined;
  readonly aggregate?: Aggregation[] | undefined;
  // readonly subQueries?: {
  //   readonly alias: string;
  //   readonly query: AST;
  // }[];
  readonly where?: Condition | undefined;
  // readonly joins?: {
  //   readonly table: string;
  //   readonly as: string;
  //   readonly on: ConditionList;
  // }[];
  readonly limit?: number | undefined;
  readonly groupBy?: string[] | undefined;
  readonly orderBy: Ordering;
  // readonly after?: Primitive;
};

export type Condition = SimpleCondition | Conjunction;
export type Conjunction = {
  type: 'conjunction';
  op: 'AND' | 'OR';
  conditions: Condition[];
};
export type SimpleOperator = EqualityOps | OrderOps | InOps | LikeOps;

export type EqualityOps = '=' | '!=';

export type OrderOps = '<' | '>' | '<=' | '>=';

export type InOps = 'IN' | 'NOT IN';

export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';

export type SimpleCondition =
  // | ConditionList
  {
    type: 'simple';
    op: SimpleOperator;
    field: string;
    value: {
      type: 'literal';
      value: Primitive;
    };
    //  | {
    //   type: 'ref';
    //   value: Ref;
    // } | {
    //   type: 'query';
    //   value: AST;
    // };
  };

/**
 * Returns a normalized version the AST with all order-agnostic lists
 * (everything except ORDER BY) sorted in a deterministic manner, and
 * condition trees flattened, such that semantically equivalent ASTs have
 * the same structure.
 */
export function normalizeAST(ast: AST): AST {
  return {
    table: ast.table,
    alias: ast.alias,
    select: ast.select
      ? [...ast.select].sort(([a], [b]) => compareUTF8(a, b))
      : undefined,
    aggregate: ast.aggregate
      ? [...ast.aggregate].sort(
          (a, b) =>
            compareUTF8(a.aggregate, b.aggregate) ||
            compareUTF8(a.field ?? '*', b.field ?? '*'),
        )
      : undefined,
    where: ast.where ? sorted(flattened(ast.where)) : undefined,
    groupBy: ast.groupBy ? [...ast.groupBy].sort(compareUTF8) : undefined,
    // The order of ORDER BY expressions is semantically significant, so it
    // is left as is (i.e. not sorted).
    orderBy: ast.orderBy,
    limit: ast.limit,
  };
}

/**
 * Returns a flattened version of the Conditions in which nested Conjunctions with
 * the same operation ('AND' or 'OR') are flattened to the same level. e.g.
 *
 * ```
 * ((a AND b) AND (c AND (d OR (e OR f)))) -> (a AND b AND c AND (d OR e OR f))
 * ```
 */
function flattened(cond: Condition): Condition {
  if (cond.type === 'simple') {
    return cond;
  }
  const {type, op, conditions} = cond;
  return {
    type,
    op,
    conditions: conditions.flatMap(c =>
      c.op === op ? c.conditions.map(c => flattened(c)) : flattened(c),
    ),
  };
}

/**
 * Returns a sorted version of the Conditions for deterministic hashing / deduping.
 * This is semantically valid because the order of evaluation of subexpressions is
 * not defined; specifically, the query engine chooses the best order for them:
 * https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-EXPRESS-EVAL
 */
function sorted(cond: Condition): Condition {
  if (cond.type === 'simple') {
    return cond;
  }
  return {
    type: cond.type,
    op: cond.op,
    conditions: cond.conditions.map(c => sorted(c)).sort(cmp),
  };
}

function cmp(a: Condition, b: Condition): number {
  if (a.type === 'simple') {
    if (b.type !== 'simple') {
      return -1; // Order SimpleConditions first to simplify logic for invalidation filtering.
    }
    return (
      compareUTF8(a.field, b.field) ||
      compareUTF8(a.op, b.op) ||
      // Comparing the same field with the same op more than once doesn't make logical
      // sense, but is technically possible. Assume the values are of the same type and
      // sort by their String forms.
      compareUTF8(String(a.value.value), String(b.value.value))
    );
  }
  if (b.type === 'simple') {
    return 1; // Order SimpleConditions first to simplify logic for invalidation filtering.
  }
  // For comparing two conjunctions, compare the ops first, and then compare
  // the conjunctions member-wise.
  const val = compareUTF8(a.op, b.op);
  if (val !== 0) {
    return val;
  }
  for (
    let l = 0, r = 0;
    l < a.conditions.length && r < b.conditions.length;
    l++, r++
  ) {
    const val = cmp(a.conditions[l], b.conditions[r]);
    if (val !== 0) {
      return val;
    }
  }
  // prefixes first
  return a.conditions.length - b.conditions.length;
}
