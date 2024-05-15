// Going for a subset of the SQL `SELECT` grammar
// https://www.sqlite.org/lang_select.html

import {compareUTF8} from 'compare-utf8';
import {defined} from 'shared/src/arrays.js';

export type Selector = [table: string, column: string];

// TODO: the chosen operator needs to constrain the allowed values for the value
// input to the query builder.
export type Ordering = readonly [
  fields: readonly Selector[],
  direction: 'asc' | 'desc',
];
export type Primitive = string | number | boolean | null;
export type PrimitiveArray = string[] | number[] | boolean[];

// I think letting users provide their own lambda functions
// to perform the aggregation would make the most sense.
// We should should extend that to let users provide `filter`, `map`, and `reduce` lambdas
// to do things not available in the query language itself.
export type Aggregate = 'sum' | 'avg' | 'min' | 'max' | 'array' | 'count';
export type Aggregation = {
  readonly field?: Selector | undefined;
  readonly alias: string;
  readonly aggregate: Aggregate;
};

export type Join = {
  readonly type: 'inner' | 'left' | 'right' | 'full';
  readonly other: AST;
  readonly as: string;
  // only joining by equality is supported at the moment.
  readonly on: [left: Selector, right: Selector];
};

// type Ref = `${string}.${string}`;

/**
 * Note: We'll eventually need to start ordering conditions
 * in the dataflow graph so we get the maximum amount
 * of sharing between queries.
 */
export type AST = {
  readonly schema?: string | undefined;
  readonly table: string;
  readonly alias?: string | undefined;
  readonly select?: [selector: Selector, alias: string][] | undefined;
  readonly distinct?: Selector | undefined;
  readonly aggregate?: Aggregation[] | undefined;
  // readonly subQueries?: {
  //   readonly alias: string;
  //   readonly query: AST;
  // }[];
  readonly where?: Condition | undefined;
  readonly joins?: Join[] | undefined;
  readonly limit?: number | undefined;
  readonly groupBy?: Selector[] | undefined;
  readonly orderBy?: Ordering | undefined;
  readonly having?: Condition | undefined;
  // readonly after?: Primitive;
};

export type Condition = SimpleCondition | Conjunction;
export type Conjunction = {
  type: 'conjunction';
  op: 'AND' | 'OR';
  conditions: Condition[];
};
export type SimpleOperator = EqualityOps | OrderOps | InOps | LikeOps | SetOps;

export type EqualityOps = '=' | '!=';

export type OrderOps = '<' | '>' | '<=' | '>=';

export type InOps = 'IN' | 'NOT IN';

export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';

export type SetOps =
  | 'INTERSECTS'
  | 'DISJOINT'
  | 'SUPERSET'
  | 'CONGRUENT'
  | 'INCONGRUENT'
  | 'SUBSET';

export type SimpleCondition = {
  type: 'simple';
  op: SimpleOperator;
  field: Selector;
  value: {
    type: 'value';
    value: Primitive | PrimitiveArray;
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
 *
 * Conjunctions are also normalized such that:
 * * Those with an empty list of Conditions are removed and
 * * Those with a singleton Condition are flattened.
 *
 * This means that in a normalized AST, Conjunctions are guaranteed to have at
 * least 2 Conditions.
 */
export function normalizeAST(ast: AST): AST {
  const where = flattened(ast.where);
  return {
    schema: ast.schema,
    table: ast.table,
    alias: ast.alias,
    select: ast.select
      ? [...ast.select].sort(
          ([a], [b]) => compareUTF8(a[0], b[0]) || compareUTF8(a[1], b[1]),
        )
      : undefined,
    aggregate: ast.aggregate
      ? [...ast.aggregate].sort((a, b) => {
          const cmp = compareUTF8(a.aggregate, b.aggregate);
          if (cmp !== 0) {
            return cmp;
          }
          if (a.field === undefined) {
            return b.field === undefined ? 0 : -1;
          } else if (b.field === undefined) {
            return 1;
          }
          return (
            compareUTF8(a.field[0], b.field[0]) ||
            compareUTF8(a.field[1], b.field[1])
          );
        })
      : undefined,
    where: where ? sorted(where) : undefined,
    joins: ast.joins?.map(join => ({...join, other: normalizeAST(join.other)})),
    groupBy: ast.groupBy
      ? [...ast.groupBy].sort(
          (l, r) => compareUTF8(l[0], r[0]) || compareUTF8(l[1], r[1]),
        )
      : undefined,
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
 *
 * Also flattens singleton Conjunctions regardless of operator, and removes
 * empty Conjunctions.
 */
function flattened(cond: Condition | undefined): Condition | undefined {
  if (cond === undefined) {
    return undefined;
  }
  if (cond.type === 'simple') {
    return cond;
  }
  const conditions = defined(
    cond.conditions.flatMap(c =>
      c.op === cond.op ? c.conditions.map(c => flattened(c)) : flattened(c),
    ),
  );

  switch (conditions.length) {
    case 0:
      return undefined;
    case 1:
      return conditions[0];
    default:
      return {
        type: cond.type,
        op: cond.op,
        conditions,
      };
  }
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
      compareUTF8(a.field[0], b.field[0]) ||
      compareUTF8(a.field[1], b.field[1]) ||
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

export function isJoinWithQuery(join: Join) {
  const {other} = join;
  return (
    other.aggregate !== undefined ||
    other.groupBy !== undefined ||
    other.joins !== undefined ||
    other.where !== undefined
  );
}
