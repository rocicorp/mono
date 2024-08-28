/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];
export type Ordering = readonly OrderPart[];
import {compareUTF8} from 'compare-utf8';
import {must} from 'shared/src/must.js';

export type SimpleOperator = EqualityOps | OrderOps | LikeOps | InOps;
export type EqualityOps = '=' | '!=';
export type OrderOps = '<' | '>' | '<=' | '>=';
export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';
export type InOps = 'IN' | 'NOT IN';

export type AST = {
  readonly schema?: string | undefined;
  readonly table: string;

  // A query would be aliased if the AST is a subquery.
  // e.g., when two subqueries select from the same table
  // they need an alias to differentiate them.
  // `SELECT
  //   [SELECT * FROM issue WHERE issue.id = outer.parentId] AS parent
  //   [SELECT * FROM issue WHERE issue.parentId = outer.id] AS children
  //  FROM issue as outer`
  readonly alias?: string | undefined;

  // `select` is missing given we return all columns for now.

  // The PipelineBuilder will pick what to use to correlate
  // a subquery with a parent query. It can choose something from the
  // where conditions or choose the _first_ `related` entry.
  // Choosing the first `related` entry is almost always the best choice if
  // one exists.
  readonly where?: readonly Condition[] | undefined;

  readonly related?: readonly CorrelatedSubQuery[] | undefined;
  readonly limit?: number | undefined;
  readonly orderBy?: Ordering | undefined;
};

export type CorrelatedSubQuery = {
  /**
   * Only equality correlations are supported for now.
   * E.g., direct foreign key relationships.
   */
  readonly correlation: {
    readonly parentField: string;
    readonly childField: string;
    readonly op: '=';
  };
  readonly subquery: AST;
};

/**
 * Starting only with SimpleCondition for now.
 * ivm1 supports Conjunctions and Disjunctions.
 * We'll support them in the future.
 */
export type Condition = SimpleCondition;
export type SimpleCondition = {
  type: 'simple';
  op: SimpleOperator;

  /**
   * Not a path yet as we're currently not allowing
   * comparisons across tables. This will need to
   * be a path through the tree in the near future.
   */
  field: string;

  /**
   * `null` is absent since we do not have an `IS` or `IS NOT`
   * operator defined and `null != null` in SQL.
   */
  value: string | number | boolean | ReadonlyArray<string | number | boolean>;
};

export function normalizeAST(ast: AST): Required<AST> {
  return {
    schema: ast.schema,
    table: ast.table,
    alias: ast.alias,
    where: ast.where ? sortedWhere(ast.where) : undefined,
    related: ast.related
      ? sortedRelated(
          ast.related.map(r => ({
            correlation: {
              parentField: r.correlation.parentField,
              childField: r.correlation.childField,
              op: r.correlation.op,
            },
            subquery: normalizeAST(r.subquery),
          })),
        )
      : undefined,
    limit: ast.limit,
    orderBy: ast.orderBy,
  };
}

function sortedWhere(where: readonly Condition[]): readonly Condition[] {
  return [...where].sort(cmpCondition);
}

function sortedRelated(
  related: CorrelatedSubQuery[],
): readonly CorrelatedSubQuery[] {
  return related.sort(cmpRelated);
}

function cmpCondition(a: Condition, b: Condition): number {
  return (
    compareUTF8MaybeNull(a.field, b.field) ||
    compareUTF8MaybeNull(a.op, b.op) ||
    // Comparing the same field with the same op more than once doesn't make logical
    // sense, but is technically possible. Assume the values are of the same type and
    // sort by their String forms.
    compareUTF8MaybeNull(String(a.value), String(b.value))
  );
}

function cmpRelated(a: CorrelatedSubQuery, b: CorrelatedSubQuery): number {
  return compareUTF8(must(a.subquery.alias), must(b.subquery.alias));
}

function compareUTF8MaybeNull(a: string | null, b: string | null): number {
  if (a !== null && b !== null) {
    return compareUTF8(a, b);
  }
  if (b !== null) {
    return -1;
  }
  if (a !== null) {
    return 1;
  }
  return 0;
}
