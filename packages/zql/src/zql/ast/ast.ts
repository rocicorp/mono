import {compareUTF8} from 'compare-utf8';
import {must} from 'shared/src/must.js';
import {Bound} from '../ivm/skip.js';

/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];
export type Ordering = readonly OrderPart[];

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
  readonly start?: Bound | undefined;
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
  // If a hop in the subquery chain should be hidden from the output view.
  // A common example is junction edges. The query API provides the illusion
  // that they don't exist: `issue.related('labels')` instead of `issue.related('issue_labels').related('labels')`.
  // To maintain this illusion, the junction edge should be hidden.
  // When `hidden` is set to true, this hop will not be included in the output view
  // but its children will be.
  readonly hidden?: boolean | undefined;
};

/**
 * Starting only with SimpleCondition for now.
 * ivm1 supports Conjunctions and Disjunctions.
 * We'll support them in the future.
 */
export type Condition = SimpleCondition | ParameterizedCondition;
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
export type ParameterizedCondition = {
  type: 'parameterized';
  op: SimpleOperator;
  field: string;
  value: Parameter;
};

/**
 * A parameter is a value that is not known at the time the query is written
 * and is resolved at runtime.
 *
 * StaticParameters refer to something provided by the caller.
 * StaticParameters are injected when the query pipeline is built from the AST
 * and do not change for the life of that pipeline.
 *
 * An example StaticParameter is the current authentication data.
 * When a user is authenticated, queries on the server have access
 * to the user's authentication data in order to evaluate authorization rules.
 * Authentication data doesn't change over the life of a query as a change
 * in auth data would represent a log-in / log-out of the user.
 *
 * AncestorParameters refer to rows encountered while running the query.
 * They are used by subqueries to refer to rows emitted by parent queries.
 */
type Parameter = StaticParameter;
type StaticParameter = {
  type: 'static';
  // The "namespace" of the injected parameter.
  // Write authorization will send the value of a row
  // prior to the mutation being run (preMutationRow).
  // Read and write authorization will both send the
  // current authentication data (authData).
  anchor: 'authData' | 'preMutationRow';
  field: string;
};

export function normalizeAST(ast: AST): Required<AST> {
  return {
    schema: ast.schema,
    table: ast.table,
    alias: ast.alias,
    where: ast.where ? sortedWhere(ast.where) : undefined,
    related: ast.related
      ? sortedRelated(
          ast.related.map(
            r =>
              ({
                correlation: {
                  parentField: r.correlation.parentField,
                  childField: r.correlation.childField,
                  op: r.correlation.op,
                },
                hidden: r.hidden,
                subquery: normalizeAST(r.subquery),
              }) satisfies Required<CorrelatedSubQuery>,
          ),
        )
      : undefined,
    start: ast.start,
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
