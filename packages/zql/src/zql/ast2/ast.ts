/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];
export type Ordering = readonly OrderPart[];

export type SimpleOperator = EqualityOps | OrderOps | LikeOps;
export type EqualityOps = '=' | '!=';
export type OrderOps = '<' | '>' | '<=' | '>=';
export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';

export type AST = {
  readonly type: 'unmoored' | 'anchored';
  // Table can be undefined if the query is an anchored subquery.
  // Anchored subqueries start at a _row_ in a table rather than at a table.
  // Example: `SELECT *, [true WHERE row.title LIKE '%foo%'] as hasFoo FROM table`
  // This would return all rows from table as well as a sub-row when the row's
  // title contains foo.
  readonly table?: string | undefined;

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
  readonly where?: Condition[] | undefined;
  readonly related?: (FieldRelationship | JunctionRelationship)[] | undefined;

  readonly subqueries?: readonly AST[] | undefined;
  readonly limit?: number | undefined;
  readonly orderBy?: Ordering | undefined;
};

type FieldRelationship = {
  readonly sourceField: string;
  readonly destField: string;
  readonly destTable: string;
};

type JunctionRelationship = {
  readonly sourceField: string;
  readonly junctionTable: string;
  readonly junctionSourceField: string;
  readonly junctionDestField: string;
  readonly destField: string;
  readonly destTable: string;
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
  value: string | number | boolean;
};
