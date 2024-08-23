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
  readonly table: string;
  readonly alias?: string | undefined;

  // `select` is missing given we return all columns for now.

  readonly subqueries?: readonly SubQuery[] | undefined;
  readonly where?: Condition | undefined;
  readonly limit?: number | undefined;
  readonly orderBy: Ordering;
};

export type SubQuery = {
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
  value: string | number | boolean;
};
