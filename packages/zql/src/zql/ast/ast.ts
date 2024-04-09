// Going for a subset of the SQL `SELECT` grammar
// https://www.sqlite.org/lang_select.html

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
  readonly table?: string | undefined;
  readonly alias?: number | undefined;
  readonly select?: [string, string][] | undefined;
  readonly aggregate?: Aggregation[];
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
  readonly groupBy?: string[];
  readonly orderBy: Ordering;
  // readonly after?: Primitive;
};

export type Condition = SimpleCondition | Conjunction;
export type Conjunction = {
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
