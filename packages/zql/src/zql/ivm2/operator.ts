import type {SimpleOperator} from '../ast2/ast.js';
import type {Value} from './data.js';
import type {TreeDiff} from './tree-diff.js';

/**
 * An input to an operator. Operators can pull() on their inputs to request
 * additional data from upstream operators. For example to get the intial
 * results of a query, we pull() on the end node of the pipeline.
 */
export interface Input {
  pull(req: Request): Response;
}

/**
 * An output from an operator. Operators can push() changelists down the graph
 * when updates happen.
 */
export interface Output {
  push(source: Input, diff: TreeDiff): void;
}

/**
 * A Constraint is a required condition that data being returned to a Request
 * must meet. There can only ever be one constraint on a request at a time.
 * Constraints have narrower flexibility than Predicates because we need to
 * implement them client-side where we lack a real database 😢.
 */
export type Constraint = {
  field: string;
  value: Value;
};

/**
 * A Filter is a more flexible version of a Constraint. It allows for more
 * complex conditions to be applied to a query. Request filters are optional. On
 * the client we won't implement them, but on the server, we'll turn them into
 * SQLite WHERE clauses.
 */
export type Filter = {
  field: string;
  op: SimpleOperator;
  value: Value;
};

/**
 * A Request is sent with pull() to tell source what data needs to be returned.
 */
export type Request = {
  constraint: Constraint | null;
  optionalFilters: Filter[];

  // If null, it means include all subdiffs.
  restrictToSubdiffs: string[] | null;
};

export const everything = {
  constraint: null,
  optionalFilters: [],
  restrictToSubdiffs: null,
};

/**
 * A Response is the result of pull() and indicates which of the requested
 * filters the source was able to honor.
 */
export type Response = {
  diff: TreeDiff;
  // The filters that the source applied to the response. This allows the
  // Filter operator to know if it needs to reapply the filter.
  appliedFilters: Filter[];
};

/**
 * Operators are chained together, so each Operator itself is both an Input and
 * Output.
 */
export interface Operator extends Input, Output {}
