import type {AST, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from '../planner-connection.ts';
import type {PlannerConstraint} from '../planner-constraint.ts';

/**
 * Simple cost model for testing.
 * Base cost of 100, reduced by 10 per constraint.
 * Ignores sort and filters for simplicity.
 */
export const simpleCostModel: ConnectionCostModel = (
  _sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const constraintCount = constraint ? Object.keys(constraint).length : 0;
  return Math.max(1, 100 - constraintCount * 10);
};

/**
 * Create a simple AST for testing
 */
export function createAST(
  table: string,
  options: {
    where?: Condition | undefined;
    orderBy?: Ordering | undefined;
    related?: readonly CorrelatedSubquery[] | undefined;
    alias?: string | undefined;
  } = {},
): AST {
  return {
    table,
    alias: options.alias,
    where: options.where,
    orderBy: options.orderBy ?? [['id', 'asc']],
    related: options.related,
  };
}

/**
 * Create an EXISTS/NOT EXISTS condition
 */
export function createExistsCondition(
  related: CorrelatedSubquery,
  op: 'EXISTS' | 'NOT EXISTS' = 'EXISTS',
): CorrelatedSubqueryCondition {
  return {
    type: 'correlatedSubquery',
    related,
    op,
  };
}

/**
 * Create a correlated subquery
 */
export function createCorrelatedSubquery(
  subquery: AST,
  parentField: readonly string[],
  childField: readonly string[],
): CorrelatedSubquery {
  return {
    correlation: {
      parentField: parentField as [string, ...string[]],
      childField: childField as [string, ...string[]],
    },
    subquery,
  };
}
