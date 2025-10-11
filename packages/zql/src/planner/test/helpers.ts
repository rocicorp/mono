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
 * Predictable cost model for testing optimal plan selection.
 *
 * Base costs by table:
 * - issue: 1000
 * - project: 100
 * - project_member: 1
 * - creator: 2
 *
 * Constraint reductions:
 * - creatorId constraint: divide by 20
 * - projectId constraint: divide by 10
 * - memberId constraint: divide by 100
 */
export const predictableCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  // Determine table name from sort (first column)
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';

  // Base costs
  const baseCosts: Record<string, number> = {
    issue: 1000,
    project: 100,
    project_member: 1,
    creator: 2,
  };

  let cost = baseCosts[tableName] ?? 100;

  // Apply constraint reductions
  if (constraint) {
    if ('creatorId' in constraint) {
      cost = cost / 20;
    }
    if ('projectId' in constraint) {
      cost = cost / 10;
    }
    if ('memberId' in constraint) {
      cost = cost / 100;
    }
  }

  return Math.max(1, Math.floor(cost));
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
