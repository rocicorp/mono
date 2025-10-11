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
 * - issue: 10000 (very expensive)
 * - project: 100
 * - project_member: 1
 * - creator: 2
 *
 * Constraint reductions:
 * - creatorId constraint: divide by 5 (issue: 10000 -> 2000, then with projectId -> 200)
 * - projectId constraint: divide by 100 (issue: 10000 -> 100, project: 100 -> 1)
 *
 * This creates the following progression:
 * Initial: issue=10000, project=100, project_member=1, creator=2
 * Pick project_member (1) -> project gets projectId (100->1), issue unchanged (10000)
 * Pick creator (2) -> issue gets creatorId (10000->2000)
 * Pick project (1) with projectId constraint
 * Pick issue (2000) with creatorId constraint
 * Total: 1 + 2 + 1 + 2000 = 2004 (suboptimal)
 *
 * OR pick creator first:
 * Pick creator (2) -> issue gets creatorId (10000->2000)
 * Pick project_member (1) -> project gets projectId (100->1), issue gets projectId (2000->20)
 * Pick project (1) with projectId constraint
 * Pick issue (20) with both constraints
 * Total: 2 + 1 + 1 + 20 = 24 (optimal!)
 */
export const predictableCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  // Extract table name - the sort column format is "table.column"
  const firstColumn = sort[0]?.[0] ?? 'unknown.id';
  const tableName = firstColumn.split('.')[0];

  // Base costs
  const baseCosts: Record<string, number> = {
    issue: 10000,
    project: 100,
    project_member: 1,
    creator: 2,
  };

  let cost = baseCosts[tableName] ?? 100;

  // Apply constraint reductions
  if (constraint) {
    if ('creatorId' in constraint) {
      cost = Math.floor(cost / 5);
    }
    if ('projectId' in constraint) {
      cost = Math.floor(cost / 100);
    }
  }

  return Math.max(1, cost);
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
