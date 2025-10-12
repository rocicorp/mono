import type {
  AST,
  Condition,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Ordering,
} from '../../../../zero-protocol/src/ast.ts';
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
 * Linear chain cost model: A → B → C
 * Tests basic constraint propagation through a chain.
 *
 * Base costs: A=1000, B=100, C=10
 * Constraint effect: Each level divides parent by 10
 *
 * Optimal: C(10) → B(1) → C(1) = 12
 */
export const linearChainCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';
  const baseCosts: Record<string, number> = {
    A: 1000,
    B: 100,
    C: 10,
  };

  let cost = baseCosts[tableName] ?? 100;

  // Each constraint divides cost by 10
  if (constraint) {
    const constraintCount = Object.keys(constraint).length;
    for (let i = 0; i < constraintCount; i++) {
      cost = Math.floor(cost / 10);
    }
  }

  return Math.max(1, cost);
};

/**
 * Star schema cost model: Central with 3 satellites
 * Tests multiple independent constraints reducing central table.
 *
 * Base costs: central=1000, sat1=100, sat2=50, sat3=10
 * Each satellite constraint divides central by 2
 *
 * Optimal: sat3(10) → sat2(50) → sat1(100) → central(125) = 285
 */
export const starSchemaCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';
  const baseCosts: Record<string, number> = {
    central: 1000,
    sat1: 100,
    sat2: 50,
    sat3: 10,
  };

  let cost = baseCosts[tableName] ?? 100;

  // For central table, each satellite constraint divides by 2
  if (tableName === 'central' && constraint) {
    const constraintCount = Object.keys(constraint).length;
    for (let i = 0; i < constraintCount; i++) {
      cost = Math.floor(cost / 2);
    }
  }

  return Math.max(1, cost);
};

/**
 * Diamond pattern cost model: Two paths converge at root
 * Tests constraints from different branches helping the root.
 *
 * Base costs: root=10000, left=50, right=100, bottom=1
 * leftId divides root by 10, rightId divides root by 5
 * bottomId divides right by 10
 *
 * Optimal: bottom(1) → right(10) → left(50) → root(200) = 261
 */
export const diamondCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';
  const baseCosts: Record<string, number> = {
    root: 10000,
    left: 50,
    right: 100,
    bottom: 1,
  };

  let cost = baseCosts[tableName] ?? 100;

  if (constraint) {
    if (tableName === 'root') {
      if ('leftId' in constraint) {
        cost = Math.floor(cost / 10);
      }
      if ('rightId' in constraint) {
        cost = Math.floor(cost / 5);
      }
    } else if (tableName === 'right') {
      if ('bottomId' in constraint) {
        cost = Math.floor(cost / 10);
      }
    }
  }

  return Math.max(1, cost);
};

/**
 * Wide vs narrow branches cost model
 * Tests planner picks narrow (selective) paths first.
 *
 * Base costs: main=10000, wide=1000, narrow=10
 * Each constraint divides main by 100
 *
 * Optimal: narrow(10) → wide(1000) → main(1) = 1011
 */
export const wideNarrowCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';
  const baseCosts: Record<string, number> = {
    main: 10000,
    wide: 1000,
    narrow: 10,
  };

  let cost = baseCosts[tableName] ?? 100;

  // For main table, each constraint divides by 100
  if (tableName === 'main' && constraint) {
    const constraintCount = Object.keys(constraint).length;
    for (let i = 0; i < constraintCount; i++) {
      cost = Math.floor(cost / 100);
    }
  }

  return Math.max(1, cost);
};

/**
 * Deep nesting cost model: A → B → C → D → E
 * Tests multi-level constraint propagation.
 *
 * Base costs: A=10000, B=1000, C=100, D=10, E=1
 * Each parent-child constraint divides the parent by 10
 *
 * Optimal: E(1) → D(1) → C(1) → B(1) → A(1) = 5
 */
export const deepNestingCostModel: ConnectionCostModel = (
  sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const tableName = sort[0]?.[0]?.split('.')[0] ?? 'unknown';
  const baseCosts: Record<string, number> = {
    A: 10000,
    B: 1000,
    C: 100,
    D: 10,
    E: 1,
  };

  let cost = baseCosts[tableName] ?? 100;

  // Each constraint divides cost by 10
  if (constraint) {
    const constraintCount = Object.keys(constraint).length;
    for (let i = 0; i < constraintCount; i++) {
      cost = Math.floor(cost / 10);
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
