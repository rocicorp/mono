import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from '../planner-connection.ts';
import type {PlannerConstraint} from '../planner-constraint.ts';
import {PlannerSource} from '../planner-source.ts';
import type {PlannerConnection} from '../planner-connection.ts';
import {PlannerJoin} from '../planner-join.ts';
import {PlannerFanIn} from '../planner-fan-in.ts';
import {PlannerFanOut} from '../planner-fan-out.ts';
import type {CostEstimate} from '../planner-node.ts';

// ============================================================================
// Test Constants
// ============================================================================

/**
 * Base cost used by simpleCostModel when no constraints are applied.
 */
export const BASE_COST = 100;

/**
 * Cost reduction per constraint in simpleCostModel.
 */
export const CONSTRAINT_REDUCTION = 10;

/**
 * Default sort ordering used in tests.
 */
export const DEFAULT_SORT: Ordering = [['id', 'asc']];

/**
 * Common constraints used in tests.
 */
export const CONSTRAINTS = {
  userId: {userId: undefined} as PlannerConstraint,
  id: {id: undefined} as PlannerConstraint,
  postId: {postId: undefined} as PlannerConstraint,
  name: {name: undefined} as PlannerConstraint,
} as const;

/**
 * Simple cost model for testing.
 * Base cost of 100, reduced by 10 per constraint.
 * Ignores sort and filters for simplicity.
 * Returns zero startup cost for all queries (no sorting simulated).
 */
export const simpleCostModel: ConnectionCostModel = (
  _table: string,
  _sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): {startupCost: number; baseCardinality: number} => {
  const constraintCount = constraint ? Object.keys(constraint).length : 0;
  const baseCardinality = Math.max(1, 100 - constraintCount * 10);
  return {
    startupCost: 0,
    baseCardinality,
  };
};

/**
 * Calculates expected cost given a number of constraints.
 */
export function expectedCost(constraintCount: number): CostEstimate {
  const c = Math.max(1, BASE_COST - constraintCount * CONSTRAINT_REDUCTION);
  return {
    baseCardinality: c,
    runningCost: c,
    startupCost: 0,
    selectivity: 1.0,
    limit: undefined,
  };
}

export function multCost(base: CostEstimate, factor: number): CostEstimate {
  return {
    baseCardinality: base.baseCardinality * factor,
    runningCost: base.runningCost * factor,
    startupCost: base.startupCost,
    selectivity: base.selectivity,
    limit: base.limit,
  };
}

// ============================================================================
// Test Factories
// ============================================================================

/**
 * Creates a PlannerConnection for testing.
 */
export function createConnection(
  tableName = 'users',
  sort: Ordering = DEFAULT_SORT,
  filters: Condition | undefined = undefined,
): PlannerConnection {
  const source = new PlannerSource(tableName, simpleCostModel);
  return source.connect(sort, filters);
}

/**
 * Creates a PlannerJoin with parent and child connections for testing.
 */
export function createJoin(options?: {
  parentTable?: string;
  childTable?: string;
  parentConstraint?: PlannerConstraint;
  childConstraint?: PlannerConstraint;
  flippable?: boolean;
  planId?: number;
}): {
  parent: PlannerConnection;
  child: PlannerConnection;
  join: PlannerJoin;
} {
  const {
    parentTable = 'users',
    childTable = 'posts',
    parentConstraint = CONSTRAINTS.userId,
    childConstraint = CONSTRAINTS.id,
    flippable = true,
    planId = 0,
  } = options ?? {};

  const parent = createConnection(parentTable);
  const child = createConnection(childTable);

  const join = new PlannerJoin(
    parent,
    child,
    parentConstraint,
    childConstraint,
    flippable,
    planId,
  );

  return {parent, child, join};
}

/**
 * Creates a PlannerFanIn with multiple input connections for testing.
 */
export function createFanIn(
  inputCount = 2,
  tableNames?: string[],
): {
  inputs: PlannerConnection[];
  fanIn: PlannerFanIn;
} {
  const names =
    tableNames ?? Array.from({length: inputCount}, (_, i) => `table${i}`);
  const inputs = names.map(name => createConnection(name));
  const fanIn = new PlannerFanIn(inputs);

  return {inputs, fanIn};
}

/**
 * Creates a PlannerFanOut with an input connection for testing.
 */
export function createFanOut(tableName = 'users'): {
  input: PlannerConnection;
  fanOut: PlannerFanOut;
} {
  const input = createConnection(tableName);
  const fanOut = new PlannerFanOut(input);

  return {input, fanOut};
}
