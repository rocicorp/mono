import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
import type {ConnectionCostModel} from '../planner-connection.ts';
import type {PlannerConstraint} from '../planner-constraint.ts';
import {PlannerSource} from '../planner-source.ts';
import type {PlannerConnection} from '../planner-connection.ts';
import {PlannerJoin} from '../planner-join.ts';
import {PlannerFanIn} from '../planner-fan-in.ts';
import {PlannerFanOut} from '../planner-fan-out.ts';

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
  userId: {fields: {userId: undefined}, isSemiJoin: false} as PlannerConstraint,
  id: {fields: {id: undefined}, isSemiJoin: false} as PlannerConstraint,
  postId: {fields: {postId: undefined}, isSemiJoin: false} as PlannerConstraint,
  name: {fields: {name: undefined}, isSemiJoin: false} as PlannerConstraint,
} as const;

/**
 * Simple cost model for testing.
 * Base cost of 100, reduced by 10 per constraint field.
 * Applies 10x discount for semi-joins (early termination).
 * Ignores sort and filters for simplicity.
 */
export const simpleCostModel: ConnectionCostModel = (
  _table: string,
  _sort: Ordering,
  _filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): number => {
  const constraintCount = constraint ? Object.keys(constraint.fields).length : 0;
  const baseCost = Math.max(1, 100 - constraintCount * 10);
  // Apply 10x discount for semi-joins
  return constraint?.isSemiJoin ? baseCost / 10 : baseCost;
};

/**
 * Calculates expected cost given a number of constraints.
 */
export function expectedCost(constraintCount: number): number {
  return Math.max(1, BASE_COST - constraintCount * CONSTRAINT_REDUCTION);
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
