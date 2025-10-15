import type {Condition, Ordering} from '../../../../zero-protocol/src/ast.ts';
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
