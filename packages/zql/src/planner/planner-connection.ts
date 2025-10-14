import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

export type ConnectionCostModel = (
  table: string,
  sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
) => number;
