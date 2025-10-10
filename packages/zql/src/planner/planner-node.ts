import type {PlannerConstraint} from './planner-constraint.ts';

export type FromType = 'pinned' | 'unpinned' | 'terminus';

export interface PlannerNode {
  /**
   * At each step of the planning phase,
   * we need to send constraints up the tree
   * for joins we have pinned.
   *
   * `branchPattern` exists to help us track how many fetches
   * a single source connection will end up receiving.
   *
   * UnionFanOut causes a branching that incurs additional fetches.
   * Each branch potentially having a unique set of constraints,
   */
  propagateConstraints(
    branchPattern: number[],
    constraint: PlannerConstraint | undefined,
    from: FromType,
  ): void;
}
