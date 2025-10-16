/**
 * We do not know the value a constraint will take until runtime.
 *
 * However, we do know the column.
 *
 * E.g., we know that `issue.assignee_id` will be constrained to typeof issue.assignee_id.
 */
export type PlannerConstraint = Record<string, undefined>;

/**
 * Multiple flipped joins will contribute extra constraints to a parent join.
 * These need to be merged.
 */
export function mergeConstraints(
  a: PlannerConstraint | undefined,
  b: PlannerConstraint | undefined,
): PlannerConstraint | undefined {
  if (!a) return b;
  if (!b) return a;
  return {...a, ...b};
}
