/**
 * We do not know the value a constraint will take until runtime.
 *
 * However, we do know the column.
 *
 * E.g., we know that `issue.assignee_id` will be constrained to typeof issue.assignee_id.
 *
 * The isSemiJoin flag indicates that this constraint comes from a semi-join (EXISTS)
 * which can terminate early after finding the first match. This allows the cost model
 * to apply a selectivity discount (typically 10x) to account for early termination.
 */
export type PlannerConstraint = {
  fields: Record<string, undefined>;
  isSemiJoin?: boolean;
};

/**
 * Multiple flipped joins will contribute extra constraints to a parent join.
 * These need to be merged.
 *
 * When merging:
 * - Combines field constraints from both
 * - Preserves isSemiJoin flag if either constraint has it
 */
export function mergeConstraints(
  a: PlannerConstraint | undefined,
  b: PlannerConstraint | undefined,
): PlannerConstraint | undefined {
  if (!a) return b;
  if (!b) return a;

  const merged: PlannerConstraint = {
    fields: {...a.fields, ...b.fields},
  };

  // Merge isSemiJoin: true if either is true, false if both are false, omit if both omitted
  const aHasSemiJoin = 'isSemiJoin' in a;
  const bHasSemiJoin = 'isSemiJoin' in b;

  if (aHasSemiJoin || bHasSemiJoin) {
    merged.isSemiJoin = a.isSemiJoin || b.isSemiJoin || false;
  }

  return merged;
}
