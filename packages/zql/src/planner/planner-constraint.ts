/**
 * We do not know the value a constraint will take until runtime.
 *
 * However, we do know the column.
 *
 * E.g., we know that `issue.assignee_id` will be constrained to typeof issue.assignee_id.
 *
 * We also track which join contributed the constraint so we can look up
 * per-join fanout factors for semi-join selectivity calculations.
 */
export type PlannerConstraint = Record<string, {sourceJoinId?: string}>;

/**
 * Helper to create a constraint with a source join ID.
 */
export function constraint(
  columns: Record<string, undefined>,
  sourceJoinId?: string,
): PlannerConstraint {
  const result: PlannerConstraint = {};
  for (const col in columns) {
    result[col] = sourceJoinId ? {sourceJoinId} : {};
  }
  return result;
}

/**
 * Tags an existing constraint with a source join ID.
 * Returns a new constraint with all columns tagged with the given source.
 */
export function tagConstraint(
  c: PlannerConstraint | undefined,
  sourceJoinId: string,
): PlannerConstraint | undefined {
  if (!c) return undefined;

  const result: PlannerConstraint = {};
  for (const col in c) {
    result[col] = {sourceJoinId};
  }
  return result;
}

/**
 * Multiple flipped joins will contribute extra constraints to a parent join.
 * These need to be merged.
 *
 * When both constraints have the same key, we preserve the source from the first
 * constraint (a) since it was contributed earlier in the constraint flow.
 */
export function mergeConstraints(
  a: PlannerConstraint | undefined,
  b: PlannerConstraint | undefined,
): PlannerConstraint | undefined {
  if (!a) return b;
  if (!b) return a;

  // Merge b into a, but preserve a's sources when keys overlap
  const result: PlannerConstraint = {...a};
  for (const key in b) {
    if (!(key in result)) {
      result[key] = b[key];
    }
    // If key exists in both, keep a's value (don't overwrite)
  }
  return result;
}
