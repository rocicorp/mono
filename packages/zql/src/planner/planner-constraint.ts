import type {ValueType} from '../../../zero-protocol/src/client-schema.ts';

export interface PlannerConstraint {
  [column: string]: ValueType;
}

export function mergeConstraints(
  a: PlannerConstraint | undefined,
  b: PlannerConstraint | undefined,
): PlannerConstraint | undefined {
  if (!a && !b) return undefined;
  const merged: PlannerConstraint = {};
  if (a) Object.assign(merged, a);
  if (b) Object.assign(merged, b);
  return merged;
}
