import {expect, suite, test} from 'vitest';
import {
  mergeConstraints,
  type PlannerConstraint,
} from './planner-constraint.ts';

suite('mergeConstraints', () => {
  test('both undefined returns undefined', () => {
    expect(mergeConstraints(undefined, undefined)).toBeUndefined();
  });

  test('first undefined returns second', () => {
    const second: PlannerConstraint = {a: undefined};
    expect(mergeConstraints(undefined, second)).toEqual({a: undefined});
  });

  test('second undefined returns first', () => {
    const first: PlannerConstraint = {a: undefined};
    expect(mergeConstraints(first, undefined)).toEqual({a: undefined});
  });

  test('merges non-overlapping constraints', () => {
    const first: PlannerConstraint = {a: undefined};
    const second: PlannerConstraint = {b: undefined};
    expect(mergeConstraints(first, second)).toEqual({
      a: undefined,
      b: undefined,
    });
  });

  test('second constraint overwrites first for same key', () => {
    const first: PlannerConstraint = {a: undefined};
    const second: PlannerConstraint = {a: undefined};
    expect(mergeConstraints(first, second)).toEqual({a: undefined});
  });

  test('complex merge with overlap', () => {
    const first: PlannerConstraint = {
      a: undefined,
      b: undefined,
      c: undefined,
    };
    const second: PlannerConstraint = {
      b: undefined,
      d: undefined,
    };
    expect(mergeConstraints(first, second)).toEqual({
      a: undefined,
      b: undefined, // overwritten
      c: undefined,
      d: undefined,
    });
  });
});
