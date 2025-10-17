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
    const second: PlannerConstraint = {fields: {a: undefined}, isSemiJoin: false};
    expect(mergeConstraints(undefined, second)).toEqual({fields: {a: undefined}, isSemiJoin: false});
  });

  test('second undefined returns first', () => {
    const first: PlannerConstraint = {fields: {a: undefined}, isSemiJoin: false};
    expect(mergeConstraints(first, undefined)).toEqual({fields: {a: undefined}, isSemiJoin: false});
  });

  test('merges non-overlapping constraints', () => {
    const first: PlannerConstraint = {fields: {a: undefined}, isSemiJoin: false};
    const second: PlannerConstraint = {fields: {b: undefined}, isSemiJoin: false};
    expect(mergeConstraints(first, second)).toEqual({
      fields: {
        a: undefined,
        b: undefined,
      },
      isSemiJoin: false,
    });
  });

  test('second constraint overwrites first for same key', () => {
    const first: PlannerConstraint = {fields: {a: undefined}, isSemiJoin: false};
    const second: PlannerConstraint = {fields: {a: undefined}, isSemiJoin: false};
    expect(mergeConstraints(first, second)).toEqual({fields: {a: undefined}, isSemiJoin: false});
  });

  test('complex merge with overlap', () => {
    const first: PlannerConstraint = {
      fields: {
        a: undefined,
        b: undefined,
        c: undefined,
      },
      isSemiJoin: false,
    };
    const second: PlannerConstraint = {
      fields: {
        b: undefined,
        d: undefined,
      },
      isSemiJoin: true, // Test that isSemiJoin is preserved from either constraint
    };
    expect(mergeConstraints(first, second)).toEqual({
      fields: {
        a: undefined,
        b: undefined, // overwritten
        c: undefined,
        d: undefined,
      },
      isSemiJoin: true, // Should be true if either has it
    });
  });
});
