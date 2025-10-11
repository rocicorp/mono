import {expect, suite, test} from 'vitest';
import {mergeConstraints, type PlannerConstraint} from './planner-constraint.ts';

suite('mergeConstraints', () => {
  test('both undefined returns undefined', () => {
    expect(mergeConstraints(undefined, undefined)).toBeUndefined();
  });

  test('first undefined returns second', () => {
    const second: PlannerConstraint = {a: 'string'};
    expect(mergeConstraints(undefined, second)).toEqual({a: 'string'});
  });

  test('second undefined returns first', () => {
    const first: PlannerConstraint = {a: 'string'};
    expect(mergeConstraints(first, undefined)).toEqual({a: 'string'});
  });

  test('merges non-overlapping constraints', () => {
    const first: PlannerConstraint = {a: 'string'};
    const second: PlannerConstraint = {b: 'number'};
    expect(mergeConstraints(first, second)).toEqual({
      a: 'string',
      b: 'number',
    });
  });

  test('second constraint overwrites first for same key', () => {
    const first: PlannerConstraint = {a: 'string'};
    const second: PlannerConstraint = {a: 'number'};
    expect(mergeConstraints(first, second)).toEqual({a: 'number'});
  });

  test('complex merge with overlap', () => {
    const first: PlannerConstraint = {
      a: 'string',
      b: 'number',
      c: 'boolean',
    };
    const second: PlannerConstraint = {
      b: 'string',
      d: 'number',
    };
    expect(mergeConstraints(first, second)).toEqual({
      a: 'string',
      b: 'string', // overwritten
      c: 'boolean',
      d: 'number',
    });
  });
});
