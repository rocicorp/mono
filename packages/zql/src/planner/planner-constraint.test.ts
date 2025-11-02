import {expect, suite, test} from 'vitest';
import {
  mergeConstraints,
  tagConstraint,
  type PlannerConstraint,
} from './planner-constraint.ts';

suite('mergeConstraints', () => {
  test('both undefined returns undefined', () => {
    expect(mergeConstraints(undefined, undefined)).toBeUndefined();
  });

  test('first undefined returns second', () => {
    const second: PlannerConstraint = {a: {}};
    expect(mergeConstraints(undefined, second)).toEqual({a: {}});
  });

  test('second undefined returns first', () => {
    const first: PlannerConstraint = {a: {}};
    expect(mergeConstraints(first, undefined)).toEqual({a: {}});
  });

  test('merges non-overlapping constraints', () => {
    const first: PlannerConstraint = {a: {}};
    const second: PlannerConstraint = {b: {}};
    expect(mergeConstraints(first, second)).toEqual({
      a: {},
      b: {},
    });
  });

  test('preserves first constraint source for same key', () => {
    const first: PlannerConstraint = {a: {sourceJoinId: 'join1'}};
    const second: PlannerConstraint = {a: {sourceJoinId: 'join2'}};
    expect(mergeConstraints(first, second)).toEqual({
      a: {sourceJoinId: 'join1'},
    });
  });

  test('complex merge with overlap', () => {
    const first: PlannerConstraint = {
      a: {sourceJoinId: 'join1'},
      b: {sourceJoinId: 'join1'},
      c: {},
    };
    const second: PlannerConstraint = {
      b: {sourceJoinId: 'join2'},
      d: {},
    };
    expect(mergeConstraints(first, second)).toEqual({
      a: {sourceJoinId: 'join1'},
      b: {sourceJoinId: 'join1'}, // preserved from first
      c: {},
      d: {},
    });
  });
});

suite('tagConstraint', () => {
  test('undefined constraint returns undefined', () => {
    expect(tagConstraint(undefined, 'join1')).toBeUndefined();
  });

  test('tags all constraint keys with source join ID', () => {
    const constraint: PlannerConstraint = {
      userId: {},
      postId: {},
    };
    expect(tagConstraint(constraint, 'join-5')).toEqual({
      userId: {sourceJoinId: 'join-5'},
      postId: {sourceJoinId: 'join-5'},
    });
  });

  test('overwrites existing source join IDs', () => {
    const constraint: PlannerConstraint = {
      userId: {sourceJoinId: 'join-1'},
      postId: {},
    };
    expect(tagConstraint(constraint, 'join-2')).toEqual({
      userId: {sourceJoinId: 'join-2'},
      postId: {sourceJoinId: 'join-2'},
    });
  });
});
