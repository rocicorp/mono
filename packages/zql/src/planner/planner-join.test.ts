import {expect, suite, test} from 'vitest';
import {UnflippableJoinError} from './planner-join.ts';
import {CONSTRAINTS, createJoin, expectedCost} from './test/helpers.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

suite('PlannerJoin', () => {
  test('initial state is left join, unpinned', () => {
    const {join} = createJoin();

    expect(join.kind).toBe('join');
    expect(join.type).toBe('left');
    expect(join.pinned).toBe(false);
  });

  test('can be pinned', () => {
    const {join} = createJoin();

    join.pin();
    expect(join.pinned).toBe(true);
  });

  test('can be flipped when flippable', () => {
    const {join} = createJoin();

    join.flip();
    expect(join.type).toBe('flipped');
  });

  test('cannot flip when not flippable (NOT EXISTS)', () => {
    const {join} = createJoin({flippable: false});

    expect(() => join.flip()).toThrow(UnflippableJoinError);
  });

  test('cannot flip when pinned', () => {
    const {join} = createJoin();

    join.pin();
    expect(() => join.flip()).toThrow('Cannot flip a pinned join');
  });

  test('cannot flip when already flipped', () => {
    const {join} = createJoin();

    join.flip();
    expect(() => join.flip()).toThrow('Can only flip a left join');
  });

  test('maybeFlip() flips when input is child', () => {
    const {child, join} = createJoin();

    join.flipIfNeeded(child);
    expect(join.type).toBe('flipped');
  });

  test('maybeFlip() does not flip when input is parent', () => {
    const {parent, join} = createJoin();

    join.flipIfNeeded(parent);
    expect(join.type).toBe('left');
  });

  test('reset() clears pinned and flipped state', () => {
    const {join} = createJoin();

    join.flip();
    join.pin();
    expect(join.type).toBe('flipped');
    expect(join.pinned).toBe(true);

    join.reset();
    expect(join.type).toBe('left');
    expect(join.pinned).toBe(false);
  });

  test('propagateConstraints() on pinned left join sends constraints to child', () => {
    const {child, join} = createJoin();

    join.pin();
    join.propagateConstraints([0], undefined, 'pinned');

    expect(child.estimateCost()).toBe(expectedCost(1));
  });

  test('propagateConstraints() on pinned flipped join sends undefined to child', () => {
    const {child, join} = createJoin();

    join.flip();
    join.pin();
    join.propagateConstraints([0], undefined, 'pinned');

    expect(child.estimateCost()).toBe(expectedCost(0));
  });

  test('propagateConstraints() on pinned flipped join merges constraints for parent', () => {
    const {parent, join} = createJoin({
      parentConstraint: CONSTRAINTS.userId,
      childConstraint: CONSTRAINTS.postId,
    });

    join.flip();
    join.pin();

    const outputConstraint: PlannerConstraint = {name: undefined};
    join.propagateConstraints([0], outputConstraint, 'pinned');

    expect(parent.estimateCost()).toBe(expectedCost(2));
  });
});
