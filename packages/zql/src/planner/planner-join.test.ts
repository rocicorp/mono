import {expect, suite, test} from 'vitest';
import {UnflippableJoinError} from './planner-join.ts';
import {CONSTRAINTS, createJoin, expectedCost} from './test/helpers.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

suite('PlannerJoin', () => {
  test('initial state is semi-join, unpinned', () => {
    const {join} = createJoin();

    expect(join.kind).toBe('join');
    expect(join.type).toBe('semi');
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

  test('cannot flip when already flipped', () => {
    const {join} = createJoin();

    join.flip();
    expect(() => join.flip()).toThrow('Can only flip a semi-join');
  });

  test('maybeFlip() flips when input is child', () => {
    const {child, join} = createJoin();

    join.flipIfNeeded(child);
    expect(join.type).toBe('flipped');
  });

  test('maybeFlip() does not flip when input is parent', () => {
    const {parent, join} = createJoin();

    join.flipIfNeeded(parent);
    expect(join.type).toBe('semi');
  });

  test('reset() clears pinned and flipped state', () => {
    const {join} = createJoin();

    join.flip();
    expect(join.type).toBe('flipped');

    join.reset();
    expect(join.type).toBe('semi');
  });

  test('propagateConstraints() on semi-join sends constraints to child', () => {
    const {child, join} = createJoin();

    join.propagateConstraints([0], undefined);

    expect(child.estimateCost()).toStrictEqual(expectedCost(1));
  });

  test('propagateConstraints() on flipped join sends undefined to child', () => {
    const {child, join} = createJoin();

    join.flip();
    join.propagateConstraints([0], undefined);

    expect(child.estimateCost()).toStrictEqual(expectedCost(0));
  });

  test('propagateConstraints() on pinned flipped join merges constraints for parent', () => {
    const {parent, join} = createJoin({
      parentConstraint: CONSTRAINTS.userId,
      childConstraint: CONSTRAINTS.postId,
    });

    join.flip();

    const outputConstraint: PlannerConstraint = {name: undefined};
    join.propagateConstraints([0], outputConstraint);

    expect(parent.estimateCost()).toStrictEqual(expectedCost(2));
  });

  test('semi-join has overhead multiplier applied to cost', () => {
    const {join} = createJoin();

    // Estimate cost for semi-join (not flipped)
    const semiCost = join.estimateCost();

    // Flip and estimate cost
    join.reset();
    join.flip();
    const flippedCost = join.estimateCost();

    // Semi-join should be more expensive than flipped join due to overhead multiplier
    // The multiplier inflates runningCost only (not rows, which represents logical row count)
    expect(semiCost.runningCost).toBeGreaterThan(flippedCost.runningCost);
    expect(semiCost.rows).toBe(flippedCost.rows); // Same logical rows
  });

  test('semi-join overhead allows planner to prefer flipped joins when row counts are equal', () => {
    const {join} = createJoin();

    // Get costs for both join types
    const semiCost = join.estimateCost();

    join.reset();
    join.flip();
    const flippedCost = join.estimateCost();

    // The difference should be significant enough to affect plan selection
    // With a 1.5x multiplier, semi should be 50% more expensive
    const ratio = semiCost.runningCost / flippedCost.runningCost;
    expect(ratio).toBeGreaterThanOrEqual(1.4); // Allow some tolerance
    expect(ratio).toBeLessThanOrEqual(1.6);
  });
});
