import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {PlannerJoin} from './planner-join.ts';
import {simpleCostModel} from './test/helpers.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {UnflippableJoinError} from './planner-graph.ts';

suite('PlannerJoin', () => {
  test('initial state is left join, unpinned', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    expect(join.kind).toBe('join');
    expect(join.type).toBe('left');
    expect(join.pinned).toBe(false);
  });

  test('can be pinned', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.pin();
    expect(join.pinned).toBe(true);
  });

  test('can be flipped when flippable', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true, // flippable
    );

    join.flip();
    expect(join.type).toBe('flipped');
  });

  test('cannot flip when not flippable (NOT EXISTS)', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      false, // NOT flippable (e.g., NOT EXISTS)
    );

    expect(() => join.flip()).toThrow(UnflippableJoinError);
  });

  test('cannot flip when pinned', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.pin();
    expect(() => join.flip()).toThrow('Cannot flip a pinned join');
  });

  test('cannot flip when already flipped', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.flip();
    expect(() => join.flip()).toThrow('Can only flip a left join');
  });

  test('maybeFlip() flips when input is child', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.maybeFlip(child);
    expect(join.type).toBe('flipped');
  });

  test('maybeFlip() does not flip when input is parent', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.maybeFlip(parent);
    expect(join.type).toBe('left');
  });

  test('reset() clears pinned and flipped state', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.flip();
    join.pin();
    expect(join.type).toBe('flipped');
    expect(join.pinned).toBe(true);

    join.reset();
    expect(join.type).toBe('left');
    expect(join.pinned).toBe(false);
  });

  test('propagateConstraints() on pinned left join sends constraints to child', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.pin();
    join.propagateConstraints([0], undefined, 'pinned');

    // Child should receive childConstraint
    const childCost = child.estimateCost();
    // simpleCostModel: 100 - 10 (1 constraint) = 90
    expect(childCost).toBe(90);
  });

  test('propagateConstraints() on pinned flipped join sends undefined to child', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    join.flip();
    join.pin();
    join.propagateConstraints([0], undefined, 'pinned');

    // Child should receive undefined constraint
    const childCost = child.estimateCost();
    // simpleCostModel: 100 - 0 (no constraints) = 100
    expect(childCost).toBe(100);
  });

  test('propagateConstraints() on pinned flipped join merges constraints for parent', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {postId: 'string'},
      true,
    );

    join.flip();
    join.pin();

    const outputConstraint: PlannerConstraint = {name: 'string'};
    join.propagateConstraints([0], outputConstraint, 'pinned');

    // Parent should receive merged constraints (outputConstraint + parentConstraint)
    const parentCost = parent.estimateCost();
    // simpleCostModel: 100 - 20 (2 constraints: userId, name) = 80
    expect(parentCost).toBe(80);
  });

  test('stores plan ID when provided', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
      42, // plan ID
    );

    expect(join.planId).toBe(42);
  });

  test('plan ID is undefined when not provided', () => {
    const parentSource = new PlannerSource('users', simpleCostModel);
    const childSource = new PlannerSource('posts', simpleCostModel);
    const parent = parentSource.connect([['id', 'asc']], undefined);
    const child = childSource.connect([['id', 'asc']], undefined);

    const join = new PlannerJoin(
      parent,
      child,
      {userId: 'string'},
      {id: 'string'},
      true,
    );

    expect(join.planId).toBeUndefined();
  });
});
