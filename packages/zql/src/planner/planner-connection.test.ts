import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {simpleCostModel} from './test/helpers.ts';
import type {PlannerConstraint} from './planner-constraint.ts';

suite('PlannerConnection', () => {
  test('initial state is unpinned', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    expect(connection.pinned).toBe(false);
  });

  test('estimateCost() with no constraints returns base cost', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    const cost = connection.estimateCost();
    // simpleCostModel: base 100, no constraints
    expect(cost).toBe(100);
  });

  test('estimateCost() with constraints reduces cost', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    const constraint: PlannerConstraint = {userId: undefined};
    connection.propagateConstraints([0], constraint, 'unpinned');

    const cost = connection.estimateCost();
    // simpleCostModel: 100 - 10 (1 constraint) = 90
    expect(cost).toBe(90);
  });

  test('multiple constraints reduce cost further', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    const constraint: PlannerConstraint = {
      userId: undefined,
      postId: undefined,
    };
    connection.propagateConstraints([0], constraint, 'unpinned');

    const cost = connection.estimateCost();
    // simpleCostModel: 100 - 20 (2 constraints) = 80
    expect(cost).toBe(80);
  });

  test('multiple branch patterns sum costs', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    // Each branch pattern calls estimateCost once
    // Path [0] with constraint {userId: undefined}
    connection.propagateConstraints([0], {userId: undefined}, 'unpinned');
    // Path [1] with constraint {postId: undefined}
    connection.propagateConstraints([1], {postId: undefined}, 'unpinned');

    const cost = connection.estimateCost();
    // simpleCostModel: (100 - 10) + (100 - 10) = 180
    expect(cost).toBe(180);
  });

  test('reset() clears pinned state', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    connection.pinned = true;
    expect(connection.pinned).toBe(true);

    connection.reset();
    expect(connection.pinned).toBe(false);
  });

  test('reset() clears propagated constraints', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const connection = source.connect([['id', 'asc']], undefined);

    const constraint: PlannerConstraint = {userId: undefined};
    connection.propagateConstraints([0], constraint, 'unpinned');

    let cost = connection.estimateCost();
    expect(cost).toBe(90); // Cost reduced due to constraint

    connection.reset();

    cost = connection.estimateCost();
    expect(cost).toBe(100); // Cost back to base after reset
  });
});
