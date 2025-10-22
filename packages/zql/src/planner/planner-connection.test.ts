import {expect, suite, test} from 'vitest';
import {
  BASE_COST,
  CONSTRAINTS,
  createConnection,
  expectedCost,
} from './test/helpers.ts';
import type {PlannerNode} from './planner-node.ts';

const unpinned = {
  pinned: false,
} as PlannerNode;

suite('PlannerConnection', () => {
  test('initial state is unpinned', () => {
    const connection = createConnection();

    expect(connection.pinned).toBe(false);
  });

  test('estimateCost() with no constraints returns base cost', () => {
    const connection = createConnection();

    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: BASE_COST,
      runningCost: BASE_COST,
    });
  });

  test('estimateCost() with constraints reduces cost', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId, unpinned);

    expect(connection.estimateCost()).toStrictEqual(expectedCost(1));
  });

  test('multiple constraints reduce cost further', () => {
    const connection = createConnection();

    connection.propagateConstraints(
      [0],
      {userId: undefined, postId: undefined},
      unpinned,
    );

    expect(connection.estimateCost()).toStrictEqual(expectedCost(2));
  });

  test('multiple branch patterns sum costs', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId, unpinned);
    connection.propagateConstraints([1], CONSTRAINTS.postId, unpinned);

    const ec = expectedCost(1);
    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: ec.baseCardinality * 2,
      runningCost: ec.runningCost * 2,
    });
  });

  test('reset() clears pinned state', () => {
    const connection = createConnection();

    connection.pinned = true;
    expect(connection.pinned).toBe(true);

    connection.reset();
    expect(connection.pinned).toBe(false);
  });

  test('reset() clears propagated constraints', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId, unpinned);
    expect(connection.estimateCost()).toStrictEqual(expectedCost(1));

    connection.reset();

    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: BASE_COST,
      runningCost: BASE_COST,
    });
  });
});
