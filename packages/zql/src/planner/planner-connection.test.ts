import {expect, suite, test} from 'vitest';
import {
  BASE_COST,
  CONSTRAINTS,
  createConnection,
  expectedCost,
} from './test/helpers.ts';

suite('PlannerConnection', () => {
  test('initial state is unpinned', () => {
    const connection = createConnection();

    expect(connection.pinned).toBe(false);
  });

  test('estimateCost() with no constraints returns base cost', () => {
    const connection = createConnection();

    expect(connection.estimateCost()).toBe(BASE_COST);
  });

  test('estimateCost() with constraints reduces cost', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId, 'unpinned');

    expect(connection.estimateCost()).toBe(expectedCost(1));
  });

  test('multiple constraints reduce cost further', () => {
    const connection = createConnection();

    connection.propagateConstraints(
      [0],
      {userId: undefined, postId: undefined},
      'unpinned',
    );

    expect(connection.estimateCost()).toBe(expectedCost(2));
  });

  test('multiple branch patterns sum costs', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId, 'unpinned');
    connection.propagateConstraints([1], CONSTRAINTS.postId, 'unpinned');

    expect(connection.estimateCost()).toBe(expectedCost(1) + expectedCost(1));
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

    connection.propagateConstraints([0], CONSTRAINTS.userId, 'unpinned');
    expect(connection.estimateCost()).toBe(expectedCost(1));

    connection.reset();

    expect(connection.estimateCost()).toBe(BASE_COST);
  });
});
