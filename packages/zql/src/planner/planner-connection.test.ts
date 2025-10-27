import {expect, suite, test} from 'vitest';
import {
  BASE_COST,
  CONSTRAINTS,
  createConnection,
  expectedCost,
} from './test/helpers.ts';

suite('PlannerConnection', () => {
  test('estimateCost() with no constraints returns base cost', () => {
    const connection = createConnection();

    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: BASE_COST,
      runningCost: BASE_COST,
      startupCost: 0,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('estimateCost() with constraints reduces cost', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);

    expect(connection.estimateCost()).toStrictEqual(expectedCost(1));
  });

  test('multiple constraints reduce cost further', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: undefined,
      postId: undefined,
    });

    expect(connection.estimateCost()).toStrictEqual(expectedCost(2));
  });

  test('multiple branch patterns sum costs', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    connection.propagateConstraints([1], CONSTRAINTS.postId);

    const ec = expectedCost(1);
    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: ec.baseCardinality * 2,
      runningCost: ec.runningCost * 2,
      startupCost: 0,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('reset() clears propagated constraints', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    expect(connection.estimateCost()).toStrictEqual(expectedCost(1));

    connection.reset();

    expect(connection.estimateCost()).toStrictEqual({
      baseCardinality: BASE_COST,
      runningCost: BASE_COST,
      startupCost: 0,
      selectivity: 1.0,
      limit: undefined,
    });
  });
});
