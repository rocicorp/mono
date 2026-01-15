import {expect, suite, test} from 'vitest';
import {BASE_COST, CONSTRAINTS, createConnection} from './test/helpers.ts';

suite('PlannerConnection', () => {
  test('estimateCost() with no constraints returns base cost', () => {
    const connection = createConnection();

    const result = connection.estimateCost(1, []);
    expect(result).toMatchObject({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
    expect(typeof result.fanout).toBe('function');
  });

  test('estimateCost() with constraints reduces cost', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);

    // Query branch [0] which has the constraint
    const result = connection.estimateCost(1, [0]);
    expect(result).toMatchObject({
      startupCost: 0,
      scanEst: 90, // BASE_COST - 1 constraint * 10
      cost: 0,
      returnedRows: 90,
      selectivity: 1.0,
      limit: undefined,
    });
    expect(typeof result.fanout).toBe('function');
  });

  test('multiple constraints reduce cost further', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: undefined,
      postId: undefined,
    });

    // Query branch [0] which has 2 constraints
    const result = connection.estimateCost(1, [0]);
    expect(result).toMatchObject({
      startupCost: 0,
      scanEst: 80, // BASE_COST - 2 constraints * 10
      cost: 0,
      returnedRows: 80,
      selectivity: 1.0,
      limit: undefined,
    });
    expect(typeof result.fanout).toBe('function');
  });

  test('multiple branch patterns sum costs', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    connection.propagateConstraints([1], CONSTRAINTS.postId);

    // Each branch has different constraints
    const result0 = connection.estimateCost(1, [0]);
    expect(result0).toMatchObject({
      startupCost: 0,
      scanEst: 90, // BASE_COST - 1 constraint * 10
      cost: 0,
      returnedRows: 90,
      selectivity: 1.0,
      limit: undefined,
    });

    const result1 = connection.estimateCost(1, [1]);
    expect(result1).toMatchObject({
      startupCost: 0,
      scanEst: 90, // BASE_COST - 1 constraint * 10
      cost: 0,
      returnedRows: 90,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('reset() clears propagated constraints', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    const resultBeforeReset = connection.estimateCost(1, [0]);
    expect(resultBeforeReset).toMatchObject({
      startupCost: 0,
      scanEst: 90, // Constrained
      cost: 0,
      returnedRows: 90,
      selectivity: 1.0,
      limit: undefined,
    });

    connection.reset();

    const resultAfterReset = connection.estimateCost(1, [0]);
    expect(resultAfterReset).toMatchObject({
      startupCost: 0,
      scanEst: BASE_COST, // Back to unconstrained
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });
});
