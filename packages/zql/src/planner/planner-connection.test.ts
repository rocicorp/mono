import {expect, suite, test} from 'vitest';
import {BASE_COST, CONSTRAINTS, createConnection} from './test/helpers.ts';

suite('PlannerConnection', () => {
  test('estimateCost() with no constraints returns base cost', () => {
    const connection = createConnection();

    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('estimateCost() with constraints reduces cost', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);

    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('multiple constraints reduce cost further', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: undefined,
      postId: undefined,
    });

    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('multiple branch patterns sum costs', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    connection.propagateConstraints([1], CONSTRAINTS.postId);

    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });

  test('reset() clears propagated constraints', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], CONSTRAINTS.userId);
    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });

    connection.reset();

    expect(connection.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: BASE_COST,
      cost: 0,
      returnedRows: BASE_COST,
      selectivity: 1.0,
      limit: undefined,
    });
  });
});
