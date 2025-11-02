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
      userId: {},
      postId: {},
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

suite('PlannerConnection - Constraint Source Tracking', () => {
  test('getConstraintsBySource() returns constraint for specific source', () => {
    const connection = createConnection();

    // Propagate constraints tagged with different source join IDs
    connection.propagateConstraints([0], {
      userId: {sourceJoinId: 'join-1'},
      postId: {sourceJoinId: 'join-2'},
    });

    // Trigger cost estimation to populate fanout cache
    connection.estimateCost(1, [0]);

    // Verify we can retrieve constraints by source
    expect(connection.getConstraintsBySource([0], 'join-1')).toEqual({
      userId: {sourceJoinId: 'join-1'},
    });

    expect(connection.getConstraintsBySource([0], 'join-2')).toEqual({
      postId: {sourceJoinId: 'join-2'},
    });
  });

  test('getConstraintsBySource() returns undefined for unknown source', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: {sourceJoinId: 'join-1'},
    });

    expect(connection.getConstraintsBySource([0], 'join-999')).toBeUndefined();
  });

  test('multiple sources contribute to same branch pattern', () => {
    const connection = createConnection();

    // First propagation with source join-1
    connection.propagateConstraints([0], {
      userId: {sourceJoinId: 'join-1'},
    });

    // Second propagation with source join-2
    connection.propagateConstraints([0], {
      postId: {sourceJoinId: 'join-2'},
    });

    // Trigger cost estimation
    connection.estimateCost(1, [0]);

    // Both sources should be retrievable
    expect(connection.getConstraintsBySource([0], 'join-1')).toEqual({
      userId: {sourceJoinId: 'join-1'},
    });

    expect(connection.getConstraintsBySource([0], 'join-2')).toEqual({
      postId: {sourceJoinId: 'join-2'},
    });
  });

  test('different branch patterns store constraints separately', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: {sourceJoinId: 'join-1'},
    });

    connection.propagateConstraints([1], {
      postId: {sourceJoinId: 'join-2'},
    });

    connection.estimateCost(1, [0]);
    connection.estimateCost(1, [1]);

    // Each branch pattern has its own constraints
    expect(connection.getConstraintsBySource([0], 'join-1')).toEqual({
      userId: {sourceJoinId: 'join-1'},
    });

    expect(connection.getConstraintsBySource([1], 'join-2')).toEqual({
      postId: {sourceJoinId: 'join-2'},
    });

    // Cross-branch access returns undefined
    expect(connection.getConstraintsBySource([0], 'join-2')).toBeUndefined();
    expect(connection.getConstraintsBySource([1], 'join-1')).toBeUndefined();
  });

  test('reset() clears constraint source tracking', () => {
    const connection = createConnection();

    connection.propagateConstraints([0], {
      userId: {sourceJoinId: 'join-1'},
    });

    connection.estimateCost(1, [0]);

    expect(connection.getConstraintsBySource([0], 'join-1')).toEqual({
      userId: {sourceJoinId: 'join-1'},
    });

    connection.reset();

    // After reset, constraints should be cleared
    expect(connection.getConstraintsBySource([0], 'join-1')).toBeUndefined();
  });
});
