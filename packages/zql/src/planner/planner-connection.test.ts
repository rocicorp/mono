import {expect, suite, test, vi} from 'vitest';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {CostModelCost} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {PlannerSource} from './planner-source.ts';
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

suite('PlannerConnection.setPerBranchFilter', () => {
  const fanout = () => ({fanout: 1, confidence: 'none' as const});

  /**
   * Build a connection wired to a spy cost model so we can inspect the
   * exact filter argument it receives.
   */
  function setup(connFilters?: Condition) {
    const model = vi.fn(
      (
        _t: string,
        _s: Ordering,
        _f: Condition | undefined,
        _c: PlannerConstraint | undefined,
      ): CostModelCost => ({
        startupCost: 0,
        rows: 1,
        fanout,
      }),
    );
    const source = new PlannerSource('t', model);
    const conn = source.connect([['id', 'asc']], connFilters, false);
    return {conn, model};
  }

  const cmpAEq1: Condition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'a'},
    right: {type: 'literal', value: 1},
  };
  const cmpBEq2: Condition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'b'},
    right: {type: 'literal', value: 2},
  };

  test('per-branch filter is passed to the cost model on the matching branch', () => {
    const {conn, model} = setup();

    // Branch [0] gets the per-branch filter; branch [1] does not.
    conn.setPerBranchFilter([0], cmpAEq1);
    conn.estimateCost(1, [0]);
    conn.estimateCost(1, [1]);

    const filterArgs = model.mock.calls.map(c => c[2]);
    expect(filterArgs[0]).toEqual(cmpAEq1);
    expect(filterArgs[1]).toBeUndefined();
  });

  test('per-branch filter is AND-merged with the connection-time filter', () => {
    const {conn, model} = setup(cmpBEq2);

    conn.setPerBranchFilter([0], cmpAEq1);
    conn.estimateCost(1, [0]);

    expect(model.mock.calls[0][2]).toEqual({
      type: 'and',
      conditions: [cmpBEq2, cmpAEq1],
    });
  });

  test('setPerBranchFilter invalidates the cost cache', () => {
    const {conn, model} = setup();

    conn.estimateCost(1, [0]);
    expect(model).toHaveBeenCalledTimes(1);

    // Same branch, no change → cache hit.
    conn.estimateCost(1, [0]);
    expect(model).toHaveBeenCalledTimes(1);

    // Per-branch filter changed → cache invalidated.
    conn.setPerBranchFilter([0], cmpAEq1);
    conn.estimateCost(1, [0]);
    expect(model).toHaveBeenCalledTimes(2);
  });

  test('reset() clears per-branch filters', () => {
    const {conn, model} = setup();

    conn.setPerBranchFilter([0], cmpAEq1);
    conn.estimateCost(1, [0]);
    expect(model.mock.calls[0][2]).toEqual(cmpAEq1);

    conn.reset();
    conn.estimateCost(1, [0]);
    expect(model.mock.calls[1][2]).toBeUndefined();
  });
});
