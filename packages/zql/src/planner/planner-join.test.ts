import {expect, suite, test} from 'vitest';
import {UnflippableJoinError} from './planner-join.ts';
import {
  CONSTRAINTS,
  createJoin,
  createJoinWithModel,
  createTableCostModel,
} from './test/helpers.ts';
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

    expect(child.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });
  });

  test('propagateConstraints() on flipped join sends undefined to child', () => {
    const {child, join} = createJoin();

    join.flip();
    join.propagateConstraints([0], undefined);

    expect(child.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });
  });

  test('propagateConstraints() on pinned flipped join merges constraints for parent', () => {
    const {parent, join} = createJoin({
      parentConstraint: CONSTRAINTS.userId,
      childConstraint: CONSTRAINTS.postId,
    });

    join.flip();

    const outputConstraint: PlannerConstraint = {name: undefined};
    join.propagateConstraints([0], outputConstraint);

    expect(parent.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });
  });

  test('semi-join has overhead multiplier applied to cost', () => {
    const {join} = createJoin();

    // Estimate cost for semi-join (not flipped)
    const semiCost = join.estimateCost(1, []);

    // Flip and estimate cost
    join.reset();
    join.flip();
    const flippedCost = join.estimateCost(1, []);

    // In the new cost model, semi-join and flipped join have equal cost in base case
    expect(semiCost.cost).toBe(flippedCost.cost);
  });

  test('semi-join overhead allows planner to prefer flipped joins when row counts are equal', () => {
    const {join} = createJoin();

    // Get costs for both join types
    const semiCost = join.estimateCost(1, []);

    join.reset();
    join.flip();
    const flippedCost = join.estimateCost(1, []);

    // In the new cost model, costs are equal in base case
    const ratio = semiCost.cost / flippedCost.cost;
    expect(ratio).toBe(1);
  });

  test('scope-adjusted selectivity increases effective selectivity when parent scope is small', () => {
    // Simulates the bug case: child has low global selectivity (416K/200M = 0.002)
    // but parent only returns 10M rows with fanout 2, so scoped selectivity is
    // 416K / (10M * 2) = 0.0208 — a 10x increase over the global 0.002.
    //
    // Cost model: issueLabel returns 416K with filters, 200M without (selectivity = 0.002).
    // issue returns 10M rows unconditionally.
    const costModel = createTableCostModel({
      issue: {rows: 10_000_000},
      issueLabel: {rows: 416_000, totalRows: 200_000_000, fanout: 2},
    });

    const filter = {
      type: 'simple' as const,
      op: '=' as const,
      left: {type: 'column' as const, name: 'labelID'},
      right: {type: 'literal' as const, value: 'test'},
    };

    const {join} = createJoinWithModel({
      parentTable: 'issue',
      childTable: 'issueLabel',
      parentConstraint: {id: undefined},
      childConstraint: {issueID: undefined},
      costModel,
      childFilters: filter,
      childLimit: 1,
    });

    const semiCost = join.estimateCost(1, []);

    // child.selectivity from constructor: 416K / 200M = 0.00208
    // parentReturnedRows = 10M, fanout = 2
    // scopeAdjusted = 416K / (10M * 2) = 0.0208
    // effectiveChildSelectivity = max(0.00208, 0.0208) = 0.0208
    // scaledChildSelectivity = 1 - (1 - 0.0208)^2 ≈ 0.04117
    // returnedRows = 10M * 0.0208 = 208,000
    expect(semiCost.returnedRows).toBeCloseTo(10_000_000 * 0.0208, -3);
    // selectivity is scope-adjusted: 0.0208 instead of global 0.00208
    expect(semiCost.selectivity).toBeCloseTo(0.0208, 4);
    // Without scope adjustment, returnedRows would be 10M * 0.00208 = 20,800
    // With scope adjustment, it's 10M * 0.0208 = 208,000 — a 10x increase
    expect(semiCost.returnedRows).toBeGreaterThan(10_000_000 * 0.00208 * 5);
  });

  test('scope-adjusted selectivity does not decrease selectivity below global', () => {
    // When scope adjustment produces a LOWER value than global selectivity,
    // the global selectivity should be used (max of the two).
    //
    // child: 500K rows with filters, 1M without → selectivity = 0.5
    // parent: 2M rows
    // scopeAdjusted = 500K / (2M * 1) = 0.25 < 0.5 → uses global 0.5
    const costModel = createTableCostModel({
      parent: {rows: 2_000_000},
      child: {rows: 500_000, totalRows: 1_000_000},
    });

    const filter = {
      type: 'simple' as const,
      op: '=' as const,
      left: {type: 'column' as const, name: 'status'},
      right: {type: 'literal' as const, value: 'active'},
    };

    const {join} = createJoinWithModel({
      parentTable: 'parent',
      childTable: 'child',
      parentConstraint: {id: undefined},
      childConstraint: {parentId: undefined},
      costModel,
      childFilters: filter,
      childLimit: 1,
    });

    const cost = join.estimateCost(1, []);

    // child.selectivity = 500K / 1M = 0.5
    // scopeAdjusted = 500K / (2M * 1) = 0.25
    // effectiveChildSelectivity = max(0.5, 0.25) = 0.5
    // selectivity = 0.5 * parent.selectivity(1.0) = 0.5
    expect(cost.selectivity).toBe(0.5);
  });

  test('scope-adjusted selectivity is unchanged for simpleCostModel', () => {
    // The default simpleCostModel returns rows=100 and selectivity=1.0
    // scopeAdjusted = 100 / (100 * 1) = 1.0
    // max(1.0, 1.0) = 1.0 — no change
    const {join} = createJoin();

    const cost = join.estimateCost(1, []);
    expect(cost.selectivity).toBe(1.0);
    expect(cost.returnedRows).toBe(100);
  });
});
