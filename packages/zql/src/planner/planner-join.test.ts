import {expect, suite, test} from 'vitest';
import {
  getMultiConstraintChunkSize,
  setMultiConstraintChunkSizeForTest,
} from '../ivm/flipped-join.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {
  FLIP_IVM_PER_CHILD_OVERHEAD,
  PlannerJoin,
  UnflippableJoinError,
} from './planner-join.ts';
import {PlannerSource} from './planner-source.ts';
import {CONSTRAINTS, createJoin, DEFAULT_SORT} from './test/helpers.ts';

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

  test('flipped-join cost includes per-child IVM overhead', () => {
    const {child, join} = createJoin();

    // Estimate cost for semi-join (not flipped)
    const semiCost = join.estimateCost(1, []);

    // Flip and estimate cost
    join.reset();
    join.flip();
    const flippedCost = join.estimateCost(1, []);

    // Flipped now carries an eager-load + chunk-priming penalty that semi
    // doesn't pay (see FLIP_IVM_PER_CHILD_OVERHEAD docstring).
    const childRows = child.estimateCost(1, []).returnedRows;
    expect(flippedCost.cost).toBe(
      semiCost.cost + childRows * FLIP_IVM_PER_CHILD_OVERHEAD,
    );
  });

  test('semi is preferred over flipped at equal row counts', () => {
    const {join} = createJoin();

    const semiCost = join.estimateCost(1, []);

    join.reset();
    join.flip();
    const flippedCost = join.estimateCost(1, []);

    // The IVM overhead added to flipped means semi wins when row counts
    // are equal — flipped should only be chosen when its row-count savings
    // exceed the overhead.
    expect(semiCost.cost).toBeLessThan(flippedCost.cost);
  });

  // Flipped join batches child→parent lookups into chunks of
  // getMultiConstraintChunkSize(). parent.startupCost is paid once per chunk,
  // so cost should step up by `parent.startupCost` whenever child.scanEst
  // crosses a chunk boundary. These tests guard against off-by-one in the
  // Math.ceil divisor and against the planner and IVM drifting out of sync.
  suite('flipped join chunk-boundary cost', () => {
    const PARENT_STARTUP = 100;

    // Cost model where the parent has startupCost > 0 (paid per IN-list
    // query) and the child returns `childRows` rows.
    function makeModel(childRows: number): ConnectionCostModel {
      return (table, _sort, _filters, _constraint) => {
        const fanout = () => ({fanout: 1, confidence: 'none'}) as const;
        if (table === 'parent') {
          return {startupCost: PARENT_STARTUP, rows: 1, fanout};
        }
        return {startupCost: 0, rows: childRows, fanout};
      };
    }

    function flippedCost(childRows: number): number {
      const model = makeModel(childRows);
      const parentSource = new PlannerSource('parent', model);
      const childSource = new PlannerSource('child', model);
      const parent = parentSource.connect(DEFAULT_SORT, undefined, false);
      const child = childSource.connect(DEFAULT_SORT, undefined, false);
      const join = new PlannerJoin(
        parent,
        child,
        CONSTRAINTS.userId,
        CONSTRAINTS.id,
        true,
        0,
      );
      join.flip();
      return join.estimateCost(1, []).cost;
    }

    // Per-child IVM overhead added by FLIP_IVM_PER_CHILD_OVERHEAD.
    const OVH = FLIP_IVM_PER_CHILD_OVERHEAD;

    test('cost jumps by parent.startupCost at each chunk boundary', () => {
      // With chunk size = 2, ceil(N/2) gives [1,1,2,2,3] for N=[1,2,3,4,5].
      // Expected cost = ceil(N/2) * 100 + N * (parent.cost + parent.scanEst + OVH)
      //               = ceil(N/2) * 100 + N * (1 + OVH)
      const restore = setMultiConstraintChunkSizeForTest(2);
      try {
        expect(flippedCost(1)).toBe(1 * PARENT_STARTUP + 1 * (1 + OVH));
        expect(flippedCost(2)).toBe(1 * PARENT_STARTUP + 2 * (1 + OVH));
        expect(flippedCost(3)).toBe(2 * PARENT_STARTUP + 3 * (1 + OVH));
        expect(flippedCost(4)).toBe(2 * PARENT_STARTUP + 4 * (1 + OVH));
        expect(flippedCost(5)).toBe(3 * PARENT_STARTUP + 5 * (1 + OVH));
      } finally {
        restore();
      }
    });

    test('cost respects the default chunk size at the 256 boundary', () => {
      const C = getMultiConstraintChunkSize();
      // N=C uses 1 chunk; N=C+1 uses 2 chunks.
      expect(flippedCost(1)).toBe(1 * PARENT_STARTUP + 1 * (1 + OVH));
      expect(flippedCost(C)).toBe(1 * PARENT_STARTUP + C * (1 + OVH));
      expect(flippedCost(C + 1)).toBe(2 * PARENT_STARTUP + (C + 1) * (1 + OVH));
      expect(flippedCost(2 * C)).toBe(2 * PARENT_STARTUP + 2 * C * (1 + OVH));
      expect(flippedCost(2 * C + 1)).toBe(
        3 * PARENT_STARTUP + (2 * C + 1) * (1 + OVH),
      );
    });

    test('setMultiConstraintChunkSizeForTest is observed by planner cost', () => {
      // If the planner imported the frozen constant instead of the runtime
      // accessor, changing the seam would not affect cost — this test pins
      // that the planner and IVM stay in sync.
      const defaultCost = flippedCost(256);
      const restore = setMultiConstraintChunkSizeForTest(64);
      try {
        // 256 rows → 4 chunks at size 64 vs 1 chunk at size 256.
        expect(flippedCost(256)).toBe(4 * PARENT_STARTUP + 256 * (1 + OVH));
      } finally {
        restore();
      }
      // After restore, cost returns to the default chunking.
      expect(flippedCost(256)).toBe(defaultCost);
    });
  });
});
