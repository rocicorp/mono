import {expect, suite, test} from 'vitest';
import type {Condition} from '../../../zero-protocol/src/ast.ts';
import {
  getMultiConstraintChunkSize,
  setMultiConstraintChunkSizeForTest,
} from '../ivm/flipped-join.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {PlannerJoin, UnflippableJoinError} from './planner-join.ts';
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

    test('cost jumps by parent.startupCost at each chunk boundary', () => {
      // With chunk size = 2, ceil(N/2) gives [1,1,2,2,3] for N=[1,2,3,4,5].
      // Expected cost = ceil(N/2) * 100 + N * (parent.cost + parent.scanEst)
      //               = ceil(N/2) * 100 + N * 1
      const restore = setMultiConstraintChunkSizeForTest(2);
      try {
        expect(flippedCost(1)).toBe(1 * PARENT_STARTUP + 1);
        expect(flippedCost(2)).toBe(1 * PARENT_STARTUP + 2);
        expect(flippedCost(3)).toBe(2 * PARENT_STARTUP + 3);
        expect(flippedCost(4)).toBe(2 * PARENT_STARTUP + 4);
        expect(flippedCost(5)).toBe(3 * PARENT_STARTUP + 5);
      } finally {
        restore();
      }
    });

    test('cost respects the default chunk size at the 256 boundary', () => {
      const C = getMultiConstraintChunkSize();
      // N=C uses 1 chunk; N=C+1 uses 2 chunks.
      expect(flippedCost(1)).toBe(1 * PARENT_STARTUP + 1);
      expect(flippedCost(C)).toBe(1 * PARENT_STARTUP + C);
      expect(flippedCost(C + 1)).toBe(2 * PARENT_STARTUP + (C + 1));
      expect(flippedCost(2 * C)).toBe(2 * PARENT_STARTUP + 2 * C);
      expect(flippedCost(2 * C + 1)).toBe(3 * PARENT_STARTUP + (2 * C + 1));
    });

    test('setMultiConstraintChunkSizeForTest is observed by planner cost', () => {
      // If the planner imported the frozen constant instead of the runtime
      // accessor, changing the seam would not affect cost — this test pins
      // that the planner and IVM stay in sync.
      const defaultCost = flippedCost(256);
      const restore = setMultiConstraintChunkSizeForTest(64);
      try {
        // 256 rows → 4 chunks at size 64 vs 1 chunk at size 256.
        expect(flippedCost(256)).toBe(4 * PARENT_STARTUP + 256);
      } finally {
        restore();
      }
      // After restore, cost returns to the default chunking.
      expect(flippedCost(256)).toBe(defaultCost);
    });
  });

  suite('anti-join (NOT EXISTS) costing', () => {
    // Child filter passing 20% of rows (200 of 1000).
    const CHILD_FILTER: Condition = {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'deleted'},
      right: {type: 'literal', value: false},
    };

    // parent: 1000 rows. child: 1000 rows unfiltered, 200 filtered
    // (filter selectivity 0.2).
    function makeJoin(opts: {
      op: 'EXISTS' | 'NOT EXISTS';
      childFilters?: Condition | undefined;
      fanout?: number;
      parentLimit?: number | undefined;
    }) {
      const {op, childFilters, fanout = 1, parentLimit} = opts;
      const model: ConnectionCostModel = (table, _sort, filters) => ({
        startupCost: 0,
        rows: table === 'child' ? (filters ? 200 : 1000) : 1000,
        fanout: () => ({fanout, confidence: 'high'}) as const,
      });
      const parent = new PlannerSource('parent', model).connect(
        DEFAULT_SORT,
        undefined,
        true,
        undefined,
        parentLimit,
      );
      const child = new PlannerSource('child', model).connect(
        DEFAULT_SORT,
        childFilters,
        false,
        undefined,
        1, // existence probe, same as EXISTS
      );
      const join = new PlannerJoin(
        parent,
        child,
        CONSTRAINTS.userId,
        CONSTRAINTS.id,
        op === 'EXISTS',
        0,
        'semi',
        op,
      );
      return {parent, child, join};
    }

    test('NOT EXISTS pass rate is the complement of the EXISTS match rate', () => {
      // filter selectivity 0.2, fanout 2:
      // match rate = 1 - (1 - 0.2)^2 = 0.36 → anti pass rate = 0.64
      const {join} = makeJoin({
        op: 'NOT EXISTS',
        childFilters: CHILD_FILTER,
        fanout: 2,
      });
      const cost = join.estimateCost(1, []);
      expect(cost.selectivity).toBeCloseTo(0.64);
      expect(cost.returnedRows).toBeCloseTo(640);
    });

    test('EXISTS costing is unchanged by the op parameter', () => {
      const {join} = makeJoin({
        op: 'EXISTS',
        childFilters: CHILD_FILTER,
        fanout: 2,
      });
      const cost = join.estimateCost(1, []);
      // EXISTS keeps the historical unscaled child filter selectivity for
      // row estimates.
      expect(cost.selectivity).toBeCloseTo(0.2);
      expect(cost.returnedRows).toBeCloseTo(200);
    });

    test('unfiltered NOT EXISTS clamps to a selectivity floor instead of 0', () => {
      // Unfiltered child selectivity is 1.0 so the raw complement is 0,
      // which would estimate that no parent rows survive and zero out all
      // plan costs. The clamp keeps estimates finite and comparable.
      const {join} = makeJoin({op: 'NOT EXISTS'});
      const cost = join.estimateCost(1, []);
      expect(cost.selectivity).toBeCloseTo(0.1);
      expect(cost.returnedRows).toBeCloseTo(100);
    });

    test('anti-join pass rate scales how many parent rows must be scanned', () => {
      // Pass rate 0.64 with limit 10 → scan ~10/0.64 ≈ 15.6 parents, each
      // paying one limit-1 child probe.
      const {join} = makeJoin({
        op: 'NOT EXISTS',
        childFilters: CHILD_FILTER,
        fanout: 2,
        parentLimit: 10,
      });
      const cost = join.estimateCost(1, []);
      expect(cost.cost).toBeCloseTo(10 / 0.64);
    });

    test('NOT EXISTS child probe is costed as a limit-1 fetch per parent row', () => {
      // 1000 parents each pay a probe of scanEst 1 → cost 1000, not a full
      // 1000-row child scan per parent (10^6).
      const {join} = makeJoin({op: 'NOT EXISTS'});
      const cost = join.estimateCost(1, []);
      expect(cost.cost).toBe(1000);
    });

    test('NOT EXISTS joins must be constructed non-flippable and semi', () => {
      const model: ConnectionCostModel = () => ({
        startupCost: 0,
        rows: 100,
        fanout: () => ({fanout: 1, confidence: 'none'}) as const,
      });
      const parent = new PlannerSource('parent', model).connect(
        DEFAULT_SORT,
        undefined,
        false,
      );
      const child = new PlannerSource('child', model).connect(
        DEFAULT_SORT,
        undefined,
        false,
      );
      expect(
        () =>
          new PlannerJoin(
            parent,
            child,
            CONSTRAINTS.userId,
            CONSTRAINTS.id,
            true, // flippable NOT EXISTS is invalid
            0,
            'semi',
            'NOT EXISTS',
          ),
      ).toThrow('NOT EXISTS joins must be non-flippable semi-joins');
      expect(
        () =>
          new PlannerJoin(
            parent,
            child,
            CONSTRAINTS.userId,
            CONSTRAINTS.id,
            false,
            0,
            'flipped', // flipped NOT EXISTS is invalid
            'NOT EXISTS',
          ),
      ).toThrow('NOT EXISTS joins must be non-flippable semi-joins');
    });
  });
});
