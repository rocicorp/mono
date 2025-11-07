import {expect, suite, test} from 'vitest';
import {CONSTRAINTS, createConnection, createFanIn} from './test/helpers.ts';
import type {PlannerNode} from './planner-node.ts';
import {PlannerSource} from './planner-source.ts';
import type {ConnectionCostModel, CostModelCost} from './planner-connection.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import {PlannerFanIn} from './planner-fan-in.ts';

const unpinned = {
  pinned: false,
} as PlannerNode;

suite('PlannerFanIn', () => {
  test('initial state is FI type', () => {
    const {fanIn} = createFanIn();

    expect(fanIn.kind).toBe('fan-in');
    expect(fanIn.type).toBe('FI');
  });

  test('can be converted to UFI', () => {
    const {fanIn} = createFanIn();
    expect(fanIn.type).toBe('FI');

    fanIn.convertToUFI();
    expect(fanIn.type).toBe('UFI');
  });

  test('reset() restores FI type', () => {
    const {fanIn} = createFanIn();
    fanIn.convertToUFI();
    expect(fanIn.type).toBe('UFI');

    fanIn.reset();
    expect(fanIn.type).toBe('FI');
  });

  test('propagateConstraints() with FI type sends same branch pattern to all inputs', () => {
    const {inputs, fanIn} = createFanIn();

    fanIn.propagateConstraints([], CONSTRAINTS.userId, unpinned);

    const baseCost = {
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    };
    expect(inputs[0].estimateCost(1, [])).toStrictEqual(baseCost);
    expect(inputs[1].estimateCost(1, [])).toStrictEqual(baseCost);
  });

  test('propagateConstraints() with UFI type sends unique branch patterns to each input', () => {
    const {inputs, fanIn} = createFanIn(3);
    fanIn.convertToUFI();

    fanIn.propagateConstraints([], CONSTRAINTS.userId, unpinned);

    const baseCost = {
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    };
    expect(inputs[0].estimateCost(1, [])).toStrictEqual(baseCost);
    expect(inputs[1].estimateCost(1, [])).toStrictEqual(baseCost);
    expect(inputs[2].estimateCost(1, [])).toStrictEqual(baseCost);
  });

  test('can set and get output', () => {
    const {fanIn} = createFanIn();
    const output = createConnection('comments');

    fanIn.setOutput(output);

    expect(fanIn.output).toBe(output);
  });

  suite('OR selectivity calculation', () => {
    // Helper to create a connection with a specific selectivity
    function createSelectiveConnection(
      tableName: string,
      selectivityPercent: number,
    ) {
      const costModel: ConnectionCostModel = (
        _table: string,
        _sort: Ordering,
        filters,
        _constraint,
      ): CostModelCost => ({
        startupCost: 0,
        rows: filters ? selectivityPercent : 100,
        fanout: () => ({
          confidence: 'none',
          fanout: 1,
        }),
      });

      return new PlannerSource(tableName, costModel).connect(
        [['id', 'asc']],
        {
          type: 'simple',
          left: {type: 'column', name: 'x'},
          op: '=',
          right: {type: 'literal', value: 1},
        },
        false,
        undefined,
        1, // limit triggers selectivity calculation
      );
    }

    test.each([
      {type: 'FI' as const, convert: false},
      {type: 'UFI' as const, convert: true},
    ])(
      '$type combines selectivities using independent probability',
      ({convert}) => {
        const connectionA = createSelectiveConnection('branchA', 50); // 50% selective
        const connectionB = createSelectiveConnection('branchB', 30); // 30% selective

        expect(connectionA.selectivity).toBe(0.5);
        expect(connectionB.selectivity).toBe(0.3);

        const fanIn = new PlannerFanIn([connectionA, connectionB]);
        if (convert) fanIn.convertToUFI();

        // P(A OR B) = 1 - (1-0.5)(1-0.3) = 1 - (0.5)(0.7) = 0.65
        expect(fanIn.estimateCost(1, []).selectivity).toBeCloseTo(0.65, 10);
      },
    );

    test('three OR branches combine correctly', () => {
      const connectionA = createSelectiveConnection('branchA', 50);
      const connectionB = createSelectiveConnection('branchB', 40);
      const connectionC = createSelectiveConnection('branchC', 60);

      const fanIn = new PlannerFanIn([connectionA, connectionB, connectionC]);

      // P(A OR B OR C) = 1 - (1-0.5)(1-0.4)(1-0.6) = 1 - 0.12 = 0.88
      expect(fanIn.estimateCost(1, []).selectivity).toBeCloseTo(0.88, 10);
    });

    test('selectivity never exceeds 1.0 with high individual selectivities', () => {
      const connectionA = createSelectiveConnection('branchA', 99);
      const connectionB = createSelectiveConnection('branchB', 99);

      const fanIn = new PlannerFanIn([connectionA, connectionB]);

      // P(A OR B) = 1 - (1-0.99)(1-0.99) = 0.9999
      const selectivity = fanIn.estimateCost(1, []).selectivity;
      expect(selectivity).toBeCloseTo(0.9999, 10);
      expect(selectivity).toBeLessThanOrEqual(1.0);
    });
  });
});
