import {expect, suite, test} from 'vitest';
import {CONSTRAINTS, createConnection, createFanOut} from './test/helpers.ts';
import type {PlannerNode} from './planner-node.ts';

const unpinned = {
  pinned: false,
} as PlannerNode;

suite('PlannerFanOut', () => {
  test('initial state is FO type', () => {
    const {fanOut} = createFanOut();

    expect(fanOut.kind).toBe('fan-out');
    expect(fanOut.type).toBe('FO');
  });

  test('can add outputs', () => {
    const {fanOut} = createFanOut();
    const output1 = createConnection('posts');
    const output2 = createConnection('comments');

    fanOut.addOutput(output1);
    fanOut.addOutput(output2);

    expect(fanOut.outputs).toHaveLength(2);
    expect(fanOut.outputs[0]).toBe(output1);
    expect(fanOut.outputs[1]).toBe(output2);
  });

  test('can be converted to UFO', () => {
    const {fanOut} = createFanOut();
    expect(fanOut.type).toBe('FO');

    fanOut.convertToUFO();
    expect(fanOut.type).toBe('UFO');
  });

  test('propagateConstraints() forwards to input', () => {
    const {input, fanOut} = createFanOut();

    fanOut.propagateConstraints([0], CONSTRAINTS.userId, unpinned);

    expect(input.estimateCost(1, [])).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });
  });

  test('reset() restores FO type', () => {
    const {fanOut} = createFanOut();
    fanOut.convertToUFO();
    expect(fanOut.type).toBe('UFO');

    fanOut.reset();
    expect(fanOut.type).toBe('FO');
  });
});
