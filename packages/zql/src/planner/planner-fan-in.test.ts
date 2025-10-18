import {expect, suite, test} from 'vitest';
import {
  CONSTRAINTS,
  createConnection,
  createFanIn,
  expectedCost,
} from './test/helpers.ts';
import type {PlannerNode} from './planner-node.ts';

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

    expect(inputs[0].estimateCost()).toBe(expectedCost(1));
    expect(inputs[1].estimateCost()).toBe(expectedCost(1));
  });

  test('propagateConstraints() with UFI type sends unique branch patterns to each input', () => {
    const {inputs, fanIn} = createFanIn(3);
    fanIn.convertToUFI();

    fanIn.propagateConstraints([], CONSTRAINTS.userId, unpinned);

    expect(inputs[0].estimateCost()).toBe(expectedCost(1));
    expect(inputs[1].estimateCost()).toBe(expectedCost(1));
    expect(inputs[2].estimateCost()).toBe(expectedCost(1));
  });

  test('can set and get output', () => {
    const {fanIn} = createFanIn();
    const output = createConnection('comments');

    fanIn.setOutput(output);

    expect(fanIn.output).toBe(output);
  });
});
