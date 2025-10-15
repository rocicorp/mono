import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {PlannerFanOut} from './planner-fan-out.ts';
import {simpleCostModel} from './test/helpers.ts';

suite('PlannerFanOut', () => {
  test('initial state is FO type', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const input = source.connect([['id', 'asc']], undefined);

    const fanOut = new PlannerFanOut(input);

    expect(fanOut.kind).toBe('fan-out');
    expect(fanOut.type).toBe('FO');
  });

  test('can add outputs', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const input = source.connect([['id', 'asc']], undefined);

    const source1 = new PlannerSource('posts', simpleCostModel);
    const output1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('comments', simpleCostModel);
    const output2 = source2.connect([['id', 'asc']], undefined);

    const fanOut = new PlannerFanOut(input);
    fanOut.addOutput(output1);
    fanOut.addOutput(output2);

    expect(fanOut.outputs).toHaveLength(2);
    expect(fanOut.outputs[0]).toBe(output1);
    expect(fanOut.outputs[1]).toBe(output2);
  });

  test('can be converted to UFO', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const input = source.connect([['id', 'asc']], undefined);

    const fanOut = new PlannerFanOut(input);
    expect(fanOut.type).toBe('FO');

    fanOut.convertToUFO();
    expect(fanOut.type).toBe('UFO');
  });

  test('propagateConstraints() forwards to input', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const input = source.connect([['id', 'asc']], undefined);

    const fanOut = new PlannerFanOut(input);

    fanOut.propagateConstraints([0], {userId: undefined}, 'unpinned');

    // Input should receive the constraint
    const cost = input.estimateCost();
    // simpleCostModel: 100 - 10 (1 constraint) = 90
    expect(cost).toBe(90);
  });

  test('reset() restores FO type', () => {
    const source = new PlannerSource('users', simpleCostModel);
    const input = source.connect([['id', 'asc']], undefined);

    const fanOut = new PlannerFanOut(input);
    fanOut.convertToUFO();
    expect(fanOut.type).toBe('UFO');

    fanOut.reset();
    expect(fanOut.type).toBe('FO');
  });
});
