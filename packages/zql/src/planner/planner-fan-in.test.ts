import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {PlannerFanIn} from './planner-fan-in.ts';
import {simpleCostModel} from './test/helpers.ts';

suite('PlannerFanIn', () => {
  test('initial state is FI type', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2]);

    expect(fanIn.kind).toBe('fan-in');
    expect(fanIn.type).toBe('FI');
  });

  test('can be converted to UFI', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2]);
    expect(fanIn.type).toBe('FI');

    fanIn.convertToUFI();
    expect(fanIn.type).toBe('UFI');
  });

  test('reset() restores FI type', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2]);
    fanIn.convertToUFI();
    expect(fanIn.type).toBe('UFI');

    fanIn.reset();
    expect(fanIn.type).toBe('FI');
  });

  test('propagateConstraints() with FI type sends same branch pattern to all inputs', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2]);

    // FI type: all inputs get same branch pattern [0] (prepended to [])
    fanIn.propagateConstraints([], {userId: undefined}, 'unpinned');

    // Both inputs should receive the same constraint
    const cost1 = input1.estimateCost();
    const cost2 = input2.estimateCost();

    // simpleCostModel: 100 - 10 (1 constraint) = 90
    expect(cost1).toBe(90);
    expect(cost2).toBe(90);
  });

  test('propagateConstraints() with UFI type sends unique branch patterns to each input', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const source3 = new PlannerSource('comments', simpleCostModel);
    const input3 = source3.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2, input3]);
    fanIn.convertToUFI();

    // UFI type: each input gets unique branch pattern [0], [1], [2]
    fanIn.propagateConstraints([], {userId: undefined}, 'unpinned');

    // Each input should receive the constraint with unique branch patterns
    // UFI creates separate cost entries per branch
    const cost1 = input1.estimateCost();
    const cost2 = input2.estimateCost();
    const cost3 = input3.estimateCost();

    // simpleCostModel: 100 - 10 (1 constraint) = 90 per branch
    expect(cost1).toBe(90);
    expect(cost2).toBe(90);
    expect(cost3).toBe(90);
  });

  test('can set and get output', () => {
    const source1 = new PlannerSource('users', simpleCostModel);
    const input1 = source1.connect([['id', 'asc']], undefined);

    const source2 = new PlannerSource('posts', simpleCostModel);
    const input2 = source2.connect([['id', 'asc']], undefined);

    const outputSource = new PlannerSource('comments', simpleCostModel);
    const output = outputSource.connect([['id', 'asc']], undefined);

    const fanIn = new PlannerFanIn([input1, input2]);
    fanIn.setOutput(output);

    expect(fanIn.output).toBe(output);
  });
});
