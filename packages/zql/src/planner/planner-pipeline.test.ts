import {expect, suite, test} from 'vitest';
import {PlannerSource} from './planner-source.ts';
import {PlannerJoin} from './planner-join.ts';
import {PlannerFanOut} from './planner-fan-out.ts';
import {PlannerFanIn} from './planner-fan-in.ts';
import {PlannerTerminus} from './planner-terminus.ts';
import {CONSTRAINTS, simpleCostModel} from './test/helpers.ts';

suite('Planner Pipeline Integration', () => {
  test('FO/FI pairing produces small cost (single fetch)', () => {
    // Create: source -> connection -> fan-out
    const parentSource = new PlannerSource('users', simpleCostModel);
    const parentConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const fanOut = new PlannerFanOut(parentConnection);

    // Create 3 child sources and joins
    const childSource1 = new PlannerSource('posts', simpleCostModel);
    const childConnection1 = childSource1.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join1 = new PlannerJoin(
      parentConnection,
      childConnection1,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      1,
    );

    const childSource2 = new PlannerSource('comments', simpleCostModel);
    const childConnection2 = childSource2.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join2 = new PlannerJoin(
      parentConnection,
      childConnection2,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      2,
    );

    const childSource3 = new PlannerSource('likes', simpleCostModel);
    const childConnection3 = childSource3.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join3 = new PlannerJoin(
      parentConnection,
      childConnection3,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      3,
    );

    // Wire fan-out -> joins
    fanOut.addOutput(join1);
    fanOut.addOutput(join2);
    fanOut.addOutput(join3);
    parentConnection.setOutput(fanOut);

    // Create fan-in -> terminus
    const fanIn = new PlannerFanIn([join1, join2, join3]);
    join1.setOutput(fanIn);
    join2.setOutput(fanIn);
    join3.setOutput(fanIn);

    const terminus = new PlannerTerminus(fanIn);
    fanIn.setOutput(terminus);

    // Propagate constraints
    terminus.propagateConstraints();

    // FO/FI: All 3 joins get the same branch pattern [0]
    // Parent connection sees only 1 unique branch pattern
    // Cost should be: BASE_COST for base case
    const parentCost = parentConnection.estimateCost(1, []);
    expect(parentCost).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });

    // Verify fan-out and fan-in are still normal types
    expect(fanOut.type).toBe('FO');
    expect(fanIn.type).toBe('FI');
  });

  test('UFO/UFI pairing produces 3x cost (multiple fetches)', () => {
    // Create: source -> connection -> fan-out
    const parentSource = new PlannerSource('users', simpleCostModel);
    const parentConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const fanOut = new PlannerFanOut(parentConnection);

    // Create 3 child sources and joins
    const childSource1 = new PlannerSource('posts', simpleCostModel);
    const childConnection1 = childSource1.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join1 = new PlannerJoin(
      parentConnection,
      childConnection1,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      1,
    );

    const childSource2 = new PlannerSource('comments', simpleCostModel);
    const childConnection2 = childSource2.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join2 = new PlannerJoin(
      parentConnection,
      childConnection2,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      2,
    );

    const childSource3 = new PlannerSource('likes', simpleCostModel);
    const childConnection3 = childSource3.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const join3 = new PlannerJoin(
      parentConnection,
      childConnection3,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      3,
    );

    // Wire fan-out -> joins
    fanOut.addOutput(join1);
    fanOut.addOutput(join2);
    fanOut.addOutput(join3);
    parentConnection.setOutput(fanOut);

    // Create fan-in -> terminus
    const fanIn = new PlannerFanIn([join1, join2, join3]);
    join1.setOutput(fanIn);
    join2.setOutput(fanIn);
    join3.setOutput(fanIn);

    const terminus = new PlannerTerminus(fanIn);
    fanIn.setOutput(terminus);

    // Convert to union types
    fanOut.convertToUFO();
    fanIn.convertToUFI();

    // Propagate constraints
    terminus.propagateConstraints();

    // UFO/UFI: Each join gets a unique branch pattern [0], [1], [2]
    // Parent connection sees 3 unique branch patterns
    // Cost should be: BASE_COST (union costs don't multiply in new model)
    const parentCost = parentConnection.estimateCost(1, []);
    expect(parentCost).toStrictEqual({
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    });

    // Verify fan-out and fan-in are union types
    expect(fanOut.type).toBe('UFO');
    expect(fanIn.type).toBe('UFI');
  });

  test('UFO/UFI demonstrates fetch issue cost blowup', () => {
    // This test explicitly demonstrates the performance difference
    const parentSource = new PlannerSource('users', simpleCostModel);

    // Test 1: Normal FO/FI
    const normalConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const normalFanOut = new PlannerFanOut(normalConnection);

    const joins1 = Array.from({length: 3}, (_, i) => {
      const childSource = new PlannerSource(`child${i}`, simpleCostModel);
      const childConnection = childSource.connect(
        [['id', 'asc']],
        undefined,
        false,
      );
      return new PlannerJoin(
        normalConnection,
        childConnection,
        CONSTRAINTS.userId,
        CONSTRAINTS.id,
        true,
        i,
      );
    });

    joins1.forEach(j => normalFanOut.addOutput(j));
    normalConnection.setOutput(normalFanOut);

    const normalFanIn = new PlannerFanIn(joins1);
    joins1.forEach(j => j.setOutput(normalFanIn));

    const normalTerminus = new PlannerTerminus(normalFanIn);
    normalFanIn.setOutput(normalTerminus);

    normalTerminus.propagateConstraints();
    const normalCost = normalConnection.estimateCost(1, []);

    // Test 2: Union UFO/UFI
    const unionConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const unionFanOut = new PlannerFanOut(unionConnection);
    unionFanOut.convertToUFO();

    const joins2 = Array.from({length: 3}, (_, i) => {
      const childSource = new PlannerSource(`child${i}`, simpleCostModel);
      const childConnection = childSource.connect(
        [['id', 'asc']],
        undefined,
        false,
      );
      return new PlannerJoin(
        unionConnection,
        childConnection,
        CONSTRAINTS.userId,
        CONSTRAINTS.id,
        true,
        i,
      );
    });

    joins2.forEach(j => unionFanOut.addOutput(j));
    unionConnection.setOutput(unionFanOut);

    const unionFanIn = new PlannerFanIn(joins2);
    unionFanIn.convertToUFI();
    joins2.forEach(j => j.setOutput(unionFanIn));

    const unionTerminus = new PlannerTerminus(unionFanIn);
    unionFanIn.setOutput(unionTerminus);

    unionTerminus.propagateConstraints();
    const unionCost = unionConnection.estimateCost(1, []);

    // Union cost should be same as normal cost in new model (costs don't multiply)
    const baseCost = {
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    };
    expect(normalCost).toStrictEqual(baseCost);
    expect(unionCost).toStrictEqual(baseCost);
  });

  test('chained UFO/UFI pairs demonstrate exponential cost explosion (2x2=4x)', () => {
    // Pipeline: source -> conn -> FO -> join1,join2 -> FI -> FO -> join3,join4 -> FI -> terminus
    const parentSource = new PlannerSource('users', simpleCostModel);

    // === Test 1: Normal FO/FI chaining ===
    const normalConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );

    // First FO layer
    const normalFanOut1 = new PlannerFanOut(normalConnection);
    normalConnection.setOutput(normalFanOut1);

    // First layer joins (2 branches)
    const normalChild1 = new PlannerSource('posts', simpleCostModel);
    const normalChildConn1 = normalChild1.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const normalJoin1 = new PlannerJoin(
      normalConnection,
      normalChildConn1,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      1,
    );

    const normalChild2 = new PlannerSource('comments', simpleCostModel);
    const normalChildConn2 = normalChild2.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const normalJoin2 = new PlannerJoin(
      normalConnection,
      normalChildConn2,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      2,
    );

    normalFanOut1.addOutput(normalJoin1);
    normalFanOut1.addOutput(normalJoin2);

    // First FI layer
    const normalFanIn1 = new PlannerFanIn([normalJoin1, normalJoin2]);
    normalJoin1.setOutput(normalFanIn1);
    normalJoin2.setOutput(normalFanIn1);

    // Second FO layer
    const normalFanOut2 = new PlannerFanOut(normalFanIn1);
    normalFanIn1.setOutput(normalFanOut2);

    // Second layer joins (2 more branches)
    const normalChild3 = new PlannerSource('likes', simpleCostModel);
    const normalChildConn3 = normalChild3.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const normalJoin3 = new PlannerJoin(
      normalFanIn1,
      normalChildConn3,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      3,
    );

    const normalChild4 = new PlannerSource('shares', simpleCostModel);
    const normalChildConn4 = normalChild4.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const normalJoin4 = new PlannerJoin(
      normalFanIn1,
      normalChildConn4,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      4,
    );

    normalFanOut2.addOutput(normalJoin3);
    normalFanOut2.addOutput(normalJoin4);

    // Second FI layer
    const normalFanIn2 = new PlannerFanIn([normalJoin3, normalJoin4]);
    normalJoin3.setOutput(normalFanIn2);
    normalJoin4.setOutput(normalFanIn2);

    // Terminus
    const normalTerminus = new PlannerTerminus(normalFanIn2);
    normalFanIn2.setOutput(normalTerminus);

    normalTerminus.propagateConstraints();
    const normalCost = normalConnection.estimateCost(1, []);

    // === Test 2: Union UFO/UFI chaining ===
    const unionConnection = parentSource.connect(
      [['id', 'asc']],
      undefined,
      false,
    );

    // First UFO layer
    const unionFanOut1 = new PlannerFanOut(unionConnection);
    unionFanOut1.convertToUFO();
    unionConnection.setOutput(unionFanOut1);

    // First layer joins (2 branches)
    const unionChild1 = new PlannerSource('posts', simpleCostModel);
    const unionChildConn1 = unionChild1.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const unionJoin1 = new PlannerJoin(
      unionConnection,
      unionChildConn1,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      1,
    );

    const unionChild2 = new PlannerSource('comments', simpleCostModel);
    const unionChildConn2 = unionChild2.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const unionJoin2 = new PlannerJoin(
      unionConnection,
      unionChildConn2,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      2,
    );

    unionFanOut1.addOutput(unionJoin1);
    unionFanOut1.addOutput(unionJoin2);

    // First UFI layer
    const unionFanIn1 = new PlannerFanIn([unionJoin1, unionJoin2]);
    unionFanIn1.convertToUFI();
    unionJoin1.setOutput(unionFanIn1);
    unionJoin2.setOutput(unionFanIn1);

    // Second UFO layer
    const unionFanOut2 = new PlannerFanOut(unionFanIn1);
    unionFanOut2.convertToUFO();
    unionFanIn1.setOutput(unionFanOut2);

    // Second layer joins (2 more branches)
    const unionChild3 = new PlannerSource('likes', simpleCostModel);
    const unionChildConn3 = unionChild3.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const unionJoin3 = new PlannerJoin(
      unionFanIn1,
      unionChildConn3,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      3,
    );

    const unionChild4 = new PlannerSource('shares', simpleCostModel);
    const unionChildConn4 = unionChild4.connect(
      [['id', 'asc']],
      undefined,
      false,
    );
    const unionJoin4 = new PlannerJoin(
      unionFanIn1,
      unionChildConn4,
      CONSTRAINTS.userId,
      CONSTRAINTS.id,
      true,
      4,
    );

    unionFanOut2.addOutput(unionJoin3);
    unionFanOut2.addOutput(unionJoin4);

    // Second UFI layer
    const unionFanIn2 = new PlannerFanIn([unionJoin3, unionJoin4]);
    unionFanIn2.convertToUFI();
    unionJoin3.setOutput(unionFanIn2);
    unionJoin4.setOutput(unionFanIn2);

    // Terminus
    const unionTerminus = new PlannerTerminus(unionFanIn2);
    unionFanIn2.setOutput(unionTerminus);

    unionTerminus.propagateConstraints();
    const unionCost = unionConnection.estimateCost(1, []);

    // Normal FO/FI: All branches collapse to single pattern [0, 0]
    // Cost = BASE_COST = 100
    const baseCost = {
      startupCost: 0,
      scanEst: 100,
      cost: 0,
      returnedRows: 100,
      selectivity: 1.0,
      limit: undefined,
      fanout: expect.any(Function),
    };
    expect(normalCost).toStrictEqual(baseCost);

    // Union UFO/UFI: In new model, costs don't explode
    // Cost = BASE_COST = 100
    expect(unionCost).toStrictEqual(baseCost);
  });
});
