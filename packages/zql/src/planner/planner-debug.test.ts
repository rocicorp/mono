import {expect, suite, test} from 'vitest';
import {buildPlanGraph} from './planner-builder.ts';
import {simpleCostModel} from './test/helpers.ts';
import {builder} from './test/test-schema.ts';
import {AccumulatorDebugger} from './planner-debug.ts';

suite('planner debugging', () => {
  test('captures planning events for simple query', () => {
    const ast = builder.users.ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    // Should have at least one attempt
    const attemptStarts = dbg.getEvents('attempt-start');
    expect(attemptStarts.length).toBeGreaterThan(0);

    // Should have connection costs events
    const connectionCosts = dbg.getEvents('connection-costs');
    expect(connectionCosts.length).toBeGreaterThan(0);

    // Should have a complete plan
    const planComplete = dbg.getEvents('plan-complete');
    expect(planComplete.length).toBeGreaterThan(0);

    // Should have a best plan selected
    const bestPlan = dbg.getEvents('best-plan-selected');
    expect(bestPlan.length).toBe(1);
  });

  test('captures planning events with joins', () => {
    const ast = builder.users.whereExists('posts').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    // Should have multiple attempts (different root connections)
    const attemptStarts = dbg.getEvents('attempt-start');
    expect(attemptStarts.length).toBeGreaterThan(1);

    // Should have connection selected events
    const connectionSelected = dbg.getEvents('connection-selected');
    expect(connectionSelected.length).toBeGreaterThan(0);

    // Should have constraints propagated events
    const constraintsPropagated = dbg.getEvents('constraints-propagated');
    expect(constraintsPropagated.length).toBeGreaterThan(0);

    // Should have complete plans
    const planComplete = dbg.getEvents('plan-complete');
    expect(planComplete.length).toBeGreaterThan(0);

    // Check that join states are captured
    const firstComplete = planComplete[0];
    expect(firstComplete.joinStates.length).toBeGreaterThan(0);
    expect(firstComplete.joinStates[0].join).toContain('⋈');
    expect(['semi', 'flipped']).toContain(firstComplete.joinStates[0].type);

    // Best plan should have join information
    const bestPlan = dbg.getEvents('best-plan-selected');
    expect(bestPlan.length).toBe(1);
    expect(bestPlan[0].joinStates.length).toBeGreaterThan(0);
  });

  test('captures multiple planning attempts', () => {
    const ast = builder.users.whereExists('posts').whereExists('comments').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    // Should have multiple attempts
    const attemptStarts = dbg.getEvents('attempt-start');
    expect(attemptStarts.length).toBeGreaterThan(1);

    // Each attempt should have a unique attempt number
    const attemptNumbers = attemptStarts.map(e => e.attemptNumber);
    expect(new Set(attemptNumbers).size).toBe(attemptNumbers.length);

    // Should have multiple joins
    const planComplete = dbg.getEvents('plan-complete');
    expect(planComplete.length).toBeGreaterThan(0);
    const firstComplete = planComplete[0];
    expect(firstComplete.joinStates.length).toBe(2);
  });

  test('format() produces readable output', () => {
    const ast = builder.users.whereExists('posts').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    const formatted = dbg.format();

    // Should contain key information
    expect(formatted).toContain('Attempt');
    expect(formatted).toContain('Connection costs');
    expect(formatted).toContain('Selected');
    expect(formatted).toContain('Plan complete');
    expect(formatted).toContain('Best plan selected');
    expect(formatted).toContain('⋈'); // Join symbol
  });

  test('captures constraint information', () => {
    const ast = builder.users.whereExists('posts').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    // Check that constraints are captured in connection costs
    const connectionCosts = dbg.getEvents('connection-costs');
    expect(connectionCosts.length).toBeGreaterThan(0);

    // After constraint propagation, some connections should have constraints
    const constraintsPropagated = dbg.getEvents('constraints-propagated');
    expect(constraintsPropagated.length).toBeGreaterThan(0);

    // At least one event should show constraints being set
    const hasConstraints = constraintsPropagated.some(event =>
      event.connectionConstraints.some(c => c.constraints.size > 0),
    );
    expect(hasConstraints).toBe(true);
  });

  test('captures cost estimates', () => {
    const ast = builder.users.whereExists('posts').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    const connectionCosts = dbg.getEvents('connection-costs');
    expect(connectionCosts.length).toBeGreaterThan(0);

    // Check that cost estimates include all required fields
    const firstCostEvent = connectionCosts[0];
    for (const costInfo of firstCostEvent.costs) {
      expect(costInfo.costEstimate).toBeDefined();
      expect(typeof costInfo.costEstimate.baseCardinality).toBe('number');
      expect(typeof costInfo.costEstimate.runningCost).toBe('number');
    }
  });

  test('captures node costs after plan completion', () => {
    const ast = builder.users.whereExists('posts').ast;
    const plans = buildPlanGraph(ast, simpleCostModel);
    const dbg = new AccumulatorDebugger();

    plans.plan.plan(dbg);

    const planComplete = dbg.getEvents('plan-complete');
    expect(planComplete.length).toBeGreaterThan(0);

    const firstComplete = planComplete[0];

    // Should have costs for all nodes
    expect(firstComplete.nodeCosts.length).toBeGreaterThan(0);

    // Should have both connections and joins
    const nodeTypes = new Set(firstComplete.nodeCosts.map(n => n.nodeType));
    expect(nodeTypes.has('connection')).toBe(true);
    expect(nodeTypes.has('join')).toBe(true);

    // Each node cost should have required fields
    for (const nodeCost of firstComplete.nodeCosts) {
      expect(nodeCost.node).toBeDefined();
      expect(nodeCost.nodeType).toBeDefined();
      expect(nodeCost.costEstimate).toBeDefined();
    }
  });
});
