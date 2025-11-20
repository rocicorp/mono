import {describe, expect, test} from 'vitest';
import type {PlanDebugEventJSON} from '../../../zero-protocol/src/analyze-query-result.ts';
import type {PlanState} from './planner-graph.ts';
import {
  AccumulatorDebugger,
  formatPlannerEvents,
  serializePlanDebugEvents,
} from './planner-debug.ts';

describe('formatPlannerEvents', () => {
  test('formats simple plan with single attempt', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 1,
      },
      {
        type: 'node-cost',
        attemptNumber: 0,
        nodeType: 'connection',
        node: 'users',
        branchPattern: [],
        downstreamChildSelectivity: 1.0,
        costEstimate: {
          startupCost: 0,
          scanEst: 10,
          cost: 10,
          returnedRows: 100,
          selectivity: 1.0,
          limit: undefined,
        },
        filters: undefined,
      },
      {
        type: 'plan-complete',
        attemptNumber: 0,
        totalCost: 10,
        flipPattern: 0,
        joinStates: [],
      },
      {
        type: 'best-plan-selected',
        bestAttemptNumber: 0,
        totalCost: 10,
        flipPattern: 0,
        joinStates: [],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain('[Attempt 1/1]');
    expect(formatted).toContain('users:');
    expect(formatted).toContain('cost=10.00');
    expect(formatted).toContain('✓ Best plan: Attempt 1');
  });

  test('formats plan with multiple attempts', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 2,
      },
      {
        type: 'plan-complete',
        attemptNumber: 0,
        totalCost: 20,
        flipPattern: 0,
        joinStates: [],
      },
      {
        type: 'attempt-start',
        attemptNumber: 1,
        totalAttempts: 2,
      },
      {
        type: 'plan-complete',
        attemptNumber: 1,
        totalCost: 15,
        flipPattern: 1,
        joinStates: [],
      },
      {
        type: 'best-plan-selected',
        bestAttemptNumber: 1,
        totalCost: 15,
        flipPattern: 1,
        joinStates: [],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain('[Attempt 1/2]');
    expect(formatted).toContain('[Attempt 2/2]');
    expect(formatted).toContain('✓ Best plan: Attempt 2');
    expect(formatted).toContain('cost=15.00');
  });

  test('formats plan with join states', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 1,
      },
      {
        type: 'node-cost',
        attemptNumber: 0,
        nodeType: 'join',
        node: 'users-issues',
        branchPattern: [],
        downstreamChildSelectivity: 1.0,
        costEstimate: {
          startupCost: 0,
          scanEst: 5,
          cost: 15,
          returnedRows: 50,
          selectivity: 0.5,
          limit: undefined,
        },
        joinType: 'semi',
      },
      {
        type: 'plan-complete',
        attemptNumber: 0,
        totalCost: 15,
        flipPattern: 0,
        joinStates: [
          {
            join: 'users-issues',
            type: 'semi',
          },
        ],
      },
      {
        type: 'best-plan-selected',
        bestAttemptNumber: 0,
        totalCost: 15,
        flipPattern: 0,
        joinStates: [
          {
            join: 'users-issues',
            type: 'semi',
          },
        ],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain('Joins:');
    expect(formatted).toContain('users-issues (semi)');
    expect(formatted).toContain('Join types:');
    expect(formatted).toContain('users-issues: semi');
  });

  test('formats plan with failed attempt', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 2,
      },
      {
        type: 'plan-failed',
        attemptNumber: 0,
        reason: 'Unflippable join detected',
      },
      {
        type: 'attempt-start',
        attemptNumber: 1,
        totalAttempts: 2,
      },
      {
        type: 'plan-complete',
        attemptNumber: 1,
        totalCost: 10,
        flipPattern: 1,
        joinStates: [],
      },
      {
        type: 'best-plan-selected',
        bestAttemptNumber: 1,
        totalCost: 10,
        flipPattern: 1,
        joinStates: [],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain('[Attempt 1/2]');
    expect(formatted).toContain('✗ Plan failed: Unflippable join detected');
    expect(formatted).toContain('[Attempt 2/2]');
    expect(formatted).toContain('✓ Plan complete');
  });

  test('formats constraints', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 1,
      },
      {
        type: 'node-constraint',
        attemptNumber: 0,
        nodeType: 'connection',
        node: 'users',
        branchPattern: [],
        constraint: {
          id: undefined,
          userId: undefined,
        },
        from: 'parent',
      },
      {
        type: 'node-cost',
        attemptNumber: 0,
        nodeType: 'connection',
        node: 'users',
        branchPattern: [],
        downstreamChildSelectivity: 1.0,
        costEstimate: {
          startupCost: 0,
          scanEst: 5,
          cost: 5,
          returnedRows: 50,
          selectivity: 0.5,
          limit: undefined,
        },
      },
      {
        type: 'plan-complete',
        attemptNumber: 0,
        totalCost: 5,
        flipPattern: 0,
        joinStates: [],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain('constraints={id, userId}');
  });

  test('formats filters', () => {
    const events: PlanDebugEventJSON[] = [
      {
        type: 'attempt-start',
        attemptNumber: 0,
        totalAttempts: 1,
      },
      {
        type: 'node-cost',
        attemptNumber: 0,
        nodeType: 'connection',
        node: 'users',
        branchPattern: [],
        downstreamChildSelectivity: 1.0,
        costEstimate: {
          startupCost: 0,
          scanEst: 5,
          cost: 5,
          returnedRows: 10,
          selectivity: 0.1,
          limit: undefined,
        },
        filters: {
          type: 'simple',
          left: {type: 'column', name: 'status'},
          op: '=',
          right: {type: 'literal', value: 'active'},
        },
      },
      {
        type: 'plan-complete',
        attemptNumber: 0,
        totalCost: 5,
        flipPattern: 0,
        joinStates: [],
      },
    ];

    const formatted = formatPlannerEvents(events);

    expect(formatted).toContain("filters=status = 'active'");
  });

  test('formats empty events array', () => {
    const formatted = formatPlannerEvents([]);
    expect(formatted).toBe('');
  });
});

describe('AccumulatorDebugger', () => {
  test('format() delegates to formatPlannerEvents', () => {
    const planDebugger = new AccumulatorDebugger();

    planDebugger.log({
      type: 'attempt-start',
      attemptNumber: 0,
      totalAttempts: 1,
    });
    planDebugger.log({
      type: 'plan-complete',
      attemptNumber: 0,
      totalCost: 10,
      flipPattern: 0,
      joinStates: [],
      planSnapshot: {} as unknown as PlanState,
    });
    planDebugger.log({
      type: 'best-plan-selected',
      bestAttemptNumber: 0,
      totalCost: 10,
      flipPattern: 0,
      joinStates: [],
    });

    const formatted = planDebugger.format();

    expect(formatted).toContain('[Attempt 1/1]');
    expect(formatted).toContain('✓ Best plan: Attempt 1');
  });

  test('getEvents filters by type', () => {
    const planDebugger = new AccumulatorDebugger();

    planDebugger.log({
      type: 'attempt-start',
      attemptNumber: 0,
      totalAttempts: 2,
    });
    planDebugger.log({
      type: 'attempt-start',
      attemptNumber: 1,
      totalAttempts: 2,
    });
    planDebugger.log({
      type: 'plan-complete',
      attemptNumber: 0,
      totalCost: 10,
      flipPattern: 0,
      joinStates: [],
      planSnapshot: {} as unknown as PlanState,
    });

    const attempts = planDebugger.getEvents('attempt-start');
    expect(attempts).toHaveLength(2);
    expect(attempts[0].attemptNumber).toBe(0);
    expect(attempts[1].attemptNumber).toBe(1);

    const completions = planDebugger.getEvents('plan-complete');
    expect(completions).toHaveLength(1);
    expect(completions[0].totalCost).toBe(10);
  });
});

describe('serializePlanDebugEvents', () => {
  test('serializes events by omitting non-serializable fields', () => {
    const planDebugger = new AccumulatorDebugger();

    // Add a connection-costs event
    planDebugger.log({
      type: 'connection-costs',
      attemptNumber: 0,
      costs: [
        {
          connection: 'users',
          cost: 10,
          costEstimate: {
            startupCost: 0,
            scanEst: 10,
            cost: 10,
            returnedRows: 100,
            selectivity: 1.0,
            limit: undefined,
          },
          pinned: false,
          constraints: {id: {id: undefined}},
          constraintCosts: {
            id: {
              startupCost: 0,
              scanEst: 5,
              cost: 5,
              returnedRows: 10,
              selectivity: 0.1,
              limit: undefined,
            },
          },
        },
      ],
    });

    const serialized = serializePlanDebugEvents(planDebugger.events);

    expect(serialized).toHaveLength(1);
    const event = serialized[0];
    if (event.type === 'connection-costs') {
      expect(event.costs[0].constraints).toEqual({id: {id: null}});
      expect(event.costs[0].constraintCosts).toHaveProperty('id');
      expect(event.costs[0].costEstimate).not.toHaveProperty('fanout');
    }
  });
});
