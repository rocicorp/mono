import {expect, suite, test} from 'vitest';
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import {applyPlansToAST, buildPlanGraph} from './planner-builder.ts';
import type {ConnectionCostModel} from './planner-connection.ts';
import {AccumulatorDebugger} from './planner-debug.ts';

/**
 * Plan-level tests for NOT EXISTS (anti-join) optimization.
 *
 * NOT EXISTS joins cannot be flipped, but the planner still needs to:
 * 1. Evaluate and cost plans for queries whose only joins are NOT EXISTS
 *    (constraint propagation, cost estimates, debug/analyze events).
 * 2. Use realistic anti-join selectivity so flip decisions for sibling
 *    EXISTS joins are based on how many parent rows actually survive the
 *    NOT EXISTS filter.
 */
suite('planning NOT EXISTS', () => {
  function notExists(
    childTable: string,
    parentField = 'id',
    childField = 'userId',
  ): Condition {
    return {
      type: 'correlatedSubquery',
      op: 'NOT EXISTS',
      related: {
        correlation: {
          parentField: [parentField],
          childField: [childField],
        },
        subquery: {
          table: childTable,
          alias: childTable,
        },
      },
    };
  }

  function exists(
    childTable: string,
    parentField = 'id',
    childField = 'userId',
  ): Condition {
    return {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {
          parentField: [parentField],
          childField: [childField],
        },
        subquery: {
          table: childTable,
          alias: childTable,
        },
      },
    };
  }

  test('a NOT EXISTS-only query still gets a planning pass', () => {
    // No joins are flippable, so there is only one possible plan — but the
    // planner must still evaluate it so constraints propagate and cost
    // estimates / analyze output exist for the query.
    const ast: AST = {
      table: 'users',
      where: notExists('bans'),
    };

    const model: ConnectionCostModel = (
      table,
      _sort,
      _filters,
      constraint,
    ) => ({
      startupCost: 0,
      rows: constraint ? 2 : table === 'users' ? 1000 : 5000,
      fanout: () => ({fanout: 1, confidence: 'high'}) as const,
    });

    const plans = buildPlanGraph(ast, model, true);
    const dbg = new AccumulatorDebugger();
    plans.plan.plan(dbg);

    // Exactly one plan (all-semi) was evaluated and selected.
    expect(dbg.getEvents('plan-complete')).toHaveLength(1);
    expect(dbg.getEvents('best-plan-selected')).toHaveLength(1);

    // The correlation constraint was propagated to the NOT EXISTS child, so
    // its probe is costed as a constrained (indexed) lookup.
    const bansConnection = plans.plan.connections.find(c => c.table === 'bans');
    expect(bansConnection?.getConstraintsForDebug()).toEqual({
      '': {userId: undefined},
    });

    // The plan has a finite, non-zero cost: the anti-join selectivity floor
    // prevents the "no rows survive" estimate that would zero everything out.
    const totalCost = plans.plan.getTotalCost();
    expect(Number.isFinite(totalCost)).toBe(true);
    expect(totalCost).toBeGreaterThan(0);

    // NOT EXISTS stays a semi (anti) join.
    expect(plans.plan.joins).toHaveLength(1);
    expect(plans.plan.joins[0].type).toBe('semi');
    expect(plans.plan.joins[0].op).toBe('NOT EXISTS');
  });

  test('anti-join selectivity informs flip decisions for sibling EXISTS joins', () => {
    // users(1000 rows, limit 10) filtered by NOT EXISTS bans AND EXISTS posts.
    //
    // The unfiltered NOT EXISTS estimates that only a small fraction of users
    // survive (the anti-join selectivity floor), so the semi plan must scan
    // many users — each paying a bans probe — to fill the limit. Scanning the
    // small posts table (20 rows) and fetching users per post is cheaper, so
    // the planner should flip the EXISTS join.
    //
    // (Before anti-join costing, NOT EXISTS was estimated with EXISTS
    // semantics: ~every user survives, making the semi plan look ~5x cheaper
    // than it is and keeping the EXISTS join unflipped.)
    const ast: AST = {
      table: 'users',
      limit: 10,
      where: {
        type: 'and',
        conditions: [notExists('bans'), exists('posts')],
      },
    };

    const model: ConnectionCostModel = (table, _sort, _filters, constraint) => {
      const constrained =
        constraint !== undefined && Object.keys(constraint).length > 0;
      const rows =
        table === 'users'
          ? constrained
            ? 1
            : 1000
          : table === 'posts'
            ? constrained
              ? 1
              : 20
            : constrained // bans
              ? 1
              : 100_000;
      return {
        startupCost: 0,
        rows,
        fanout: () => ({fanout: 1, confidence: 'high'}) as const,
      };
    };

    const plans = buildPlanGraph(ast, model, true);
    plans.plan.plan();

    expect(plans.plan.joins).toHaveLength(2);
    const [bansJoin, postsJoin] = plans.plan.joins;

    // The NOT EXISTS join can never flip.
    expect(bansJoin.op).toBe('NOT EXISTS');
    expect(bansJoin.type).toBe('semi');

    // The EXISTS join is flipped: driving from the small posts table beats
    // scanning ~limit/antiSelectivity users.
    expect(postsJoin.op).toBe('EXISTS');
    expect(postsJoin.type).toBe('flipped');

    // The decision lands in the AST: EXISTS gets flip: true, NOT EXISTS
    // stays unflipped.
    const planned = applyPlansToAST(ast, plans);
    const where = planned.where;
    expect(where?.type).toBe('and');
    const conditions = (where as Extract<Condition, {type: 'and'}>).conditions;
    expect(conditions[0]).toMatchObject({op: 'NOT EXISTS', flip: false});
    expect(conditions[1]).toMatchObject({op: 'EXISTS', flip: true});
  });
});
