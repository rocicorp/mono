import {describe, test} from 'vitest';
import {buildPlanGraph} from './planner-builder.ts';
import type {PlannerDebugDelegate} from './planner-debug-delegate.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';

describe('PlannerDebugDelegate', () => {
  test('example: trace planning decisions', () => {
    // Example cost model
    const costModel = (table: string) => {
      const costs: Record<string, number> = {
        users: 10000,
        posts: 100,
        comments: 5000,
      };
      return costs[table] ?? 1000;
    };

    // Example AST (simplified - normally you'd use a query builder)
    const ast: AST = {
      table: 'users',
      orderBy: [],
      where: {
        type: 'correlatedSubquery' as const,
        op: 'EXISTS' as const,
        related: {
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          subquery: {
            table: 'posts',
            orderBy: [],
          },
        },
      },
    };

    // Create a debug delegate to trace planning
    const trace: string[] = [];
    const delegate: PlannerDebugDelegate = {
      onAttemptStart(attempt, connection, cost) {
        trace.push(
          `Attempt ${attempt}: Starting with ${connection.table} (cost: ${cost})`,
        );
      },

      onConnectionCosts(step, costs) {
        const costSummary = costs
          .map(c => {
            const branchInfo =
              c.branchCosts.size > 1
                ? ` [${Array.from(c.branchCosts.entries())
                    .map(([path, cost]) => `${path}:${cost}`)
                    .join(', ')}]`
                : '';
            return `${c.connection.table}=${c.totalCost}${branchInfo}`;
          })
          .join(', ');
        trace.push(`  Step ${step}: Available: ${costSummary}`);
      },

      onConnectionPinned(connection, cost, flippedJoins) {
        const flipped = flippedJoins.length > 0 ? ' (flipped)' : '';
        trace.push(`  Pinned: ${connection.table} (cost: ${cost})${flipped}`);
      },

      onAttemptComplete(attempt, totalCost, summary) {
        trace.push(
          `Attempt ${attempt} complete: cost=${totalCost}, flipped=${summary.flippedJoins}/${summary.totalJoins} joins`,
        );
      },

      onBestPlanFound(attempt, cost) {
        trace.push(
          `*** New best plan found at attempt ${attempt}: cost=${cost}`,
        );
      },

      onAttemptFailed(attempt, reason) {
        trace.push(`Attempt ${attempt} failed: ${reason}`);
      },
    };

    // Build and plan the query with debug delegate
    const {plan} = buildPlanGraph(ast, costModel);
    plan.plan(false, delegate);

    // The trace array now contains a complete log of planning decisions
    // eslint-disable-next-line no-console
    console.log('Planning trace:');
    // eslint-disable-next-line no-console
    trace.forEach(line => console.log(line));

    // Example output (will vary based on the query):
    // Attempt 0: Starting with users (cost: 10000)
    //   Pinned: users (cost: 10000)
    //   Step 1: Available: posts=100
    //   Pinned: posts (cost: 100)
    // Attempt 0 complete: cost=1000000, flipped=0/1 joins
    // *** New best plan found at attempt 0: cost=1000000
    // Attempt 1: Starting with posts (cost: 100)
    //   Pinned: posts (cost: 100) (flipped)
    //   Step 1: Available: users=1
    //   Pinned: users (cost: 1)
    // Attempt 1 complete: cost=100, flipped=1/1 joins
    // *** New best plan found at attempt 1: cost=100
  });
});
