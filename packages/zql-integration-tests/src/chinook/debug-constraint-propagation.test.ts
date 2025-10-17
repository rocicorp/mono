// Detailed test to understand constraint propagation in OR queries
import {describe, test} from 'vitest';
import {planQuery} from '../../../zql/src/planner/planner-builder.ts';
import type {PlannerDebugDelegate} from '../../../zql/src/planner/planner-debug-delegate.ts';
import {builder} from './schema.ts';
import type {PlannerConstraint} from '../../../zql/src/planner/planner-constraint.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {must} from '../../../shared/src/must.ts';

function makeCostModel(costs: Record<string, number>) {
  return (
    table: string,
    _sort: Ordering,
    _filters: Condition | undefined,
    constraint: PlannerConstraint | undefined,
  ) => {
    if (!constraint) {
      return must(costs[table]);
    }

    const fields = constraint.fields;
    if ('id' in fields) {
      return 1;
    }

    const fieldCount = Object.keys(fields).length;
    const baseCost = must(costs[table]) / (fieldCount * 100 || 1);

    // Apply 10x discount for semi-joins (early termination)
    return constraint.isSemiJoin ? baseCost / 10 : baseCost;
  };
}

describe('debug constraint propagation in OR', () => {
  test('track.exists(album).or.exists(invoiceLines) - detailed trace', () => {
    const costModel = makeCostModel({
      track: 10_000,
      album: 10,
      invoiceLine: 1_000_000,
    });

    const ast = builder.track
      .where(({or, exists}) => or(exists('album'), exists('invoiceLines')))
      .ast;

    const trace: string[] = [];
    const delegate: PlannerDebugDelegate = {
      onAttemptStart(attempt, connection, cost) {
        trace.push(
          `\n=== Attempt ${attempt}: Starting with ${connection.table} (cost: ${cost}) ===`,
        );
      },

      onConnectionCosts(step, costs) {
        trace.push(`  Step ${step} - Connection costs:`);
        for (const {connection, totalCost, branchCosts, branchConstraints} of costs) {
          if (branchCosts.size > 1) {
            const branches = Array.from(branchCosts.entries())
              .map(([path, cost]) => {
                const constraint = branchConstraints.get(path);
                const constraintInfo = constraint
                  ? `{${Object.keys(constraint.fields).join(',')}${constraint.isSemiJoin ? ',semi' : ''}}`
                  : '{}';
                return `${path}:${cost}${constraintInfo}`;
              })
              .join(', ');
            trace.push(
              `    ${connection.table}: total=${totalCost} branches=[${branches}]`,
            );
          } else {
            const singlePath = Array.from(branchCosts.keys())[0] || '';
            const constraint = branchConstraints.get(singlePath);
            const constraintInfo = constraint
              ? ` {${Object.keys(constraint.fields).join(',')}${constraint.isSemiJoin ? ',semi' : ''}}`
              : '';
            trace.push(
              `    ${connection.table}: ${totalCost} (path="${singlePath}")${constraintInfo}`,
            );
          }
        }
      },

      onConnectionPinned(connection, cost, flippedJoins) {
        const flipped =
          flippedJoins.length > 0
            ? ` (flipped ${flippedJoins.length} join${flippedJoins.length > 1 ? 's' : ''})`
            : '';
        trace.push(`  Pinned: ${connection.table} (cost: ${cost})${flipped}`);
      },

      onAttemptComplete(attempt, totalCost, summary) {
        trace.push(
          `  Attempt ${attempt} complete: totalCost=${totalCost}, flipped=${summary.flippedJoins}/${summary.totalJoins} joins`,
        );
      },

      onBestPlanFound(attempt, cost, summary) {
        trace.push(
          `  *** NEW BEST PLAN: attempt=${attempt}, cost=${cost}, flipped=${summary.flippedJoins}/${summary.totalJoins}`,
        );
      },

      onAttemptFailed(attempt, reason) {
        trace.push(`  Attempt ${attempt} FAILED: ${reason}`);
      },
    };

    const planned = planQuery(ast, costModel, false, delegate);

    // eslint-disable-next-line no-console
    console.log('\n' + trace.join('\n'));

    // eslint-disable-next-line no-console
    console.log('\n=== Analysis ===');
    // eslint-disable-next-line no-console
    console.log('Expected: album flip=true, invoiceLine flip=false');
    // eslint-disable-next-line no-console
    console.log('This should give plan: album -> track -> invoiceLines');
    const albumCond = planned.where?.type === 'or' ? planned.where.conditions?.[0] : undefined;
    const invoiceLineCond = planned.where?.type === 'or' ? planned.where.conditions?.[1] : undefined;
    // eslint-disable-next-line no-console
    console.log(
      '\nActual: album flip=',
      albumCond?.type === 'correlatedSubquery' ? albumCond.flip : 'N/A',
      ', invoiceLine flip=',
      invoiceLineCond?.type === 'correlatedSubquery' ? invoiceLineCond.flip : 'N/A',
    );
    // eslint-disable-next-line no-console
    console.log(
      '\nKey question: In Attempt 0, after pinning track, does invoiceLine get the trackId constraint?',
    );
  });
});
