import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  LiteralValue,
  Ordering,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {pushDownCorrelatedPredicates} from '../builder/correlated-predicate-pushdown.ts';
import {transformFilters} from '../builder/filter.ts';
import {planQuery} from './planner-builder.ts';
import type {ConnectionCostModel, CostModelCost} from './planner-connection.ts';
import type {PlannerConstraint} from './planner-constraint.ts';
import {AccumulatorDebugger} from './planner-debug.ts';
import {PlannerSource} from './planner-source.ts';

const tables: Record<
  string,
  {
    rows: number;
    ndv: Record<string, number>;
    columns: Record<string, SchemaValue>;
  }
> = {
  reading: {
    rows: 100_000,
    ndv: {id: 100_000, workID: 10_000},
    columns: {id: {type: 'string'}, workID: {type: 'string'}},
  },
  work: {
    rows: 10_000,
    ndv: {id: 10_000, public: 2},
    columns: {id: {type: 'string'}, public: {type: 'boolean'}},
  },
};

/**
 * Divides a table's rows by the distinct values of each column that the
 * constraint binds or an `=` conjunct pins. So it counts `c = ? AND c = 1`
 * twice, as a naive cost model would.
 */
const costModel: ConnectionCostModel = (
  table: string,
  _sort: Ordering,
  filters: Condition | undefined,
  constraint: PlannerConstraint | undefined,
): CostModelCost => {
  const {rows, ndv} = must(tables[table]);
  let est = rows;
  for (const column of Object.keys(constraint ?? {})) {
    est /= ndv[column];
  }
  for (const c of conjuncts(transformFilters(filters).filters)) {
    if (c.op === '=' && c.left.type === 'column') {
      est /= ndv[c.left.name];
    }
  }
  return {
    startupCost: 0,
    rows: est,
    fanout: columns => ({
      fanout: rows / Math.min(...columns.map(c => ndv[c])),
      confidence: 'high',
    }),
  };
};

function conjuncts(c: Condition | undefined): SimpleCondition[] {
  switch (c?.type) {
    case 'simple':
      return [c];
    case 'and':
      return c.conditions.flatMap(conjuncts);
    default:
      return [];
  }
}

function cmp(column: string, value: LiteralValue): SimpleCondition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op: '=',
    right: {type: 'literal', value},
  };
}

/**
 * `reading.where('workID', 'w1')
 *   .whereExists('work', w => w.where('public', true)).limit(10)`
 */
function readingOfPublicWork(): AST {
  return {
    table: 'reading',
    orderBy: [['id', 'asc']],
    limit: 10,
    where: {
      type: 'and',
      conditions: [
        cmp('workID', 'w1'),
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['workID'], childField: ['id']},
            subquery: {
              table: 'work',
              alias: 'work',
              orderBy: [['id', 'asc']],
              where: cmp('public', true),
            },
          },
        },
      ],
    },
  };
}

function push(ast: AST, pushed?: Set<SimpleCondition>): AST {
  return pushDownCorrelatedPredicates(
    ast,
    table => must(tables[table]).columns,
    pushed,
  );
}

/** The estimated cost of each plan, by flip pattern, and the chosen plan. */
function plan(ast: AST, pushed?: ReadonlySet<SimpleCondition>) {
  const planDebugger = new AccumulatorDebugger();
  const planned = planQuery(ast, costModel, planDebugger, undefined, pushed);
  const costs = new Map(
    planDebugger
      .getEvents('plan-complete')
      .map(e => [e.flipPattern, e.totalCost]),
  );
  const where = planned.where;
  const flipped =
    where?.type === 'and' &&
    where.conditions.some(c => c.type === 'correlatedSubquery' && c.flip);
  return {costs, flipped};
}

describe('PlannerConnection', () => {
  const pin = cmp('id', 'w1');
  const filters: Condition = {
    type: 'and',
    conditions: [cmp('public', true), pin],
  };

  function connect(pushed?: ReadonlySet<SimpleCondition>) {
    return new PlannerSource('work', costModel).connect(
      [['id', 'asc']],
      filters,
      false,
      undefined,
      1,
      pushed,
    );
  }

  test('selectivity leaves out pushed conditions', () => {
    expect(connect(new Set([pin])).selectivity).toBe(1 / 2);
    expect(connect().selectivity).toBe(1 / 2 / 10_000);
  });

  test('a pushed condition counts when the constraint does not bind its column', () => {
    const connection = connect(new Set([pin]));
    expect(connection.estimateCost(1, []).returnedRows).toBe(
      10_000 / 2 / 10_000,
    );
  });

  test('a pushed condition does not count when the constraint binds its column', () => {
    const connection = connect(new Set([pin]));
    connection.propagateConstraints([0], {id: undefined});
    expect(connection.estimateCost(1, [0]).returnedRows).toBe(
      10_000 / 10_000 / 2,
    );

    // A condition that the pass did not push still counts.
    const unmarked = connect();
    unmarked.propagateConstraints([0], {id: undefined});
    expect(unmarked.estimateCost(1, [0]).returnedRows).toBe(
      10_000 / 10_000 / 2 / 10_000,
    );
  });

  test('selectivity with a per-branch filter leaves out pushed conditions', () => {
    const connection = connect(new Set([pin]));
    connection.propagateConstraints([0], {id: undefined});
    connection.setPerBranchFilter([0], cmp('public', false));
    expect(connection.estimateCost(1, [0]).selectivity).toBe(1 / 2 / 2);
  });
});

describe('planQuery', () => {
  test('the pass does not change the estimate of a plan with no flipped joins', () => {
    const ast = readingOfPublicWork();
    const off = plan(ast);

    const pushed = new Set<SimpleCondition>();
    const on = plan(push(ast, pushed), pushed);
    expect(must(on.costs.get(0))).toBe(must(off.costs.get(0)));

    // Without the pushed conditions, the planner counts the copy of
    // `workID = 'w1'` again.
    const unmarked = plan(push(ast));
    expect(must(unmarked.costs.get(0))).not.toBe(off.costs.get(0));
  });

  test('a pushed condition makes a flipped join cheap', () => {
    const ast = readingOfPublicWork();
    const off = plan(ast);
    expect(off.flipped).toBe(false);

    const pushed = new Set<SimpleCondition>();
    const on = plan(push(ast, pushed), pushed);
    expect(must(on.costs.get(1))).toBeLessThan(must(off.costs.get(1)));
    expect(on.flipped).toBe(true);
  });
});
