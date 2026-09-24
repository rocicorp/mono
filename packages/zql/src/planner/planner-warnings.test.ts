import {describe, expect, test} from 'vitest';
import type {AST, Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import {planQuery, type PlanWarningSink} from './planner-builder.ts';
import type {AccessPlan, ConnectionCostModel} from './planner-connection.ts';
import type {PlanWarning, PlanWarningThresholds} from './planner-warnings.ts';

type TableSpec = {
  readonly rows: number | undefined;
  /** The columns of each index, in order. */
  readonly indexes: readonly (readonly string[])[];
};

/**
 * A cost model that picks an access the way SQLite would for a single-table
 * query: it searches an index whose leading columns are all bound (by a
 * constraint or an equality filter), and it sorts unless the index continues
 * with the ordering.
 */
function costModel(tables: Record<string, TableSpec>): ConnectionCostModel {
  return (table, sort, filters, constraint) => {
    const {rows: tableRows, indexes} = tables[table];
    const rows = tableRows ?? 1_000_000;
    const bound = new Set([
      ...Object.keys(constraint ?? {}),
      ...equalities(filters),
    ]);
    const sortColumns = sort.map(([column]) => column);

    let plan: AccessPlan = {
      access: 'scan',
      sort: indexes.some(index => startsWith(index, sortColumns))
        ? 'none'
        : 'full',
      tableRows,
    };
    for (const index of indexes) {
      const prefix = index.findIndex(column => !bound.has(column));
      const boundPrefix = prefix === -1 ? index.length : prefix;
      if (boundPrefix === 0 || boundPrefix < bound.size) {
        continue;
      }
      const rest = index.slice(boundPrefix);
      plan = {
        access: 'search',
        sort: startsWith(rest, sortColumns)
          ? 'none'
          : rest[0] === sortColumns[0]
            ? 'partial'
            : 'full',
        tableRows,
      };
      break;
    }
    return {
      startupCost: 0,
      rows: plan.access === 'search' ? 10 : rows,
      fanout: () => ({fanout: 10, confidence: 'none'}),
      plan,
    };
  };
}

function equalities(filters: Condition | undefined): string[] {
  if (filters?.type === 'and') {
    return filters.conditions.flatMap(equalities);
  }
  return filters?.type === 'simple' &&
    filters.op === '=' &&
    filters.left.type === 'column'
    ? [filters.left.name]
    : [];
}

function startsWith(
  index: readonly string[],
  columns: readonly string[],
): boolean {
  return columns.every((column, i) => index[i] === column);
}

const TABLES: Record<string, TableSpec> = {
  issue: {rows: 100_000, indexes: [['id'], ['projectID', 'modified', 'id']]},
  // No index on issueID.
  comment: {rows: 50_000, indexes: [['id']]},
  label: {rows: 100, indexes: [['id']]},
  issueLabel: {rows: 200_000, indexes: [['labelID', 'issueID']]},
  unanalyzed: {rows: undefined, indexes: [['id']]},
};

const THRESHOLDS: PlanWarningThresholds = {rows: 10_000, cost: 1_000_000};

function warningsFor(
  ast: AST,
  tables: Record<string, TableSpec> = TABLES,
  thresholds: PlanWarningThresholds = THRESHOLDS,
): PlanWarning[] {
  const warnings: PlanWarning[] = [];
  const sink: PlanWarningSink = {
    thresholds,
    report: w => warnings.push(...w),
  };
  planQuery(ast, costModel(tables), undefined, undefined, undefined, sink);
  return warnings;
}

const BY_ID: Ordering = [['id', 'asc']];

function eq(column: string, value: string): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op: '=',
    right: {type: 'literal', value},
  };
}

function issueWithComments(commentOrder: Ordering = BY_ID): AST {
  return {
    table: 'issue',
    where: eq('projectID', 'p1'),
    orderBy: [
      ['modified', 'desc'],
      ['id', 'desc'],
    ],
    limit: 50,
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {table: 'comment', alias: 'comments', orderBy: commentOrder},
      },
    ],
  };
}

describe('missing-index', () => {
  test('a relationship whose foreign key is not indexed', () => {
    expect(warningsFor(issueWithComments())).toEqual([
      {
        type: 'missing-index',
        table: 'comment',
        path: ['comments'],
        perRow: true,
        columns: ['issueID'],
        rows: 50_000,
        suggestedIndex: ['issueID', 'id'],
      },
    ]);
  });

  test('not when the foreign key is indexed', () => {
    expect(
      warningsFor(issueWithComments(), {
        ...TABLES,
        comment: {rows: 50_000, indexes: [['id'], ['issueID', 'id']]},
      }),
    ).toEqual([]);
  });

  test('an EXISTS whose correlation is not indexed', () => {
    const ast: AST = {
      table: 'label',
      orderBy: BY_ID,
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        // The planner may not flip it, so the child is looked up per label.
        flip: false,
        related: {
          correlation: {parentField: ['id'], childField: ['labelID']},
          subquery: {
            table: 'comment',
            alias: 'zsubq_comments',
            orderBy: BY_ID,
          },
        },
      },
    };
    expect(warningsFor(ast)).toEqual([
      {
        type: 'missing-index',
        table: 'comment',
        path: [],
        perRow: true,
        columns: ['labelID'],
        rows: 50_000,
        suggestedIndex: ['labelID', 'id'],
      },
    ]);
  });

  test('a filter on an unindexed column scans the table without a limit', () => {
    const ast: AST = {
      table: 'comment',
      where: eq('issueID', 'i1'),
      orderBy: BY_ID,
    };
    expect(warningsFor(ast)).toEqual([
      {
        type: 'missing-index',
        table: 'comment',
        path: [],
        perRow: false,
        columns: ['issueID'],
        rows: 50_000,
        suggestedIndex: ['issueID', 'id'],
      },
    ]);
    // With a limit, how much of the table is scanned depends on the data.
    expect(warningsFor({...ast, limit: 10})).toEqual([]);
  });
});

describe('full-sort', () => {
  test('an ordering no index covers', () => {
    const ast: AST = {
      table: 'issue',
      orderBy: [
        ['title', 'asc'],
        ['id', 'asc'],
      ],
      limit: 10,
    };
    expect(warningsFor(ast)).toEqual([
      {
        type: 'full-sort',
        table: 'issue',
        path: [],
        perRow: false,
        orderBy: ast.orderBy,
        rows: 100_000,
        suggestedIndex: ['title', 'id'],
      },
    ]);
  });

  test('suggests an index that also covers the lookup', () => {
    const ordering: Ordering = [
      ['created', 'asc'],
      ['id', 'asc'],
    ];
    expect(warningsFor(issueWithComments(ordering))).toEqual([
      {
        type: 'missing-index',
        table: 'comment',
        path: ['comments'],
        perRow: true,
        columns: ['issueID'],
        rows: 50_000,
        suggestedIndex: ['issueID', 'created', 'id'],
      },
      {
        type: 'full-sort',
        table: 'comment',
        path: ['comments'],
        perRow: true,
        orderBy: ordering,
        rows: 50_000,
        suggestedIndex: ['issueID', 'created', 'id'],
      },
    ]);
  });

  test('not a partial sort', () => {
    const ast: AST = {
      table: 'issue',
      where: eq('projectID', 'p1'),
      orderBy: [
        ['modified', 'desc'],
        ['title', 'asc'],
      ],
    };
    expect(warningsFor(ast)).toEqual([]);
  });
});

describe('high-cost', () => {
  test('counts related subqueries once per parent row', () => {
    const ast = issueWithComments();
    // The model estimates 10 issues for the project, each scanning 50k
    // comments.
    const [warning] = warningsFor(ast, TABLES, {rows: 0, cost: 1});
    expect(warning).toMatchObject({type: 'high-cost', table: 'issue'});
    expect((warning as {cost: number}).cost).toBeGreaterThanOrEqual(
      10 * 50_000,
    );
    expect(warningsFor(ast, TABLES, {rows: 0, cost: 500_000})).toHaveLength(1);
    expect(warningsFor(ast, TABLES, {rows: 0, cost: 600_000})).toEqual([]);
  });

  test('not below the threshold', () => {
    expect(
      warningsFor({table: 'label', orderBy: BY_ID}, TABLES, {
        rows: 0,
        cost: 1_000,
      }),
    ).toEqual([]);
  });
});

test('thresholds of 0 disable warnings', () => {
  expect(warningsFor(issueWithComments(), TABLES, {rows: 0, cost: 0})).toEqual(
    [],
  );
});

test('no warnings for small tables or tables without statistics', () => {
  for (const table of ['label', 'unanalyzed']) {
    expect(
      warningsFor({
        table,
        where: eq('name', 'bug'),
        orderBy: [
          ['name', 'asc'],
          ['id', 'asc'],
        ],
      }),
    ).toEqual([]);
  }
});

test('collecting warnings does not change the plan', () => {
  const ast: AST = {
    table: 'issue',
    orderBy: BY_ID,
    limit: 10,
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'issueLabel',
          alias: 'zsubq_labels',
          orderBy: [
            ['labelID', 'asc'],
            ['issueID', 'asc'],
          ],
          where: eq('labelID', 'bug'),
        },
      },
    },
  };
  const model = costModel(TABLES);
  const planned = planQuery(structuredClone(ast), model);
  const reported: PlanWarning[][] = [];
  const plannedWithWarnings = planQuery(
    structuredClone(ast),
    model,
    undefined,
    undefined,
    undefined,
    {thresholds: {rows: 1, cost: 1}, report: w => reported.push([...w])},
  );
  expect(plannedWithWarnings).toEqual(planned);
  expect(reported).toHaveLength(1);
});
