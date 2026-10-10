import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Condition} from '../../zero-protocol/src/ast.ts';
import {Database} from './db.ts';
import {createSQLiteCostModel} from './sqlite-cost-model.ts';

function cmp(column: string, op: '=' | '>', value: number): Condition {
  return {
    type: 'simple',
    op,
    left: {type: 'column', name: column},
    right: {type: 'literal', value},
  };
}

// SQLite plans the inner OR below as a MULTI-INDEX OR inside the second
// branch of the outer MULTI-INDEX OR. For that nested OR loop it records a
// scanstatus entry without an OP_Explain, so sqlite3_stmt_scanstatus_v2
// reports SELECTID and PARENTID as -1 and EXPLAIN as NULL.
test('estimates a MULTI-INDEX OR nested in a branch of another one', () => {
  const db = new Database(createSilentLogContext(), ':memory:');
  db.exec(`
    CREATE TABLE t (id TEXT PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
    CREATE INDEX t_a ON t (a);
    CREATE INDEX t_b ON t (b);
    CREATE INDEX t_c ON t (c);
    ANALYZE;
  `);
  const filters: Condition = {
    type: 'or',
    conditions: [
      cmp('a', '=', 1),
      {
        type: 'and',
        conditions: [
          cmp('a', '>', 5),
          {type: 'or', conditions: [cmp('b', '=', 1), cmp('c', '=', 2)]},
        ],
      },
    ],
  };
  expect(
    db
      .prepare(
        'EXPLAIN QUERY PLAN SELECT id FROM t WHERE a = 1 OR (a > 5 AND (b = 1 OR c = 2))',
      )
      .all<{detail: string}>()
      .filter(({detail}) => detail === 'MULTI-INDEX OR'),
  ).toHaveLength(2);

  const costModel = createSQLiteCostModel(
    db,
    new Map([
      [
        't',
        {
          zqlSpec: {
            id: {type: 'string'},
            a: {type: 'number'},
            b: {type: 'number'},
            c: {type: 'number'},
          },
        },
      ],
    ]),
  );

  const cost = costModel('t', [['id', 'asc']], filters, undefined);
  expect(cost.rows).toBeGreaterThan(0);
  expect(cost.plan).toMatchObject({access: 'search', sort: 'full'});
});
