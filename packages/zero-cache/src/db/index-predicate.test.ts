import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {createLiteIndexStatement} from './create.ts';
import {
  extractLiteIndexPredicateSQL,
  liteIndexPredicateSQL,
  mapIndexPredicateColumns,
  parseLiteIndexPredicateSQL,
} from './index-predicate.ts';
import type {IndexPredicate} from './specs.ts';

describe('SQLite index predicates', () => {
  const predicate = {
    type: 'and',
    conditions: [
      {
        type: 'comparison',
        column: 'active',
        op: '=',
        value: {type: 'boolean', value: true},
      },
      {
        type: 'or',
        conditions: [
          {type: 'null-test', column: 'deleted_at', op: 'IS NULL'},
          {
            type: 'comparison',
            column: 'attempts',
            op: '>=',
            value: {type: 'integer', value: '-5'},
          },
          {
            type: 'comparison',
            column: `kind"quoted`,
            op: '<>',
            value: {type: 'string', value: `it's \\ unicode ✓`},
          },
        ],
      },
    ],
  } as const satisfies IndexPredicate;

  test('generates and parses canonical SQLite SQL', () => {
    const sql = liteIndexPredicateSQL(predicate);
    expect(sql).toBe(
      `("active" = 1 AND ("deleted_at" IS NULL OR "attempts" >= -5 OR "kind""quoted" <> 'it''s \\ unicode ✓'))`,
    );
    expect(
      parseLiteIndexPredicateSQL(sql, column => column === 'active'),
    ).toEqual(predicate);
  });

  test('extracts only a top-level WHERE clause', () => {
    const create =
      `CREATE INDEX "WHE""RE" ON "t" ("where" ASC) ` +
      `WHERE ${liteIndexPredicateSQL(predicate)};`;
    expect(extractLiteIndexPredicateSQL(create)).toBe(
      liteIndexPredicateSQL(predicate),
    );
  });

  test('maps predicate columns recursively', () => {
    expect(
      mapIndexPredicateColumns(predicate, column => `new_${column}`),
    ).toEqual({
      ...predicate,
      conditions: [
        {...predicate.conditions[0], column: 'new_active'},
        {
          ...predicate.conditions[1],
          conditions: [
            {
              ...predicate.conditions[1].conditions[0],
              column: 'new_deleted_at',
            },
            {
              ...predicate.conditions[1].conditions[1],
              column: 'new_attempts',
            },
            {
              ...predicate.conditions[1].conditions[2],
              column: 'new_kind"quoted',
            },
          ],
        },
      ],
    });
  });

  test('unique indexes constrain only predicate members', () => {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(`CREATE TABLE item(id INTEGER, code TEXT, active BOOL);`);
    db.exec(
      createLiteIndexStatement({
        name: 'active_code',
        tableName: 'item',
        unique: true,
        columns: {code: 'ASC'},
        predicate: {
          type: 'comparison',
          column: 'active',
          op: '=',
          value: {type: 'boolean', value: true},
        },
      }),
    );

    db.exec(`
      INSERT INTO item VALUES (1, 'same', 0), (2, 'same', 0);
      INSERT INTO item VALUES (3, 'same', 1);
    `);
    expect(() => db.exec(`INSERT INTO item VALUES (4, 'same', 1);`)).toThrow(
      'UNIQUE constraint failed',
    );
  });
});
