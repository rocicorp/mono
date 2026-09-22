import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {createLiteIndexStatement} from './create.ts';
import {
  liteIndexPredicateSQL,
  mapIndexPredicateColumns,
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

  test('generates canonical SQLite SQL', () => {
    expect(liteIndexPredicateSQL(predicate)).toBe(
      `("active" = 1 AND ("deleted_at" IS NULL OR "attempts" >= -5 OR "kind""quoted" <> 'it''s \\ unicode ✓'))`,
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

  test('partial indexes are never unique on the replica', () => {
    const db = new Database(createSilentLogContext(), ':memory:');
    db.exec(`CREATE TABLE item(id INTEGER, code TEXT, active BOOL);`);
    const stmt = createLiteIndexStatement({
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
    });
    expect(stmt).toBe(
      `CREATE  INDEX "active_code" ON "item" ("code" ASC) WHERE "active" = 1;`,
    );
    db.exec(stmt);

    // Rows that would violate a UNIQUE partial index are accepted.
    db.exec(`
      INSERT INTO item VALUES (1, 'same', 1), (2, 'same', 1);
    `);
    expect(
      db
        .prepare(`SELECT "unique", partial FROM pragma_index_list('item')`)
        .all(),
    ).toEqual([{unique: 0, partial: 1}]);
  });
});
