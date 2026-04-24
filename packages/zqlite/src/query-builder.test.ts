import {expect, test} from 'vitest';
import type {SchemaValue} from '../../zero-schema/src/table-schema.ts';
import {format} from './internal/sql.ts';
import {buildSelectQuery} from './query-builder.ts';

test('non-nullable cursor columns use range and equality operators without IS NULL guards', () => {
  const columns = {
    id: {type: 'string'},
    name: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        undefined,
        undefined,
        [
          ['id', 'asc'],
          ['name', 'desc'],
        ],
        undefined,
        {
          row: {id: 'issue-1', name: 'z'},
          basis: 'after',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "id","name" FROM "issues" WHERE (("id" > ?) OR ("id" = ? AND "name" < ?)) ORDER BY "id" asc, "name" desc",
      "values": [
        "issue-1",
        "issue-1",
        "z",
      ],
    }
  `);
});

test('optional cursor columns keep IS and IS NULL checks while non-nullable columns do not', () => {
  const columns = {
    owner: {type: 'string', optional: true},
    id: {type: 'string'},
  } as const satisfies Record<string, SchemaValue>;

  expect(
    format(
      buildSelectQuery(
        'issues',
        columns,
        undefined,
        undefined,
        [
          ['owner', 'asc'],
          ['id', 'asc'],
        ],
        undefined,
        {
          row: {owner: 'alice', id: 'issue-1'},
          basis: 'at',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT "owner","id" FROM "issues" WHERE (((? IS NULL OR "owner" > ?)) OR ("owner" IS ? AND "id" > ?) OR ("owner" IS ? AND "id" = ?)) ORDER BY "owner" asc, "id" asc",
      "values": [
        "alice",
        "alice",
        "alice",
        "issue-1",
        "alice",
        "issue-1",
      ],
    }
  `);
});
