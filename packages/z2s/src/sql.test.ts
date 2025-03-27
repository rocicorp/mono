import {describe, expect, test} from 'vitest';
import {formatPg, formatPgJson, jsonPackArg, sql} from './sql.ts';

test('identical values result in a single placeholder', () => {
  const userId = 1;
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "id" = ${userId} AND "id" = ${userId}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "id" = $1 AND "id" = $1",
      "values": [
        1,
      ],
    }
  `);

  const str = JSON.stringify({a: 1});
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "meta1" = ${str} AND "id" = ${userId} OR "meta2" = ${str} OR "otherId" = ${userId} AND foo = ${''}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "meta1" = $1 AND "id" = $2 OR "meta2" = $1 OR "otherId" = $2 AND foo = $3",
      "values": [
        "{"a":1}",
        1,
        "",
      ],
    }
  `);
});

describe('json arg packing', () => {
  test('single arg', () => {
    expect(
      formatPgJson(
        sql`SELECT * FROM "user" WHERE "id" = ${jsonPackArg('number', 1)} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = ($1->>0)::numeric",
        "values": [
          "[1]",
        ],
      }
    `);
  });

  // identical values should only be encoded once
  test('many equivalent args', () => {
    expect(
      formatPgJson(
        sql`SELECT * FROM "user" WHERE "id" = ${jsonPackArg('number', 1)} OR "other_id" = ${jsonPackArg('number', 1)}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = ($1->>0)::numeric OR "other_id" = ($1->>0)::numeric",
        "values": [
          "[1]",
        ],
      }
    `);
  });

  test('all types', () => {
    expect(
      formatPgJson(
        sql`SELECT * FROM "foo" WHERE "a" = ${jsonPackArg('json', {})} OR "b" = ${jsonPackArg('number', 1)} OR "c" = ${jsonPackArg('string', 'str')} OR "d" = ${jsonPackArg('boolean', true)}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "a" = $1->0 OR "b" = ($1->>1)::numeric OR "c" = $1->>2 OR "d" = ($1->>3)::boolean",
        "values": [
          "[{},1,"str",true]",
        ],
      }
    `);
  });
});
