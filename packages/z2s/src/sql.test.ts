import {describe, expect, test} from 'vitest';
import {formatPg, formatPgInternalConvert, sqlConvertArg, sql} from './sql.ts';

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
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertArg('number', 1)} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::numeric",
        "values": [
          "1",
        ],
      }
    `);
  });

  // identical values should only be encoded once
  test('many equivalent args', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertArg('number', 1)} OR "other_id" = ${sqlConvertArg('number', 1)}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::numeric OR "other_id" = $1::text::numeric",
        "values": [
          "1",
        ],
      }
    `);
  });

  test('all types', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "foo" WHERE "a" = ${sqlConvertArg('json', {})} OR "b" = ${sqlConvertArg('number', 1)} OR "c" = ${sqlConvertArg('string', 'str')} OR "d" = ${sqlConvertArg('boolean', true)}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "a" = $1::text::jsonb OR "b" = $2::text::numeric OR "c" = $3::text OR "d" = $4::text::boolean",
        "values": [
          "{}",
          "1",
          "str",
          "true",
        ],
      }
    `);
  });

  test('mapped and joined', () => {
    const values = [1, 2, 3];
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "foo" WHERE "a" ${sql.join(
          values.map(v => sqlConvertArg('number', v)),
          ' AND ',
        )} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "a" $1::text::numeric AND $2::text::numeric AND $3::text::numeric",
        "values": [
          "1",
          "2",
          "3",
        ],
      }
    `);
  });
});
