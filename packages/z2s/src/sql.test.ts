import {expect, test} from 'vitest';
import {formatPg, sql} from './sql.ts';

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
