import type {SQLQuery} from '@databases/sql';
import type {JSONValue} from 'postgres';
import {beforeAll, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {formatPgInternalConvert, sql, sqlConvertColumnArg} from './sql.ts';

const DB_NAME = 'sql-test';

let pg: PostgresDB;
beforeAll(async () => {
  pg = await testDBs.create(DB_NAME, undefined, false);
  await pg.unsafe(`
    CREATE TABLE test_items (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      value NUMERIC,
      metadata JSONB,
      "isActive" BOOLEAN,
      "createdAt" TIMESTAMP WITH TIME ZONE,
      tags TEXT[]
    );
  `);
});

describe('SQL builder with PostgreSQL', () => {
  test('where & any', async () => {
    // Insert test data
    const now = Date.now();
    const items = [
      {
        name: 'item1',
        value: 42.5,
        metadata: {key: 'value1'},
        isActive: true,
        createdAt: now,
        tags: ['tag1', 'tag2'],
      },
      {
        name: 'item2',
        value: 123.45,
        metadata: {key: 'value2'},
        isActive: false,
        createdAt: now + 1000,
        tags: ['tag2', 'tag3'],
      },
    ];

    // Insert using SQL builder
    for (const item of items) {
      const {text, values} = formatPgInternalConvert(
        sql`
          INSERT INTO test_items (
            name, value, metadata, "isActive", "createdAt", tags
          ) VALUES (
            ${sqlConvertArg('text', item.name)},
            ${sqlConvertArg('numeric', item.value)},
            ${sqlConvertArg('json', item.metadata)},
            ${sqlConvertArg('boolean', item.isActive)},
            ${sqlConvertArg('timestamptz', item.createdAt)},
            ${sqlConvertArg('text', item.tags, plural)}
          )
        `,
      );
      await pg.unsafe(text, values as JSONValue[]);
    }

    // Test SELECT with WHERE and ANY clauses
    //  `ANY` works against arrays and `IN` works against table valued functions.
    const values = [42.5, 123.45];
    const timestamps = [now, now + 1000];
    const {text: selectText, values: selectValues} = formatPgInternalConvert(
      sql`
        SELECT
          id,
          name,
          value,
          metadata,
          "isActive",
          "createdAt",
          tags
        FROM test_items
        WHERE
          value = ANY (${sqlConvertArg('numeric', values, pluralComparison)})
          AND "createdAt" = ANY (${sqlConvertArg('timestamptz', timestamps, pluralComparison)})
          AND "isActive" = ${sqlConvertArg('boolean', true, singularComparison)}
          AND metadata->>'key' = ${sqlConvertArg('text', 'value1', singularComparison)}
          AND 'tag1' = ANY(tags)
        ORDER BY id
      `,
    );
    const result = await pg.unsafe(selectText, selectValues as JSONValue[]);

    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      name: 'item1',
      value: '42.5', // the numeric column gets converted to a string, on read, by the postgres bindings
      metadata: {key: 'value1'},
      isActive: true,
      tags: ['tag1', 'tag2'],
    });
  });
});

const pluralComparison = {
  plural: true,
  comparison: true,
};

const singularComparison = {
  comparison: true,
};

const plural = {
  plural: true,
};

function sqlConvertArg(
  type: string,
  value: unknown,
  {plural, comparison}: {plural?: boolean; comparison?: boolean} = {},
): SQLQuery {
  return sqlConvertColumnArg(
    {
      isArray: false,
      isEnum: false,
      type,
    },
    value,
    !!plural,
    !!comparison,
  );
}
