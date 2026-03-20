import type {SQLQuery} from '@databases/sql';
import type {JSONValue} from 'postgres';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
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

afterAll(async () => {
  await testDBs.drop(pg);
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

  test.each([
    {label: 'empty string', type: 'json', value: ''},
    {label: 'empty string', type: 'jsonb', value: ''},
    {label: 'non-empty string', type: 'json', value: 'same'},
    {label: 'non-empty string', type: 'jsonb', value: 'same'},
  ] as const)(
    'writes equal $label values correctly when text is formatted before $type',
    async ({label: _label, type, value}) => {
      const table = `mixed_values_${type}`;
      await pg.unsafe(`
        DROP TABLE IF EXISTS "${table}";
        CREATE TABLE "${table}" (
          summary TEXT NOT NULL,
          content ${type.toUpperCase()} NOT NULL
        );
      `);

      const stmt = formatPgInternalConvert(sql`
        INSERT INTO ${sql.ident(table)} (summary, content)
        VALUES (${sqlConvertArg('text', value)}, ${sqlConvertArg(type, value)})
        RETURNING summary, content
      `);
      const result = await pg.unsafe(stmt.text, stmt.values as JSONValue[]);

      expect(result).toEqual([{summary: value, content: value}]);
    },
  );

  test.each([
    {label: 'empty string', type: 'json', value: ''},
    {label: 'empty string', type: 'jsonb', value: ''},
    {label: 'non-empty string', type: 'json', value: 'same'},
    {label: 'non-empty string', type: 'jsonb', value: 'same'},
  ] as const)(
    'writes equal $label values correctly when $type is formatted before text',
    async ({label: _label, type, value}) => {
      const table = `mixed_values_${type}`;
      await pg.unsafe(`
        DROP TABLE IF EXISTS "${table}";
        CREATE TABLE "${table}" (
          summary TEXT NOT NULL,
          content ${type.toUpperCase()} NOT NULL
        );
      `);

      const stmt = formatPgInternalConvert(sql`
        INSERT INTO ${sql.ident(table)} (content, summary)
        VALUES (${sqlConvertArg(type, value)}, ${sqlConvertArg('text', value)})
        RETURNING content, summary
      `);
      const result = await pg.unsafe(stmt.text, stmt.values as JSONValue[]);

      expect(result).toEqual([{content: value, summary: value}]);
    },
  );

  test.each([
    {type: 'time', value: 32887654},
    {type: 'timetz', value: 32887654},
  ] as const)(
    'numeric $type inserts round-trip as milliseconds',
    async ({type, value}) => {
      await using pg = await testDBs.create(
        `${DB_NAME}_${type}`,
        undefined,
        {},
      );

      const table = `round_trip_${type}`;
      await pg.unsafe(`
        SET TIME ZONE 'UTC';
        DROP TABLE IF EXISTS "${table}";
        CREATE TABLE "${table}" (
          value ${type.toUpperCase()} NOT NULL
        );
      `);

      const stmt = formatPgInternalConvert(sql`
        INSERT INTO ${sql.ident(table)} (value)
        VALUES (${sqlConvertArg(type, value)})
        RETURNING value
      `);

      const result = await pg.unsafe(stmt.text, stmt.values as JSONValue[]);

      expect(result).toEqual([{value}]);
    },
  );
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
