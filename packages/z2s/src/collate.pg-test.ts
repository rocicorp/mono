import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {type PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {type Row} from '../../zero-protocol/src/data.ts';
import {
  completedAstSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../zql/src/query/query-impl.ts';
import {type Query} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPgInternalConvert} from './sql.ts';
import {Client} from 'pg';
import './test/comparePg.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../zql/src/query/test/query-delegate.ts';
import {fillPgAndSync} from './test/setup.ts';

const lc = createSilentLogContext();

const DB_NAME = 'collate-test';

let pg: PostgresDB;
let nodePostgres: Client;
let sqlite: Database;
let memoryQueryDelegate: QueryDelegate;
let memoryItemQuery: Query<Schema, 'item'>;

export const createTableSQL = /*sql*/ `
CREATE TABLE "item" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);
`;

const item = table('item')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [item],
});
type Schema = typeof schema;

let itemQuery: Query<Schema, 'item'>;

function makeMemorySources() {
  return Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  );
}

beforeAll(async () => {
  // Test data with various collation challenges:
  const testData = {
    item: [
      {id: '1', name: 'n'},
      {id: '2', name: 'ñ'},
      {id: '3', name: 'Banana'},
      {id: '4', name: 'banana'},
      {id: '5', name: 'Café'},
      {id: '6', name: 'café'},
      {id: '7', name: 'cafe'},
      {id: '8', name: 'École'},
      {id: '9', name: 'école'},
      {id: '10', name: '1'},
      {id: '11', name: '2'},
      {id: '12', name: '10'},
      {id: '13', name: 'o'},
      {id: '14', name: 'résumé'},
      {id: '15', name: 'resume'},
    ],
  };

  const setup = await fillPgAndSync(schema, createTableSQL, testData, DB_NAME);
  pg = setup.pg;
  sqlite = setup.sqlite;

  const queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  itemQuery = newQuery(queryDelegate, schema, 'item');

  // Set up memory query
  const memorySources = makeMemorySources();
  memoryQueryDelegate = new TestMemoryQueryDelegate(memorySources);
  memoryItemQuery = newQuery(memoryQueryDelegate, schema, 'item');

  // Initialize memory sources with test data
  for (const row of testData.item) {
    memorySources.item.push({
      type: 'add',
      row,
    });
  }

  // Check that PG, SQLite, and test data are in sync
  const [itemPgRows] = await Promise.all([pg`SELECT * FROM "item"`]);
  expect(mapResultToClientNames(itemPgRows, schema, 'item')).toEqual(
    testData.item,
  );

  const [itemLiteRows] = [
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "item"').all<Row>(),
      schema,
      'item',
    ) as Schema['tables']['item'][],
  ];
  expect(
    itemLiteRows.map(row => fromSQLiteTypes(schema.tables.item.columns, row)),
  ).toEqual(testData.item);

  const {host, port, user, pass} = pg.options;
  nodePostgres = new Client({
    user,
    host: host[0],
    port: port[0],
    password: pass ?? undefined,
    database: DB_NAME,
  });
  await nodePostgres.connect();
});

afterAll(async () => {
  await nodePostgres.end();
});

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

describe('collation behavior', () => {
  describe('postgres.js', () => {
    t((query: string, args: unknown[]) =>
      pg.unsafe(query, args as JSONValue[]),
    );
  });
  describe('node-postgres', () => {
    t(
      async (query: string, args: unknown[]) =>
        (await nodePostgres.query(query, args as JSONValue[])).rows,
    );
  });
  function t(runPgQuery: (query: string, args: unknown[]) => Promise<unknown>) {
    test('zql matches pg', async () => {
      const query = itemQuery.orderBy('name', 'asc');
      const c = compile(ast(query), schema.tables);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
      );
      const zqlResult = mapResultToClientNames(
        await query.run(),
        schema,
        'item',
      );
      const memoryResult = await memoryItemQuery.orderBy('name', 'asc').run();
      expect(zqlResult).toEqualPg(pgResult);
      expect(memoryResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "id": "10",
            "name": "1",
          },
          {
            "id": "12",
            "name": "10",
          },
          {
            "id": "11",
            "name": "2",
          },
          {
            "id": "3",
            "name": "Banana",
          },
          {
            "id": "5",
            "name": "Café",
          },
          {
            "id": "4",
            "name": "banana",
          },
          {
            "id": "7",
            "name": "cafe",
          },
          {
            "id": "6",
            "name": "café",
          },
          {
            "id": "1",
            "name": "n",
          },
          {
            "id": "13",
            "name": "o",
          },
          {
            "id": "15",
            "name": "resume",
          },
          {
            "id": "14",
            "name": "résumé",
          },
          {
            "id": "8",
            "name": "École",
          },
          {
            "id": "9",
            "name": "école",
          },
          {
            "id": "2",
            "name": "ñ",
          },
        ]
      `);
    });
  }
});
