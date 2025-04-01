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
import type {ServerSchema} from './schema.ts';

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
  "name" TEXT COLLATE "es-x-icu" NOT NULL
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

const serverSchema: ServerSchema = {
  item: {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
  },
} as const;

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
  // Test data that will compare differently in the table's default collation
  // then our desired collation.
  const testData = {
    item: [
      {id: '1', name: 'a'},
      {id: '2', name: 'ä'},
      {id: '3', name: 'ñ'},
      {id: '4', name: 'z'},
      {id: '5', name: 'Ω'},
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
      const pgResult = await runAsSQL(query, runPgQuery);
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
            "id": "1",
            "name": "a",
          },
          {
            "id": "4",
            "name": "z",
          },
          {
            "id": "2",
            "name": "ä",
          },
          {
            "id": "3",
            "name": "ñ",
          },
          {
            "id": "5",
            "name": "Ω",
          },
        ]
      `);

      function makeQuery(
        query: Query<Schema, 'item'>,
        i: number,
      ): Query<Schema, 'item'> {
        return query
          .where('name', '>', memoryResult[i].name)
          .limit(1)
          .orderBy('name', 'asc');
      }
      for (let i = 1; i < memoryResult.length - 1; i++) {
        const memResult = await makeQuery(memoryItemQuery, i).run();
        const zqlResult = mapResultToClientNames(
          await makeQuery(itemQuery, i).run(),
          schema,
          'item',
        );
        const pgResult = await runAsSQL(makeQuery(query, i), runPgQuery);
        expect(zqlResult).toEqualPg(pgResult);
        expect(memResult).toEqualPg(pgResult);
      }
    });
  }
});

async function runAsSQL(
  q: Query<Schema, 'item'>,
  runPgQuery: (query: string, args: unknown[]) => Promise<unknown>,
) {
  const c = compile(ast(q), schema.tables, serverSchema);
  const sqlQuery = formatPgInternalConvert(c);
  return extractZqlResult(
    await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
  );
}
