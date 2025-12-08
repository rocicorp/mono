import {Client} from 'pg';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import {type PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {type Row} from '../../zero-protocol/src/data.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  enumeration,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import type {QueryBuilder} from '../../zql/src/query/query-builder.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../zql/src/query/test/query-delegate.ts';
import type {Database} from '../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import './helpers/comparePg.ts';
import {fillPgAndSync} from './helpers/setup.ts';

const lc = createSilentLogContext();

const DB_NAME = 'collate-test';

let pg: PostgresDB;
let nodePostgres: Client;
let sqlite: Database;
let queryDelegate: QueryDelegate;
let memoryQueryDelegate: QueryDelegate;

const createTableSQL = /*sql*/ `
CREATE TYPE size AS ENUM('s', 'm', 'l', 'xl'); 

CREATE TABLE "item" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT COLLATE "es-x-icu" NOT NULL,
  "uuid" UUID NOT NULL,
  "size" size NOT NULL
);
`;

const item = table('item')
  .columns({
    id: string(),
    name: string(),
    uuid: string(),
    size: enumeration(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [item],
});
type Schema = typeof schema;

const serverSchema: ServerSchema = {
  item: {
    id: {type: 'text', isEnum: false, isArray: false},
    name: {type: 'text', isEnum: false, isArray: false},
    uuid: {type: 'uuid', isEnum: false, isArray: false},
    size: {type: 'size', isEnum: true, isArray: false},
  },
} as const;

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
      {
        id: '1',
        name: 'a',
        uuid: '10000000-0000-0000-0000-000000000000',
        size: 's',
      },
      {
        id: '2',
        name: 'ä',
        uuid: '20000000-0000-0000-0000-000000000000',
        size: 'm',
      },
      {
        id: '3',
        name: 'ñ',
        uuid: 'a0000000-0000-0000-0000-000000000000',
        size: 'l',
      },
      {
        id: '4',
        name: 'z',
        uuid: '30000000-0000-0000-0000-000000000000',
        size: 's',
      },
      {
        id: '5',
        name: 'Ω',
        uuid: 'f0000000-0000-0000-0000-000000000000',
        size: 'xl',
      },
    ],
  };

  const setup = await fillPgAndSync(schema, createTableSQL, testData, DB_NAME);
  pg = setup.pg;
  sqlite = setup.sqlite;

  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);

  // Set up memory query
  const memorySources = makeMemorySources();
  memoryQueryDelegate = new TestMemoryQueryDelegate({sources: memorySources});

  // Initialize memory sources with test data
  for (const row of testData.item) {
    consume(
      memorySources.item.push({
        type: 'add',
        row,
      }),
    );
  }

  // Check that PG, SQLite, and test data are in sync
  const itemPgRows = await pg`SELECT * FROM "item"`;
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
    itemLiteRows.map(row =>
      fromSQLiteTypes(schema.tables.item.columns, row, 'item'),
    ),
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
  function t(
    runPgQuery: (query: string, args: unknown[]) => Promise<unknown[]>,
  ) {
    async function testColumn(col: 'name' | 'size' | 'uuid') {
      const itemQuery = newQuery(schema, 'item');
      const query = itemQuery.orderBy(col, 'asc');
      const pgResult = await runAsSQL(query, runPgQuery);
      const zqlResult = mapResultToClientNames(
        await queryDelegate.run(query),
        schema,
        'item',
      );
      const memoryItemQuery = newQuery(schema, 'item');
      const memoryResult = await memoryQueryDelegate.run(
        memoryItemQuery.orderBy(col, 'asc'),
      );
      expect(zqlResult).toEqualPg(pgResult);
      expect(memoryResult).toEqualPg(pgResult);

      function makeQuery(
        query: QueryBuilder<'item', Schema>,
        i: number,
      ): QueryBuilder<'item', Schema> {
        return query
          .where(col, '>', memoryResult[i].name)
          .limit(1)
          .orderBy(col, 'asc');
      }
      for (let i = 0; i < memoryResult.length - 1; i++) {
        const memResult = await memoryQueryDelegate.run(
          makeQuery(memoryItemQuery, i),
        );
        const zqlResult = mapResultToClientNames(
          await queryDelegate.run(makeQuery(itemQuery, i)),
          schema,
          'item',
        );
        const pgResult = await runAsSQL(makeQuery(itemQuery, i), runPgQuery);
        expect(zqlResult).toEqualPg(pgResult);
        expect(memResult).toEqualPg(pgResult);
      }

      return zqlResult;
    }

    test('zql matches pg, text column', async () => {
      expect(await testColumn('name')).toMatchInlineSnapshot(`
        [
          {
            "id": "1",
            "name": "a",
            "size": "s",
            "uuid": "10000000-0000-0000-0000-000000000000",
          },
          {
            "id": "4",
            "name": "z",
            "size": "s",
            "uuid": "30000000-0000-0000-0000-000000000000",
          },
          {
            "id": "2",
            "name": "ä",
            "size": "m",
            "uuid": "20000000-0000-0000-0000-000000000000",
          },
          {
            "id": "3",
            "name": "ñ",
            "size": "l",
            "uuid": "a0000000-0000-0000-0000-000000000000",
          },
          {
            "id": "5",
            "name": "Ω",
            "size": "xl",
            "uuid": "f0000000-0000-0000-0000-000000000000",
          },
        ]
      `);
    });

    test('zql matches pg, enum column', async () => {
      expect(await testColumn('size')).toMatchInlineSnapshot(`
        [
          {
            "id": "3",
            "name": "ñ",
            "size": "l",
            "uuid": "a0000000-0000-0000-0000-000000000000",
          },
          {
            "id": "2",
            "name": "ä",
            "size": "m",
            "uuid": "20000000-0000-0000-0000-000000000000",
          },
          {
            "id": "1",
            "name": "a",
            "size": "s",
            "uuid": "10000000-0000-0000-0000-000000000000",
          },
          {
            "id": "4",
            "name": "z",
            "size": "s",
            "uuid": "30000000-0000-0000-0000-000000000000",
          },
          {
            "id": "5",
            "name": "Ω",
            "size": "xl",
            "uuid": "f0000000-0000-0000-0000-000000000000",
          },
        ]
      `);
    });

    test('zql matches pg, uuid column', async () => {
      expect(await testColumn('uuid')).toMatchInlineSnapshot(`
        [
          {
            "id": "1",
            "name": "a",
            "size": "s",
            "uuid": "10000000-0000-0000-0000-000000000000",
          },
          {
            "id": "2",
            "name": "ä",
            "size": "m",
            "uuid": "20000000-0000-0000-0000-000000000000",
          },
          {
            "id": "4",
            "name": "z",
            "size": "s",
            "uuid": "30000000-0000-0000-0000-000000000000",
          },
          {
            "id": "3",
            "name": "ñ",
            "size": "l",
            "uuid": "a0000000-0000-0000-0000-000000000000",
          },
          {
            "id": "5",
            "name": "Ω",
            "size": "xl",
            "uuid": "f0000000-0000-0000-0000-000000000000",
          },
        ]
      `);
    });
  }
});

async function runAsSQL(
  q: QueryBuilder<'item', Schema>,
  runPgQuery: (query: string, args: unknown[]) => Promise<unknown[]>,
) {
  const c = compile(serverSchema, schema, asQueryInternals(q).ast);
  const sqlQuery = formatPgInternalConvert(c);
  return extractZqlResult(
    await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
  );
}
