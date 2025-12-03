import {Client} from 'pg';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import {type PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {type Row} from '../../zero-protocol/src/data.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {getServerSchema} from '../../zero-server/src/schema.ts';
import {Transaction} from '../../zero-server/src/test/util.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {type Query} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import './helpers/comparePg.ts';

const lc = createSilentLogContext();

const DB_NAME = 'compiler-types-with-params';

let pg: PostgresDB;
let nodePostgres: Client;
let sqlite: Database;

export const createTableSQL = /*sql*/ `
CREATE TABLE IF NOT EXISTS "issue" (
  "id" TEXT PRIMARY KEY,
  "title" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "comment" (
  "id" TEXT PRIMARY KEY,
  "issueId" TEXT NOT NULL,
  "hash" char(6) NOT NULL,
  "weight" numeric(10, 2) NOT NULL
);
`;

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
  })
  .primaryKey('id');

const comment = table('comment')
  .columns({
    id: string(),
    issueId: string(),
    hash: string(),
    weight: number(),
  })
  .primaryKey('id');

const issueRelationships = relationships(issue, ({many}) => ({
  comments: many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
}));

const schema = createSchema({
  tables: [issue, comment],
  relationships: [issueRelationships],
});
type Schema = typeof schema;

let serverSchema: ServerSchema;

let issueQuery: Query<'issue', Schema>;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  pg = await testDBs.create(DB_NAME, undefined, false);
  await pg.unsafe(createTableSQL);

  serverSchema = await pg.begin(tx =>
    getServerSchema(new Transaction(tx), schema),
  );

  sqlite = new Database(lc, ':memory:');
  const testData = {
    issue: Array.from({length: 3}, (_, i) => ({
      id: `issue${i + 1}`,
      title: `Test Issue ${i + 1}`,
    })),
    comment: Array.from({length: 6}, (_, i) => ({
      id: `comment${i + 1}`,
      issueId: `issue${Math.ceil((i + 1) / 2)}`,
      // all but comment6 have values < Number.MAX_SAFE_INTEGER
      hash: `hash-${i + 1}`,
      weight: Number(`${i + 1}.${i + 1}${i + 1}`),
    })),
  };

  const mapper = clientToServer(schema.tables);
  for (const [table, rows] of Object.entries(testData)) {
    const columns = Object.keys(rows[0]);
    const forPg = rows.map(row =>
      columns.reduce(
        (acc, c) => ({
          ...acc,
          [mapper.columnName(table, c)]: row[c as keyof typeof row],
        }),
        {} as Record<string, unknown>,
      ),
    );
    await pg`INSERT INTO ${pg(mapper.tableName(table))} ${pg(forPg)}`;
  }
  await initialSync(
    lc,
    {appID: 'compiler_with_types_pg_test', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);

  issueQuery = newQuery(schema, 'issue');

  // Check that PG, SQLite, and test data are in sync
  const [issuePgRows, commentPgRows] = await Promise.all([
    pg`SELECT * FROM "issue"`,
    pg`SELECT * FROM "comment"`,
  ]);
  expect(mapResultToClientNames(issuePgRows, schema, 'issue')).toEqual(
    testData.issue,
  );
  expect(
    mapResultToClientNames(commentPgRows.map(mapWeight), schema, 'comment'),
  ).toEqual(testData.comment);

  const [issueLiteRows, commentLiteRows] = [
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "issue"').all<Row>(),
      schema,
      'issue',
    ) as Schema['tables']['issue'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "comment"').all<Row>(),
      schema,
      'comment',
    ) as Schema['tables']['comment'][],
  ];
  expect(
    issueLiteRows.map(row =>
      fromSQLiteTypes(schema.tables.issue.columns, row, 'issue'),
    ),
  ).toEqual(testData.issue);
  expect(
    commentLiteRows.map(row =>
      fromSQLiteTypes(schema.tables.comment.columns, row, 'comment'),
    ),
  ).toEqual(testData.comment);

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

describe('compiling ZQL to SQL', () => {
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
    test('basic', async () => {
      const query = issueQuery.related('comments');
      const c = compile(serverSchema, schema, asQueryInternals(query).ast);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
      );
      const zqlResult = mapResultToClientNames(
        await queryDelegate.run(query),
        schema,
        'issue',
      );
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "hash": "hash-1",
                "id": "comment1",
                "issueId": "issue1",
                "weight": 1.11,
              },
              {
                "hash": "hash-2",
                "id": "comment2",
                "issueId": "issue1",
                "weight": 2.22,
              },
            ],
            "id": "issue1",
            "title": "Test Issue 1",
          },
          {
            "comments": [
              {
                "hash": "hash-3",
                "id": "comment3",
                "issueId": "issue2",
                "weight": 3.33,
              },
              {
                "hash": "hash-4",
                "id": "comment4",
                "issueId": "issue2",
                "weight": 4.44,
              },
            ],
            "id": "issue2",
            "title": "Test Issue 2",
          },
          {
            "comments": [
              {
                "hash": "hash-5",
                "id": "comment5",
                "issueId": "issue3",
                "weight": 5.55,
              },
              {
                "hash": "hash-6",
                "id": "comment6",
                "issueId": "issue3",
                "weight": 6.66,
              },
            ],
            "id": "issue3",
            "title": "Test Issue 3",
          },
        ]
      `);
    });

    test('order by', async () => {
      const query = issueQuery
        .related('comments', q => q.orderBy('hash', 'asc'))
        .limit(2);
      const c = compile(serverSchema, schema, asQueryInternals(query).ast);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
      );
      const zqlResult = mapResultToClientNames(
        await queryDelegate.run(query),
        schema,
        'issue',
      );
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
              [
                {
                  "comments": [
                    {
                      "hash": "hash-1",
                      "id": "comment1",
                      "issueId": "issue1",
                      "weight": 1.11,
                    },
                    {
                      "hash": "hash-2",
                      "id": "comment2",
                      "issueId": "issue1",
                      "weight": 2.22,
                    },
                  ],
                  "id": "issue1",
                  "title": "Test Issue 1",
                },
                {
                  "comments": [
                    {
                      "hash": "hash-3",
                      "id": "comment3",
                      "issueId": "issue2",
                      "weight": 3.33,
                    },
                    {
                      "hash": "hash-4",
                      "id": "comment4",
                      "issueId": "issue2",
                      "weight": 4.44,
                    },
                  ],
                  "id": "issue2",
                  "title": "Test Issue 2",
                },
              ]
            `);
    });

    test('comparison operators with char(6)', async () => {
      // Here we test that while 'hash-51' is longer than the char(6) limit of
      // the hash column, we do not truncate it to 'hash-5' before comparison
      // thus we do not incorrectly include 'hash-5' in the results.
      const query = issueQuery.related('comments', q =>
        q.where('hash', '>=', 'hash-51'),
      );
      const c = compile(serverSchema, schema, asQueryInternals(query).ast);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
      );
      const zqlResult = mapResultToClientNames(
        await queryDelegate.run(query),
        schema,
        'issue',
      );
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "issue1",
          "title": "Test Issue 1",
        },
        {
          "comments": [],
          "id": "issue2",
          "title": "Test Issue 2",
        },
        {
          "comments": [
            {
              "hash": "hash-6",
              "id": "comment6",
              "issueId": "issue3",
              "weight": 6.66,
            },
          ],
          "id": "issue3",
          "title": "Test Issue 3",
        },
      ]
    `);

      const q2 = issueQuery.related('comments', q =>
        q.where('hash', '=', 'hash-1'),
      );
      const c2 = compile(serverSchema, schema, asQueryInternals(q2).ast);
      const sqlQuery2 = formatPgInternalConvert(c2);
      const pgResult2 = extractZqlResult(
        await runPgQuery(sqlQuery2.text, sqlQuery2.values as JSONValue[]),
      );
      const zqlResult2 = mapResultToClientNames(
        await queryDelegate.run(q2),
        schema,
        'issue',
      );
      expect(zqlResult2).toEqualPg(pgResult2);
      expect(zqlResult2).toMatchInlineSnapshot(`
              [
                {
                  "comments": [
                    {
                      "hash": "hash-1",
                      "id": "comment1",
                      "issueId": "issue1",
                      "weight": 1.11,
                    },
                  ],
                  "id": "issue1",
                  "title": "Test Issue 1",
                },
                {
                  "comments": [],
                  "id": "issue2",
                  "title": "Test Issue 2",
                },
                {
                  "comments": [],
                  "id": "issue3",
                  "title": "Test Issue 3",
                },
              ]
            `);
    });

    test('comparison operators with numeric(10, 2)', async () => {
      // Here we test that while '6.661' has more decimal places than
      // numeric(10, 2), we do not truncate it to 5.55 before comparison
      // thus we do not incorrectly include weight 5.55 in the results.
      const query = issueQuery.related('comments', q =>
        q.where('weight', '>=', 5.551),
      );
      const c = compile(serverSchema, schema, asQueryInternals(query).ast);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values as JSONValue[]),
      );
      const zqlResult = mapResultToClientNames(
        await queryDelegate.run(query),
        schema,
        'issue',
      );
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
              [
                {
                  "comments": [],
                  "id": "issue1",
                  "title": "Test Issue 1",
                },
                {
                  "comments": [],
                  "id": "issue2",
                  "title": "Test Issue 2",
                },
                {
                  "comments": [
                    {
                      "hash": "hash-6",
                      "id": "comment6",
                      "issueId": "issue3",
                      "weight": 6.66,
                    },
                  ],
                  "id": "issue3",
                  "title": "Test Issue 3",
                },
              ]
            `);

      const q2 = issueQuery.related('comments', q =>
        q.where('weight', '=', 1.11),
      );
      const c2 = compile(serverSchema, schema, asQueryInternals(q2).ast);
      const sqlQuery2 = formatPgInternalConvert(c2);
      const pgResult2 = extractZqlResult(
        await runPgQuery(sqlQuery2.text, sqlQuery2.values as JSONValue[]),
      );
      const zqlResult2 = mapResultToClientNames(
        await queryDelegate.run(q2),
        schema,
        'issue',
      );
      expect(zqlResult2).toEqualPg(pgResult2);
      expect(zqlResult2).toMatchInlineSnapshot(`
              [
                {
                  "comments": [
                    {
                      "hash": "hash-1",
                      "id": "comment1",
                      "issueId": "issue1",
                      "weight": 1.11,
                    },
                  ],
                  "id": "issue1",
                  "title": "Test Issue 1",
                },
                {
                  "comments": [],
                  "id": "issue2",
                  "title": "Test Issue 2",
                },
                {
                  "comments": [],
                  "id": "issue3",
                  "title": "Test Issue 3",
                },
              ]
            `);
    });
  }
});

function mapWeight(value: Row): Row {
  return {
    ...value,
    weight: Number(value['weight']),
  };
}
