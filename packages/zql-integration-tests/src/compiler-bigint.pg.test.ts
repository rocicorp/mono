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

const DB_NAME = 'compiler-bigint';

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
  "hash" BIGINT NOT NULL
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
    hash: number(),
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

let issueQuery: Query<'issue', Schema>;
let serverSchema: ServerSchema;
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
      hash: BigInt(Number.MAX_SAFE_INTEGER) - 4n + BigInt(i),
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
    {appID: 'compiler_bigint_pg_test', shardNum: 0, publications: []},
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
    mapResultToClientNames(commentPgRows.map(mapHash), schema, 'comment'),
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
    commentLiteRows
      .map(row =>
        fromSQLiteTypes(schema.tables.comment.columns, row, 'comment'),
      )
      .map(mapHash),
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

function mapHash(commentRow: Record<string, unknown>) {
  if ('hash' in commentRow) {
    return {
      ...commentRow,
      hash: BigInt(commentRow.hash as string | number),
    };
  }
  return commentRow;
}

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
    test('All bigints in safe Number range', async () => {
      const query = issueQuery.related('comments').limit(2);
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
                "hash": 9007199254740987,
                "id": "comment1",
                "issueId": "issue1",
              },
              {
                "hash": 9007199254740988,
                "id": "comment2",
                "issueId": "issue1",
              },
            ],
            "id": "issue1",
            "title": "Test Issue 1",
          },
          {
            "comments": [
              {
                "hash": 9007199254740989,
                "id": "comment3",
                "issueId": "issue2",
              },
              {
                "hash": 9007199254740990,
                "id": "comment4",
                "issueId": "issue2",
              },
            ],
            "id": "issue2",
            "title": "Test Issue 2",
          },
        ]
      `);
    });

    test('bigint exceeds safe range', async () => {
      const query = issueQuery.related('comments');
      const c = compile(serverSchema, schema, asQueryInternals(query).ast);
      const sqlQuery = formatPgInternalConvert(c);
      const result = await runPgQuery(
        sqlQuery.text,
        sqlQuery.values as JSONValue[],
      );
      expect(() => extractZqlResult(result)).toThrowErrorMatchingInlineSnapshot(
        `[Error: Value exceeds safe Number range. [2]['comments'][1]['hash'] = 9007199254740992]`,
      );
    });

    test('bigint comparison operators', async () => {
      const query = issueQuery.related('comments', q =>
        q
          .where('hash', '>', Number(Number.MAX_SAFE_INTEGER - 6))
          .where('hash', '<', Number(Number.MAX_SAFE_INTEGER))
          .where('hash', '!=', Number(Number.MAX_SAFE_INTEGER - 3)),
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
            "comments": [
              {
                "hash": 9007199254740987,
                "id": "comment1",
                "issueId": "issue1",
              },
            ],
            "id": "issue1",
            "title": "Test Issue 1",
          },
          {
            "comments": [
              {
                "hash": 9007199254740989,
                "id": "comment3",
                "issueId": "issue2",
              },
              {
                "hash": 9007199254740990,
                "id": "comment4",
                "issueId": "issue2",
              },
            ],
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

      const q2 = issueQuery.related('comments', q =>
        q.where('hash', '=', Number.MAX_SAFE_INTEGER - 3),
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
                "hash": 9007199254740988,
                "id": "comment2",
                "issueId": "issue1",
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
