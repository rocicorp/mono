import {beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import {type PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {type Row} from '../../zero-protocol/src/data.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import {
  completedAstSymbol,
  newQuery,
  QueryImpl,
} from '../../zql/src/query/query-impl.ts';
import {type Query} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPg} from './sql.ts';
import './test/comparePg.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
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

let issueQuery: Query<Schema, 'issue'>;

beforeAll(async () => {
  pg = await testDBs.create('compiler', undefined, false);
  await pg.unsafe(createTableSQL);
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
    {appID: 'compiler_pg_test', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1, rowBatchSize: 10000},
  );

  const queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);

  issueQuery = newQuery(queryDelegate, schema, 'issue');

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
    issueLiteRows.map(row => fromSQLiteTypes(schema.tables.issue.columns, row)),
  ).toEqual(testData.issue);
  expect(
    commentLiteRows
      .map(row => fromSQLiteTypes(schema.tables.comment.columns, row))
      .map(mapHash),
  ).toEqual(testData.comment);
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

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

describe('compiling ZQL to SQL', () => {
  test('All bigints in safe Number range', async () => {
    const query = issueQuery.related('comments').limit(2);
    const c = compile(ast(query), schema.tables);
    const sqlQuery = formatPg(c);
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
    expect(pgResult).toMatchInlineSnapshot(`
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
    const c = compile(ast(query), schema.tables);
    const sqlQuery = formatPg(c);
    const result = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(() => extractZqlResult(result)).toThrowErrorMatchingInlineSnapshot(
      `[Error: Value exceeds safe Number range. [2]['comments'][1]['hash'] = 9007199254740992]`,
    );
  });
});
