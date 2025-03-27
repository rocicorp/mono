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
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {fromSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPg} from './sql.ts';
import './test/comparePg.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;

let issueQuery: Query<Schema, 'issue'>;

/**
 * NOTE: More comprehensive tests are being added to `test/chinook`.
 * These test will likely be deprecated.
 */
beforeAll(async () => {
  pg = await testDBs.create('compiler', undefined, false);
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');
  const testData = {
    issue: Array.from({length: 3}, (_, i) => ({
      id: `issue${i + 1}`,
      title: `Test Issue ${i + 1}`,
      description: `Description for issue ${i + 1}`,
      closed: i % 2 === 0,
      ownerId: i === 0 ? null : `user${i}`,
      createdAt: new Date(Date.now() - i * 86400000).getTime(),
    })),
    user: Array.from({length: 3}, (_, i) => ({
      id: `user${i + 1}`,
      name: `User ${i + 1}`,
      metadata:
        i === 0
          ? null
          : {
              registrar: i % 2 === 0 ? 'github' : 'google',
              email: `user${i + 1}@example.com`,
              altContacts: [`alt${i + 1}@example.com`],
            },
    })),
    comment: Array.from({length: 4}, (_, i) => ({
      id: `comment${i + 1}`,
      authorId: `user${(i % 3) + 1}`,
      issueId: `issue${(i % 3) + 1}`,
      text: `Comment ${i + 1} text`,
      createdAt: new Date(Date.now() - i * 86400000).getTime(),
    })),
    issueLabel: Array.from({length: 4}, (_, i) => ({
      issueId: `issue${(i % 3) + 1}`,
      labelId: `label${(i % 2) + 1}`,
    })),
    label: Array.from({length: 2}, (_, i) => ({
      id: `label${i + 1}`,
      name: `Label ${i + 1}`,
    })),
    revision: Array.from({length: 3}, (_, i) => ({
      id: `revision${i + 1}`,
      authorId: `user${(i % 3) + 1}`,
      commentId: `comment${(i % 4) + 1}`,
      text: `Revised text ${i + 1}`,
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
  const [
    issuePgRows,
    userPgRows,
    commentPgRows,
    issueLabelPgRows,
    labelPgRows,
    revisionPgRows,
  ] = await Promise.all([
    pg`SELECT "id", "title", "description", "closed", "owner_id", "createdAt" AT TIME ZONE 'UTC' as "createdAt" FROM "issues"`,
    pg`SELECT * FROM "users"`,
    pg`SELECT "id", "authorId", "issue_id", "text", "createdAt" AT TIME ZONE 'UTC' as "createdAt" FROM "comments"`,
    pg`SELECT * FROM "issueLabel"`,
    pg`SELECT * FROM "label"`,
    pg`SELECT * FROM "revision"`,
  ]);
  expect(
    mapResultToClientNames(issuePgRows.map(createdAtToMillis), schema, 'issue'),
  ).toEqual(testData.issue);
  expect(mapResultToClientNames(userPgRows, schema, 'user')).toEqual(
    testData.user,
  );
  expect(
    mapResultToClientNames(
      commentPgRows.map(createdAtToMillis),
      schema,
      'comment',
    ),
  ).toEqual(testData.comment);
  expect(issueLabelPgRows).toEqual(testData.issueLabel);
  expect(labelPgRows).toEqual(testData.label);
  expect(revisionPgRows).toEqual(testData.revision);

  const [
    issueLiteRows,
    userLiteRows,
    commentLiteRows,
    issueLabelLiteRows,
    labelLiteRows,
    revisionLiteRows,
  ] = [
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "issues"').all<Row>(),
      schema,
      'issue',
    ) as Schema['tables']['issue'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "users"').all<Row>(),
      schema,
      'user',
    ) as Schema['tables']['user'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "comments"').all<Row>(),
      schema,
      'comment',
    ) as Schema['tables']['comment'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "issueLabel"').all<Row>(),
      schema,
      'issueLabel',
    ) as Schema['tables']['issueLabel'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "label"').all<Row>(),
      schema,
      'label',
    ) as Schema['tables']['label'][],
    mapResultToClientNames(
      sqlite.prepare('SELECT * FROM "revision"').all<Row>(),
      schema,
      'revision',
    ) as Schema['tables']['revision'][],
  ];
  expect(
    issueLiteRows.map(row => fromSQLiteTypes(schema.tables.issue.columns, row)),
  ).toEqual(testData.issue);
  expect(
    userLiteRows.map(row => fromSQLiteTypes(schema.tables.user.columns, row)),
  ).toEqual(testData.user);
  expect(
    commentLiteRows.map(row =>
      fromSQLiteTypes(schema.tables.comment.columns, row),
    ),
  ).toEqual(testData.comment);
  expect(issueLabelLiteRows).toEqual(testData.issueLabel);
  expect(
    labelLiteRows.map(row => fromSQLiteTypes(schema.tables.label.columns, row)),
  ).toEqual(testData.label);
  expect(revisionLiteRows).toEqual(testData.revision);
});

function ast(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[completedAstSymbol];
}

function createdAtToMillis(row: Record<string, unknown>) {
  if ('createdAt' in row) {
    return {
      ...row,
      createdAt: (row.createdAt as Date).getTime(),
    };
  }
  return row;
}

describe('compiling ZQL to SQL', () => {
  test('basic where clause', async () => {
    const query = issueQuery.where('title', '=', 'issue 1');
    const c = compile(ast(query), schema.tables);
    const sqlQuery = formatPg(c);
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(mapResultToClientNames(await query.run(), schema, 'issue')).toEqual(
      pgResult,
    );
  });

  test('multiple where clauses', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .where('ownerId', 'IS NOT', null);
    const sqlQuery = formatPg(compile(ast(query), schema.tables));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('whereExists with related table', async () => {
    const query = issueQuery.whereExists('labels', q =>
      q.where('name', '=', 'bug'),
    );
    const sqlQuery = formatPg(compile(ast(query), schema.tables));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('order by and limit', async () => {
    const query = issueQuery.orderBy('title', 'desc').limit(5);
    const sqlQuery = formatPg(compile(ast(query), schema.tables));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('1 to 1 foreign key relationship', async () => {
    const query = issueQuery.related('owner');
    const sqlQuery = formatPg(compile(ast(query), schema.tables, query.format));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('1 to many foreign key relationship', async () => {
    const query = issueQuery.related('comments');
    const sqlQuery = formatPg(compile(ast(query), schema.tables, query.format));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('junction relationship', async () => {
    const query = issueQuery.related('labels');
    const sqlQuery = formatPg(compile(ast(query), schema.tables, query.format));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('nested related with where clauses', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .related('comments', q =>
        q.where('createdAt', '>', 1000).related('author'),
      );
    const sqlQuery = formatPg(compile(ast(query), schema.tables, query.format));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });

  test('complex query combining multiple features', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .whereExists('labels', q => q.where('name', 'IN', ['Label 1', 'Label 2']))
      .related('owner')
      .related('comments', q =>
        q.orderBy('createdAt', 'desc').limit(3).related('author'),
      )
      .orderBy('title', 'asc');
    const sqlQuery = formatPg(compile(ast(query), schema.tables, query.format));
    const pgResult = extractZqlResult(
      await pg.unsafe(sqlQuery.text, sqlQuery.values as JSONValue[]),
    );
    expect(
      mapResultToClientNames(await query.run(), schema, 'issue'),
    ).toEqualPg(pgResult);
  });
});
