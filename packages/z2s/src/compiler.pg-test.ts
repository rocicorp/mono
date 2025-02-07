import './test/nullish.ts';
import {beforeAll} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {compile} from './compiler.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {formatSqlite, sql} from './sql.ts';
import {type Query} from '../../zql/src/query/query.ts';
import {
  astForTestingSymbol,
  newQuery,
  QueryImpl,
} from '../../zql/src/query/query-impl.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {describe, expect, test} from 'vitest';
import {formatPg} from './sql.ts';
import type {LogConfig} from '../../otel/src/log-options.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {fromSQLiteTypes, toSQLiteTypes} from '../../zqlite/src/table-source.ts';
import {type Row} from '../../zero-protocol/src/data.ts';

const lc = createSilentLogContext();
const logConfig: LogConfig = {
  format: 'text',
  level: 'debug',
  ivmSampling: 0,
  slowRowThreshold: 0,
};

const createTableSQL = /*sql*/ `
CREATE TABLE IF NOT EXISTS "issue" (
  "id" TEXT PRIMARY KEY,
  "title" TEXT NOT NULL,
  "description" TEXT NOT NULL,
  "closed" BOOLEAN NOT NULL,
  "ownerId" TEXT
);

CREATE TABLE IF NOT EXISTS "user" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL,
  "metadata" JSONB
);

CREATE TABLE IF NOT EXISTS "comment" (
  "id" TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  "issueId" TEXT NOT NULL,
  "text" TEXT NOT NULL,
  "createdAt" BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS "issueLabel" (
  "issueId" TEXT NOT NULL,
  "labelId" TEXT NOT NULL,
  PRIMARY KEY ("issueId", "labelId")
);

CREATE TABLE IF NOT EXISTS "label" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "revision" (
  "id" TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  "commentId" TEXT NOT NULL,
  "text" TEXT NOT NULL
);
`;

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;

let issueQuery: Query<Schema, 'issue'>;

beforeAll(async () => {
  pg = await testDBs.create('compiler');
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');
  sqlite.exec(createTableSQL);

  const testData = {
    issue: Array.from({length: 3}, (_, i) => ({
      id: `issue${i + 1}`,
      title: `Test Issue ${i + 1}`,
      description: `Description for issue ${i + 1}`,
      closed: i % 2 === 0,
      ownerId: i === 0 ? null : `user${i}`,
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
      createdAt: Date.now() - i * 86400000,
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

  for (const [table, rows] of Object.entries(testData)) {
    const columns = Object.keys(rows[0]);
    await pg`INSERT INTO ${pg(table)} ${pg(rows)}`;
    const sqliteSql = formatSqlite(
      sql`INSERT INTO ${sql.ident(table)} (${sql.join(
        columns.map(c => sql.ident(c)),
        ', ',
      )}) VALUES (${sql.join(
        Object.values(columns).map(_ => sql`?`),
        ',',
      )})`,
    );
    const stmt = sqlite.prepare(sqliteSql.text);
    for (const row of rows) {
      stmt.run(
        toSQLiteTypes(
          columns,
          row,
          schema.tables[table as keyof Schema['tables']].columns,
        ),
      );
    }
  }

  const queryDelegate = newQueryDelegate(lc, logConfig, sqlite, schema);

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
    pg`SELECT * FROM "issue"`,
    pg`SELECT * FROM "user"`,
    pg`SELECT * FROM "comment"`,
    pg`SELECT * FROM "issueLabel"`,
    pg`SELECT * FROM "label"`,
    pg`SELECT * FROM "revision"`,
  ]);
  expect(issuePgRows).toEqual(testData.issue);
  expect(userPgRows).toEqual(testData.user);
  expect(commentPgRows.map(noBigint)).toEqual(testData.comment);
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
  ] = await Promise.all([
    sqlite.prepare('SELECT * FROM "issue"').all<Row>(),
    sqlite.prepare('SELECT * FROM "user"').all<Row>(),
    sqlite.prepare('SELECT * FROM "comment"').all<Row>(),
    sqlite.prepare('SELECT * FROM "issueLabel"').all<Row>(),
    sqlite.prepare('SELECT * FROM "label"').all<Row>(),
    sqlite.prepare('SELECT * FROM "revision"').all<Row>(),
  ]);
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
  return (q as QueryImpl<Schema, keyof Schema['tables']>)[astForTestingSymbol];
}

function format(q: Query<Schema, keyof Schema['tables']>) {
  return (q as QueryImpl<Schema, keyof Schema['tables']>).format;
}

function noBigint(row: Record<string, unknown>) {
  if ('createdAt' in row) {
    return {
      ...row,
      createdAt: Number(row.createdAt as bigint),
    };
  }
  return row;
}

describe('compiling ZQL to SQL', () => {
  test('basic where clause', async () => {
    const query = issueQuery.where('title', '=', 'issue 1');
    const sqlQuery = formatPg(compile(ast(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });

  test('multiple where clauses', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .where('ownerId', 'IS NOT', null);
    const sqlQuery = formatPg(compile(ast(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });

  test('whereExists with related table', async () => {
    const query = issueQuery.whereExists('labels', q =>
      q.where('name', '=', 'bug'),
    );
    const sqlQuery = formatPg(compile(ast(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });

  test('order by and limit', async () => {
    const query = issueQuery.orderBy('title', 'desc').limit(5);
    const sqlQuery = formatPg(compile(ast(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });

  test('1 to 1 foreign key relationship', async () => {
    const query = issueQuery.related('owner');
    const sqlQuery = formatPg(compile(ast(query), format(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqualNullish(pgResult);
  });

  test('1 to many foreign key relationship', async () => {
    const query = issueQuery.related('comments');
    const sqlQuery = formatPg(compile(ast(query), format(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqualNullish(pgResult);
  });

  test.fails('junction relationship', async () => {
    const query = issueQuery.related('labels');
    const sqlQuery = formatPg(compile(ast(query), format(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqualNullish(pgResult);
  });

  test('nested related with where clauses', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .related('comments', q =>
        q.where('createdAt', '>', 1000).related('author'),
      );
    const sqlQuery = formatPg(compile(ast(query), format(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });

  test.fails('complex query combining multiple features', async () => {
    const query = issueQuery
      .where('closed', '=', false)
      .whereExists('labels', q => q.where('name', 'IN', ['Label 1', 'Label 2']))
      .related('owner')
      .related('comments', q =>
        q.orderBy('createdAt', 'desc').limit(3).related('author'),
      )
      .orderBy('title', 'asc');
    const sqlQuery = formatPg(compile(ast(query)));
    const pgResult = await pg.unsafe(
      sqlQuery.text,
      sqlQuery.values as JSONValue[],
    );
    expect(query.run()).toEqual(pgResult);
  });
});
