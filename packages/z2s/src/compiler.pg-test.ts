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
import {formatPgInternalConvert} from './sql.ts';
import {Client} from 'pg';
import '../../zql-integration-tests/src/helpers/comparePg.ts';
import {fillPgAndSync} from '../../zql-integration-tests/src/helpers/setup.ts';
import type {ServerSchema} from './schema.ts';

const lc = createSilentLogContext();

const BASE_TIMESTAMP = 1743127752952;
const DB_NAME = 'compiler';

const serverSchema: ServerSchema = {
  issues: {
    id: {type: 'text', isEnum: false},
    title: {type: 'text', isEnum: false},
    description: {type: 'text', isEnum: false},
    closed: {type: 'boolean', isEnum: false},
    // eslint-disable-next-line @typescript-eslint/naming-convention
    owner_id: {type: 'text', isEnum: false},
    createdAt: {type: 'timestamp without time zone', isEnum: false},
  },
  users: {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
    metadata: {type: 'jsonb', isEnum: false},
  },
  comments: {
    id: {type: 'text', isEnum: false},
    authorId: {type: 'text', isEnum: false},
    // eslint-disable-next-line @typescript-eslint/naming-convention
    issue_id: {type: 'text', isEnum: false},
    text: {type: 'text', isEnum: false},
    createdAt: {type: 'timestamp without time zone', isEnum: false},
  },
  issueLabel: {
    issueId: {type: 'text', isEnum: false},
    labelId: {type: 'text', isEnum: false},
  },
  label: {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
  },
  revision: {
    id: {type: 'text', isEnum: false},
    authorId: {type: 'text', isEnum: false},
    commentId: {type: 'text', isEnum: false},
    text: {type: 'text', isEnum: false},
  },
} as const;

let pg: PostgresDB;
let nodePostgres: Client;
let sqlite: Database;
type Schema = typeof schema;

let issueQuery: Query<Schema, 'issue'>;

/**
 * NOTE: More comprehensive tests are being added to `test/chinook`.
 * These test will likely be deprecated.
 */
beforeAll(async () => {
  sqlite = new Database(lc, ':memory:');
  const testData = {
    issue: Array.from({length: 3}, (_, i) => ({
      id: `issue${i + 1}`,
      title: `Test Issue ${i + 1}`,
      description: `Description for issue ${i + 1}`,
      closed: i % 2 === 0,
      ownerId: i === 0 ? null : `user${i}`,
      createdAt: new Date(BASE_TIMESTAMP - i * 86400000).getTime(),
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
    comment: Array.from({length: 6}, (_, i) => ({
      id: `comment${i + 1}`,
      authorId: `user${(i % 3) + 1}`,
      issueId: `issue${(i % 3) + 1}`,
      text: `Comment ${i + 1} text`,
      createdAt: new Date(BASE_TIMESTAMP - i * 86400000).getTime(),
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

  const setup = await fillPgAndSync(schema, createTableSQL, testData, DB_NAME);
  pg = setup.pg;
  sqlite = setup.sqlite;

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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function ast(q: Query<Schema, keyof Schema['tables'], any>) {
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
    test('basic where clause', async () => {
      const query = issueQuery.where('title', '=', 'Test Issue 1');
      const c = compile(ast(query), schema.tables, serverSchema);
      const sqlQuery = formatPgInternalConvert(c);
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "closed": true,
            "createdAt": 1743127752952,
            "description": "Description for issue 1",
            "id": "issue1",
            "ownerId": null,
            "title": "Test Issue 1",
          },
        ]
      `);
    });

    test('multiple where clauses', async () => {
      const query = issueQuery
        .where('closed', '=', false)
        .where('ownerId', 'IS NOT', null);
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "createdAt": 1743041352952,
          "description": "Description for issue 2",
          "id": "issue2",
          "ownerId": "user1",
          "title": "Test Issue 2",
        },
      ]
    `);
    });

    test('whereExists with related table', async () => {
      const query = issueQuery.whereExists('labels', q =>
        q.where('name', '=', 'bug'),
      );
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`[]`);
    });

    test('order by and limit', async () => {
      const query = issueQuery.orderBy('title', 'desc').limit(5);
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "closed": true,
            "createdAt": 1742954952952,
            "description": "Description for issue 3",
            "id": "issue3",
            "ownerId": "user2",
            "title": "Test Issue 3",
          },
          {
            "closed": false,
            "createdAt": 1743041352952,
            "description": "Description for issue 2",
            "id": "issue2",
            "ownerId": "user1",
            "title": "Test Issue 2",
          },
          {
            "closed": true,
            "createdAt": 1743127752952,
            "description": "Description for issue 1",
            "id": "issue1",
            "ownerId": null,
            "title": "Test Issue 1",
          },
        ]
      `);
    });

    test('1 to 1 foreign key relationship', async () => {
      const query = issueQuery.related('owner');
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema, query.format),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "closed": true,
            "createdAt": 1743127752952,
            "description": "Description for issue 1",
            "id": "issue1",
            "owner": undefined,
            "ownerId": null,
            "title": "Test Issue 1",
          },
          {
            "closed": false,
            "createdAt": 1743041352952,
            "description": "Description for issue 2",
            "id": "issue2",
            "owner": {
              "id": "user1",
              "metadata": null,
              "name": "User 1",
            },
            "ownerId": "user1",
            "title": "Test Issue 2",
          },
          {
            "closed": true,
            "createdAt": 1742954952952,
            "description": "Description for issue 3",
            "id": "issue3",
            "owner": {
              "id": "user2",
              "metadata": {
                "altContacts": [
                  "alt2@example.com",
                ],
                "email": "user2@example.com",
                "registrar": "google",
              },
              "name": "User 2",
            },
            "ownerId": "user2",
            "title": "Test Issue 3",
          },
        ]
      `);
    });

    test('1 to many foreign key relationship', async () => {
      const query = issueQuery.related('comments');
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema, query.format),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
              [
                {
                  "closed": true,
                  "comments": [
                    {
                      "authorId": "user1",
                      "createdAt": 1743127752952,
                      "id": "comment1",
                      "issueId": "issue1",
                      "text": "Comment 1 text",
                    },
                    {
                      "authorId": "user1",
                      "createdAt": 1742868552952,
                      "id": "comment4",
                      "issueId": "issue1",
                      "text": "Comment 4 text",
                    },
                  ],
                  "createdAt": 1743127752952,
                  "description": "Description for issue 1",
                  "id": "issue1",
                  "ownerId": null,
                  "title": "Test Issue 1",
                },
                {
                  "closed": false,
                  "comments": [
                    {
                      "authorId": "user2",
                      "createdAt": 1743041352952,
                      "id": "comment2",
                      "issueId": "issue2",
                      "text": "Comment 2 text",
                    },
                    {
                      "authorId": "user2",
                      "createdAt": 1742782152952,
                      "id": "comment5",
                      "issueId": "issue2",
                      "text": "Comment 5 text",
                    },
                  ],
                  "createdAt": 1743041352952,
                  "description": "Description for issue 2",
                  "id": "issue2",
                  "ownerId": "user1",
                  "title": "Test Issue 2",
                },
                {
                  "closed": true,
                  "comments": [
                    {
                      "authorId": "user3",
                      "createdAt": 1742954952952,
                      "id": "comment3",
                      "issueId": "issue3",
                      "text": "Comment 3 text",
                    },
                    {
                      "authorId": "user3",
                      "createdAt": 1742695752952,
                      "id": "comment6",
                      "issueId": "issue3",
                      "text": "Comment 6 text",
                    },
                  ],
                  "createdAt": 1742954952952,
                  "description": "Description for issue 3",
                  "id": "issue3",
                  "ownerId": "user2",
                  "title": "Test Issue 3",
                },
              ]
            `);
    });

    test('junction relationship', async () => {
      const query = issueQuery.related('labels');
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema, query.format),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
                        [
                          {
                            "closed": true,
                            "createdAt": 1743127752952,
                            "description": "Description for issue 1",
                            "id": "issue1",
                            "labels": [
                              {
                                "id": "label1",
                                "name": "Label 1",
                              },
                              {
                                "id": "label2",
                                "name": "Label 2",
                              },
                            ],
                            "ownerId": null,
                            "title": "Test Issue 1",
                          },
                          {
                            "closed": false,
                            "createdAt": 1743041352952,
                            "description": "Description for issue 2",
                            "id": "issue2",
                            "labels": [
                              {
                                "id": "label2",
                                "name": "Label 2",
                              },
                            ],
                            "ownerId": "user1",
                            "title": "Test Issue 2",
                          },
                          {
                            "closed": true,
                            "createdAt": 1742954952952,
                            "description": "Description for issue 3",
                            "id": "issue3",
                            "labels": [
                              {
                                "id": "label1",
                                "name": "Label 1",
                              },
                            ],
                            "ownerId": "user2",
                            "title": "Test Issue 3",
                          },
                        ]
                      `);
    });

    test('nested related with where clauses', async () => {
      const query = issueQuery
        .where('closed', '=', false)
        .related('comments', q =>
          q
            .where('text', 'ILIKE', '%2%')
            .where('createdAt', '=', 1743041352952)
            .related('author'),
        );
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema, query.format),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "closed": false,
            "comments": [
              {
                "author": {
                  "id": "user2",
                  "metadata": {
                    "altContacts": [
                      "alt2@example.com",
                    ],
                    "email": "user2@example.com",
                    "registrar": "google",
                  },
                  "name": "User 2",
                },
                "authorId": "user2",
                "createdAt": 1743041352952,
                "id": "comment2",
                "issueId": "issue2",
                "text": "Comment 2 text",
              },
            ],
            "createdAt": 1743041352952,
            "description": "Description for issue 2",
            "id": "issue2",
            "ownerId": "user1",
            "title": "Test Issue 2",
          },
        ]
      `);
    });

    test('complex query combining multiple features', async () => {
      const query = issueQuery
        .where('closed', '=', false)
        .whereExists('labels', q =>
          q.where('name', 'IN', ['Label 1', 'Label 2']),
        )
        .related('owner')
        .related('comments', q =>
          q.orderBy('createdAt', 'desc').limit(3).related('author'),
        )
        .orderBy('title', 'asc');
      const sqlQuery = formatPgInternalConvert(
        compile(ast(query), schema.tables, serverSchema, query.format),
      );
      const pgResult = extractZqlResult(
        await runPgQuery(sqlQuery.text, sqlQuery.values),
      );
      const zqlResult = mapResultToClientNames(await query, schema, 'issue');
      expect(zqlResult).toEqualPg(pgResult);
      expect(zqlResult).toMatchInlineSnapshot(`
        [
          {
            "closed": false,
            "comments": [
              {
                "author": {
                  "id": "user2",
                  "metadata": {
                    "altContacts": [
                      "alt2@example.com",
                    ],
                    "email": "user2@example.com",
                    "registrar": "google",
                  },
                  "name": "User 2",
                },
                "authorId": "user2",
                "createdAt": 1743041352952,
                "id": "comment2",
                "issueId": "issue2",
                "text": "Comment 2 text",
              },
              {
                "author": {
                  "id": "user2",
                  "metadata": {
                    "altContacts": [
                      "alt2@example.com",
                    ],
                    "email": "user2@example.com",
                    "registrar": "google",
                  },
                  "name": "User 2",
                },
                "authorId": "user2",
                "createdAt": 1742782152952,
                "id": "comment5",
                "issueId": "issue2",
                "text": "Comment 5 text",
              },
            ],
            "createdAt": 1743041352952,
            "description": "Description for issue 2",
            "id": "issue2",
            "owner": {
              "id": "user1",
              "metadata": null,
              "name": "User 1",
            },
            "ownerId": "user1",
            "title": "Test Issue 2",
          },
        ]
      `);
    });
  }
});
