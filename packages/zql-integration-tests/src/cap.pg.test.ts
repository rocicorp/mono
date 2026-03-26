import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeAll, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;
let issueQuery: Query<'issue', Schema>;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  pg = await testDBs.create('cap-integration');
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');

  await pg.unsafe(/*sql*/ `
    INSERT INTO "users" ("id", "name") VALUES
      ('user1', 'User 1');

    INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
      ('issue1', 'Issue 1', 'Desc 1', false, 'user1', TIMESTAMPTZ '2001-01-01T00:00:00.000Z'),
      ('issue2', 'Issue 2', 'Desc 2', false, 'user1', TIMESTAMPTZ '2001-01-02T00:00:00.000Z'),
      ('issue3', 'Issue 3', 'Desc 3', false, 'user1', TIMESTAMPTZ '2001-01-03T00:00:00.000Z'),
      ('issue4', 'Issue 4', 'Desc 4', false, 'user1', TIMESTAMPTZ '2001-01-04T00:00:00.000Z');

    -- issue1: no comments (excluded by EXISTS)
    -- issue2: 1 comment (below cap of 3)
    INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
      ('c2a', 'user1', 'issue2', 'Comment 2a', TIMESTAMP '2002-01-01 00:00:00');

    -- issue3: 3 comments (at cap limit)
    INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
      ('c3a', 'user1', 'issue3', 'Comment 3a', TIMESTAMP '2002-02-01 00:00:00'),
      ('c3b', 'user1', 'issue3', 'Comment 3b', TIMESTAMP '2002-02-02 00:00:00'),
      ('c3c', 'user1', 'issue3', 'Comment 3c', TIMESTAMP '2002-02-03 00:00:00');

    -- issue4: 5 comments (3 tracked + 2 overflow)
    INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
      ('c4a', 'user1', 'issue4', 'Comment 4a', TIMESTAMP '2002-03-01 00:00:00'),
      ('c4b', 'user1', 'issue4', 'Comment 4b', TIMESTAMP '2002-03-02 00:00:00'),
      ('c4c', 'user1', 'issue4', 'Comment 4c', TIMESTAMP '2002-03-03 00:00:00'),
      ('c4d', 'user1', 'issue4', 'Comment 4d', TIMESTAMP '2002-03-04 00:00:00'),
      ('c4e', 'user1', 'issue4', 'Comment 4e', TIMESTAMP '2002-03-05 00:00:00');
  `);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'cap_integration', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
    {},
  );

  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  issueQuery = newQuery(schema, 'issue');
});

function makeQuery() {
  return issueQuery.whereExists('comments').related('comments');
}

test('initial materialization — issue1 excluded, issue2/3/4 included', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);
  const data = view.data as ReadonlyArray<{
    readonly id: string;
    readonly comments: ReadonlyArray<{readonly id: string}>;
  }>;

  // issue1 has no comments → excluded by EXISTS
  const ids = data.map(r => r.id);
  expect(ids).not.toContain('issue1');
  expect(ids).toContain('issue2');
  expect(ids).toContain('issue3');
  expect(ids).toContain('issue4');

  // issue4 has all 5 comments in related (Cap only affects EXISTS child, not related)
  const issue4 = must(data.find(r => r.id === 'issue4'));
  expect(issue4.comments).toHaveLength(5);

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('add comment to commentless issue → issue appears', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'add',
      row: {
        id: 'c1a',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1a',
        createdAt: 1100000000000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).toContain('issue1');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('add beyond cap limit → issue stays, related shows all', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  // issue3 had 3 comments (at cap). Add a 4th.
  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'add',
      row: {
        id: 'c3d',
        authorId: 'user1',
        issue_id: 'issue3',
        text: 'Comment 3d',
        createdAt: 1100000001000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{
    readonly id: string;
    readonly comments: ReadonlyArray<{readonly id: string}>;
  }>;
  expect(data.map(r => r.id)).toContain('issue3');

  // Related shows all 4 comments
  const issue3 = must(data.find(r => r.id === 'issue3'));
  expect(issue3.comments).toHaveLength(4);

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('remove tracked comment with overflow → issue stays', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  // issue4 has 5 comments. Remove c4a (tracked by cap). Cap refills from overflow.
  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'remove',
      row: {
        id: 'c4a',
        authorId: 'user1',
        issue_id: 'issue4',
        text: 'Comment 4a',
        createdAt: 1015027200000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).toContain('issue4');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('remove untracked overflow comment → issue stays', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  // Remove c4e (overflow, not tracked by cap)
  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'remove',
      row: {
        id: 'c4e',
        authorId: 'user1',
        issue_id: 'issue4',
        text: 'Comment 4e',
        createdAt: 1015372800000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).toContain('issue4');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('remove only comment → issue disappears', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  // Remove c1a from issue1 (the only comment, added in test 2)
  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'remove',
      row: {
        id: 'c1a',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1a',
        createdAt: 1100000000000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).not.toContain('issue1');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('re-add comment → issue reappears', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'add',
      row: {
        id: 'c1b',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1b',
        createdAt: 1100000002000,
      },
    }),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).toContain('issue1');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('join-level unordered overlay — remove comment triggers overlay for multiple parent issues', () => {
  // Uses ownerComments: issue.ownerId = comment.authorId
  // All 4 issues have ownerId='user1', all comments have authorId='user1'
  // So a single comment change matches ALL 4 issues as parents.
  // With flip: false, the planner builds a regular Join + Cap(limit=3, unordered).
  // When Cap pushes a remove+refill to Join, Join iterates all 4 parent issues,
  // and for issues 2-4, generateWithOverlayUnordered (join-utils.ts) is called.
  const q = issueQuery.whereExists('ownerComments', {flip: false});
  const view = queryDelegate.materialize(q);

  // All 4 issues should be present (all have ownerComments via ownerId='user1')
  const initialData = view.data as ReadonlyArray<{readonly id: string}>;
  const initialIds = initialData.map(r => r.id);
  expect(initialIds).toContain('issue1');
  expect(initialIds).toContain('issue2');
  expect(initialIds).toContain('issue3');
  expect(initialIds).toContain('issue4');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);

  // Remove comments to ensure we hit a tracked one.
  // Cap tracks the first 3 it encounters (unordered). Removing multiple
  // guarantees at least one hits a tracked comment, triggering Cap refill → Join overlay.
  // After prior tests, the remaining comments are:
  // c1b (issue1), c2a (issue2), c3a/c3b/c3c/c3d (issue3), c4b/c4c/c4d (issue4)
  const commentsToRemove = [
    {
      id: 'c2a',
      authorId: 'user1',
      issue_id: 'issue2',
      text: 'Comment 2a',
      createdAt: 1009843200000,
    },
    {
      id: 'c3a',
      authorId: 'user1',
      issue_id: 'issue3',
      text: 'Comment 3a',
      createdAt: 1012521600000,
    },
    {
      id: 'c3b',
      authorId: 'user1',
      issue_id: 'issue3',
      text: 'Comment 3b',
      createdAt: 1012608000000,
    },
    {
      id: 'c4b',
      authorId: 'user1',
      issue_id: 'issue4',
      text: 'Comment 4b',
      createdAt: 1015113600000,
    },
  ];

  const source = must(queryDelegate.getSource('comments'));
  for (const row of commentsToRemove) {
    consume(source.push({type: 'remove', row}));
    expect(view.data).toEqual(queryDelegate.materialize(q).data);
  }
});
