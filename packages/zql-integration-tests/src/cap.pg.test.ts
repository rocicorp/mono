import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeAll, describe, expect, test} from 'vitest';
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

import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
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
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeAdd({
        id: 'c1a',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1a',
        createdAt: 1100000000000,
      }),
    ),
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
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeAdd({
        id: 'c3d',
        authorId: 'user1',
        issue_id: 'issue3',
        text: 'Comment 3d',
        createdAt: 1100000001000,
      }),
    ),
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
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeRemove({
        id: 'c4a',
        authorId: 'user1',
        issue_id: 'issue4',
        text: 'Comment 4a',
        createdAt: 1015027200000,
      }),
    ),
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
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeRemove({
        id: 'c4e',
        authorId: 'user1',
        issue_id: 'issue4',
        text: 'Comment 4e',
        createdAt: 1015372800000,
      }),
    ),
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
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeRemove({
        id: 'c1a',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1a',
        createdAt: 1100000000000,
      }),
    ),
  );

  const data = view.data as ReadonlyArray<{readonly id: string}>;
  expect(data.map(r => r.id)).not.toContain('issue1');

  expect(view.data).toEqual(queryDelegate.materialize(q).data);
});

test('re-add comment → issue reappears', () => {
  const q = makeQuery();
  const view = queryDelegate.materialize(q);

  consume(
    must(queryDelegate.getSource('comments')).push(
      makeSourceChangeAdd({
        id: 'c1b',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'Comment 1b',
        createdAt: 1100000002000,
      }),
    ),
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
    consume(source.push(makeSourceChangeRemove(row)));
    expect(view.data).toEqual(queryDelegate.materialize(q).data);
  }
});

describe('Cap edge cases', () => {
  let pg2: PostgresDB;
  let sqlite2: Database;
  let issueQuery2: Query<'issue', Schema>;
  let qd: QueryDelegate;

  beforeAll(async () => {
    pg2 = await testDBs.create('cap-edge-cases');
    await pg2.unsafe(createTableSQL);
    sqlite2 = new Database(lc, ':memory:');

    await pg2.unsafe(/*sql*/ `
      INSERT INTO "users" ("id", "name") VALUES ('u1', 'User 1');

      -- i1: 5 comments (3 tracked by cap, 2 overflow) — used by gap 2
      INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
        ('i1', 'Issue 1', 'd1', false, 'u1', TIMESTAMPTZ '2001-01-01T00:00:00.000Z');
      INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
        ('c1a', 'u1', 'i1', 'a', TIMESTAMP '2002-01-01 00:00:00'),
        ('c1b', 'u1', 'i1', 'b', TIMESTAMP '2002-01-02 00:00:00'),
        ('c1c', 'u1', 'i1', 'c', TIMESTAMP '2002-01-03 00:00:00'),
        ('c1d', 'u1', 'i1', 'd', TIMESTAMP '2002-01-04 00:00:00'),
        ('c1e', 'u1', 'i1', 'e', TIMESTAMP '2002-01-05 00:00:00');

      -- i2: 1 comment c2a with 2 revisions — used by gap 3 (CHILD)
      INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
        ('i2', 'Issue 2', 'd2', false, 'u1', TIMESTAMPTZ '2001-01-02T00:00:00.000Z');
      INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
        ('c2a', 'u1', 'i2', 'only', TIMESTAMP '2002-02-01 00:00:00');
      INSERT INTO "revision" ("id", "authorId", "commentId", "text") VALUES
        ('r2a1', 'u1', 'c2a', 'rev1'),
        ('r2a2', 'u1', 'c2a', 'rev2');

      -- i3: 1 comment c3a with 1 revision — used by gap 4 (churn both levels)
      INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
        ('i3', 'Issue 3', 'd3', false, 'u1', TIMESTAMPTZ '2001-01-03T00:00:00.000Z');
      INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
        ('c3a', 'u1', 'i3', 'only', TIMESTAMP '2002-03-01 00:00:00');
      INSERT INTO "revision" ("id", "authorId", "commentId", "text") VALUES
        ('r3a1', 'u1', 'c3a', 'rev1');

      -- i4: 1 comment — used by gap 6 (drain-to-zero and refill)
      INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
        ('i4', 'Issue 4', 'd4', false, 'u1', TIMESTAMPTZ '2001-01-04T00:00:00.000Z');
      INSERT INTO "comments" ("id", "authorId", "issue_id", "text", "createdAt") VALUES
        ('c4a', 'u1', 'i4', 'only', TIMESTAMP '2002-04-01 00:00:00');
    `);

    await initialSync(
      new LogContext('debug', {}, consoleLogSink),
      {appID: 'cap_edge', shardNum: 0, publications: []},
      sqlite2,
      getConnectionURI(pg2),
      {tableCopyWorkers: 1},
      {},
    );

    qd = newQueryDelegate(lc, testLogConfig, sqlite2, schema);
    issueQuery2 = newQuery(schema, 'issue');
  });

  // Gap 2: EDIT of an untracked (overflow) row must be dropped by Cap
  // without perturbing the live view. Cap tracks the first 3 comments
  // of i1; c1d is overflow. The drift assertion would flag any mismatch
  // between live state and a fresh materialization.
  test('edit of untracked overflow comment does not disturb view', () => {
    const q = issueQuery2.whereExists('comments').related('comments');
    const view = qd.materialize(q);

    expect(view.data).toEqual(qd.materialize(q).data);

    consume(
      must(qd.getSource('comments')).push(
        makeSourceChangeEdit(
          {
            id: 'c1d',
            authorId: 'u1',
            issue_id: 'i1',
            text: 'd-edited',
            createdAt: 1009843200000 + 3 * 86400000,
          },
          {
            id: 'c1d',
            authorId: 'u1',
            issue_id: 'i1',
            text: 'd',
            createdAt: 1009843200000 + 3 * 86400000,
          },
        ),
      ),
    );

    expect(view.data).toEqual(qd.materialize(q).data);
  });

  // Gap 3: nested whereExists. A revision change that does NOT flip
  // existence of its parent comment travels up the inner Exists as a
  // CHILD change and must be forwarded by the outer Cap for tracked
  // rows (cap.ts:272-277).
  test('nested whereExists — inner revision change forwarded as CHILD', () => {
    const q = issueQuery2
      .whereExists('comments', c => c.whereExists('revisions'))
      .related('comments', c => c.related('revisions'));
    const view = qd.materialize(q);

    expect(view.data).toEqual(qd.materialize(q).data);

    // Add a revision on c2a. c2a still has revisions both before and
    // after, so existence is unchanged — change flows as CHILD.
    consume(
      must(qd.getSource('revision')).push(
        makeSourceChangeAdd({
          id: 'r2a3',
          authorId: 'u1',
          commentId: 'c2a',
          text: 'rev3',
        }),
      ),
    );

    expect(view.data).toEqual(qd.materialize(q).data);

    // Remove a non-last revision of c2a — still existence-unchanged.
    consume(
      must(qd.getSource('revision')).push(
        makeSourceChangeRemove({
          id: 'r2a1',
          authorId: 'u1',
          commentId: 'c2a',
          text: 'rev1',
        }),
      ),
    );

    expect(view.data).toEqual(qd.materialize(q).data);
  });

  // Gap 4: doubly-nested whereExists produces two Cap levels. Exercise
  // churn on the inner level (revisions) that forces existence flips on
  // the middle level (comments) and in turn on the outer (issues),
  // covering the full propagation and the Cap size transitions.
  test('doubly nested whereExists stays consistent under existence flips', () => {
    const q = issueQuery2.whereExists('comments', c =>
      c.whereExists('revisions'),
    );
    const view = qd.materialize(q);

    expect(view.data).toEqual(qd.materialize(q).data);

    // Remove c3a's only revision → c3a loses existence → i3 retracts.
    consume(
      must(qd.getSource('revision')).push(
        makeSourceChangeRemove({
          id: 'r3a1',
          authorId: 'u1',
          commentId: 'c3a',
          text: 'rev1',
        }),
      ),
    );
    expect(view.data).toEqual(qd.materialize(q).data);

    // Re-add a revision → c3a regains existence → i3 reappears.
    // This also exercises the outer Cap transitioning away from size=0
    // for i3's partition.
    consume(
      must(qd.getSource('revision')).push(
        makeSourceChangeAdd({
          id: 'r3a2',
          authorId: 'u1',
          commentId: 'c3a',
          text: 'rev2',
        }),
      ),
    );
    expect(view.data).toEqual(qd.materialize(q).data);
  });

  // Gap 6: capState.size === 0 early-return path. After all comments
  // of i4 are removed, i4's partition in the cap stores {size: 0}.
  // A subsequent ADD must transition from 0 correctly, and the live
  // view must agree with a fresh materialization.
  test('cap transitions through size=0 without drift', () => {
    const q = issueQuery2.whereExists('comments').related('comments');
    const view = qd.materialize(q);

    expect(view.data).toEqual(qd.materialize(q).data);

    consume(
      must(qd.getSource('comments')).push(
        makeSourceChangeRemove({
          id: 'c4a',
          authorId: 'u1',
          issue_id: 'i4',
          text: 'only',
          createdAt: 1017633600000,
        }),
      ),
    );
    expect(view.data).toEqual(qd.materialize(q).data);

    consume(
      must(qd.getSource('comments')).push(
        makeSourceChangeAdd({
          id: 'c4b',
          authorId: 'u1',
          issue_id: 'i4',
          text: 'second',
          createdAt: 1100000000000,
        }),
      ),
    );
    expect(view.data).toEqual(qd.materialize(q).data);
  });
});
