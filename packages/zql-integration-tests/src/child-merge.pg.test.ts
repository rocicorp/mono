import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeAll, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';
import {consume} from '../../zql/src/ivm/stream.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
let sqlite: Database;
type Schema = typeof schema;
let issueQuery: Query<'issue', Schema>;
let queryDelegate: QueryDelegate;

beforeAll(async () => {
  pg = await testDBs.create('child-merge');
  await pg.unsafe(createTableSQL);
  sqlite = new Database(lc, ':memory:');

  await pg.unsafe(/*sql*/ `
    INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES (
      'issue1', 'Test Issue 1', 'Description for issue 1', false, 'user1', TIMESTAMPTZ '2001-02-16T20:38:40.000Z'
    );

    INSERT INTO "users" ("id", "name") VALUES (
      'user1', 'User 1'
    );

    INSERT INTO "comments" ("id", "authorId", "issue_id", text, "createdAt") VALUES (
      'comment1', 'user1', 'issue1', 'Initial comment', TIMESTAMP '2002-03-16 20:38:40'
    );

    -- Labels that will match both OR branches
    INSERT INTO "label" ("id", "name") VALUES
      ('label1', 'bug'),
      ('label2', 'feature');

    -- Link issue1 to both labels (so it matches both OR branches)
    INSERT INTO "issueLabel" ("issueId", "labelId") VALUES
      ('issue1', 'label1'),
      ('issue1', 'label2');
  `);

  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'child_merge', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  queryDelegate = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  issueQuery = newQuery(schema, 'issue');
});

/**
 * This test exercises the 'child' case in mergeRelationships() in push-accumulated.ts.
 * User report: https://discord.com/channels/830183651022471199/1460597200156622861/1460680005620076708
 *
 * The key insight is that:
 * 1. Child changes must enter via fan-out (not as internal changes) for merging
 * 2. The fan structure must be UnionFanIn (not FanIn) to use mergeRelationships
 *
 * UnionFanIn is used when OR contains exists/subqueries. FanIn is used for simple
 * filter ORs - and FanIn passes `identity` for mergeRelationships, not the real merge.
 *
 * Query structure:
 *   issues
 *     .whereExists('comments', ..., {flip: true})  // Flipped exists BEFORE OR (creates child changes)
 *     .where(({or, exists}) => or(                 // OR with EXISTS (creates UnionFanIn)
 *       exists('labels', l => l.where('name', 'bug'), {flip: true}),
 *       exists('labels', l => l.where('name', 'feature'), {flip: true})
 *     ))
 *
 * When a comment is added:
 * 1. FlippedJoin for 'comments' creates child change for parent issue
 * 2. Child change enters UnionFanOut (for the OR with exists)
 * 3. UnionFanOut broadcasts to both branches
 * 4. Both FlippedJoins for 'labels' preserve the child change
 * 5. UnionFanIn accumulates both child changes
 * 6. mergeRelationships() merges them (exercises the 'child' case)
 *
 * Without the 'child' case in mergeRelationships(), this test would fail with:
 * "mergeRelationships: when types differ, left.type must be edit, got left.type=child"
 */
test('child change merging with flipped exists before OR containing flipped exists', () => {
  // Query with:
  // - flipped exists on 'comments' BEFORE OR (creates child changes from outside)
  // - OR with flipped exists on 'labels' (creates UnionFanIn structure)
  // issue1 has labels 'bug' AND 'feature', so it matches BOTH OR branches
  const q = issueQuery
    .where('id', 'issue1')
    .whereExists('comments', cq => cq.where('authorId', '=', 'user1'), {
      flip: true,
    })
    .where(({or, exists}) =>
      or(
        // Branch 1: issue has label 'bug' (issue1 has this)
        exists('labels', lq => lq.where('name', '=', 'bug'), {flip: true}),
        // Branch 2: issue has label 'feature' (issue1 has this too)
        exists('labels', lq => lq.where('name', '=', 'feature'), {flip: true}),
      ),
    );

  const view = queryDelegate.materialize(q);

  // Verify initial state - issue1 should be present
  expect(mapResultToClientNames(view.data, schema, 'issue')).toMatchObject([
    {
      id: 'issue1',
    },
  ]);

  // Push a new comment - this triggers a child change from the flipped exists on comments
  // The child change enters the UnionFanOut (for the OR) and flows to both branches
  // Both FlippedJoins for 'labels' preserve it (they just flip and forward child changes)
  // Both child changes need to be merged in UnionFanIn
  consume(
    must(queryDelegate.getSource('comments')).push({
      type: 'add',
      row: {
        id: 'comment2',
        authorId: 'user1',
        issue_id: 'issue1',
        text: 'New comment',
        createdAt: 1100000000000,
      },
    }),
  );

  // Verify the view still has the issue
  // The child changes from both branches should have been merged successfully
  expect(mapResultToClientNames(view.data, schema, 'issue')).toMatchObject([
    {
      id: 'issue1',
    },
  ]);
});
