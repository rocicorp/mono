import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {beforeAll, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {initialSync} from '../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import type {Query} from '../../zql/src/query/query.ts';
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {Database} from '../../zqlite/src/db.ts';
import {
  mapResultToClientNames,
  newQueryDelegate,
} from '../../zqlite/src/test/source-factory.ts';

const lc = createSilentLogContext();

let pg: PostgresDB;
type Schema = typeof schema;

/**
 * Reproduction of an incremental-maintenance bug in the zql IVM engine for a
 * query whose `where` is an OR that contains BOTH a flipped EXISTS and a
 * NON-flipped EXISTS.
 *
 * Such an OR lowers through `UnionFanOut`/`UnionFanIn` (the "union fan") because
 * it contains a flipped subquery. The non-flipped EXISTS then sits as a plain
 * `Exists` gate UNDER the union fan. An incremental push of a change to that
 * non-flipped relationship is silently dropped, so the maintained `ArrayView`
 * diverges from a fresh query of the same post-push source state — i.e. it
 * violates the core IVM invariant:
 *
 *     view-after-push  ==  fresh-query-of-the-post-push-source-state
 *
 * Query (issue WHERE x=1 OR EXISTS_flipped(comments) OR EXISTS(labels)):
 *
 *   issueQuery.where(({or, cmp, exists}) =>
 *     or(
 *       cmp('ownerId', '=', 'special'),         // the `x = 1` stand-in
 *       exists('comments', c => c, {flip: true}),  // flipped EXISTS  -> union fan
 *       exists('labels',   l => l, {flip: false}), // NON-flipped EXISTS under fan
 *     ),
 *   )
 *
 * Why this is latent in production: the planner always *flips* an EXISTS under
 * an OR (that is the whole point of the flip), so a non-flipped EXISTS never
 * normally reaches the union fan. We force the buggy shape by passing an
 * explicit `{flip: false}` to the `labels` EXISTS via the query-builder API.
 *
 * Note: the same query WITHOUT the flipped `comments` EXISTS lowers through the
 * (correct) filter fan, so the C1 control below — pushing to the *flipped*
 * relationship — is maintained correctly. That isolates the bug to the
 * non-flipped-EXISTS-under-the-union-fan path.
 */

beforeAll(async () => {
  pg = await testDBs.create('union-fan-nonflipped-exists');
  await pg.unsafe(createTableSQL);

  await pg.unsafe(/*sql*/ `
    INSERT INTO "users" ("id", "name") VALUES ('user1', 'User 1');

    INSERT INTO "label" ("id", "name") VALUES ('label1', 'bug');

    -- issue1 matches the simple branch (ownerId = 'special'); no comment, no label.
    -- issue3 matches the NON-flipped EXISTS(labels) branch (has label1).
    -- issue4 is a bare issue: matches nothing initially.
    INSERT INTO "issues" ("id", "title", "description", "closed", "owner_id", "createdAt") VALUES
      ('issue1', 'Issue 1', 'd', false, 'special', TIMESTAMPTZ '2001-02-16T20:38:40.000Z'),
      ('issue3', 'Issue 3', 'd', false, 'other',   TIMESTAMPTZ '2001-02-16T20:38:40.000Z'),
      ('issue4', 'Issue 4', 'd', false, 'other',   TIMESTAMPTZ '2001-02-16T20:38:40.000Z');

    INSERT INTO "issueLabel" ("issueId", "labelId") VALUES ('issue3', 'label1');
  `);
});

/**
 * Builds a fresh, independent SQLite replica + QueryDelegate from the shared pg
 * seed so that each scenario's pushes don't leak into other scenarios.
 */
async function setup(): Promise<QueryDelegate> {
  const sqlite = new Database(lc, ':memory:');
  await initialSync(
    new LogContext('debug', {}, consoleLogSink),
    {appID: 'union_fan_nonflipped', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
    {},
  );
  return newQueryDelegate(lc, testLogConfig, sqlite, schema);
}

function makeQuery(delegate: QueryDelegate): Query<'issue', Schema> {
  // Force the buggy lowering: comments EXISTS is flipped (triggers the union
  // fan), labels EXISTS is explicitly NOT flipped (lands under the fan).
  return newQuery(schema, 'issue').where(({or, cmp, exists}) =>
    or(
      cmp('ownerId', '=', 'special'),
      exists('comments', c => c, {flip: true}),
      exists('labels', l => l, {flip: false}),
    ),
  ) as unknown as Query<'issue', Schema>;
}

function ids(delegate: QueryDelegate, data: unknown): string[] {
  return (mapResultToClientNames(data, schema, 'issue') as {id: string}[])
    .map(r => r.id)
    .sort();
}

test('U1: add a label to bare issue4 -> issue4 should appear', async () => {
  const delegate = await setup();
  const q = makeQuery(delegate);
  const view = delegate.materialize(q);

  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3']);

  // Add a label link to the bare issue4. It now matches the (non-flipped)
  // EXISTS(labels) branch, so it must enter the view.
  consume(
    must(delegate.getSource('issueLabel')).push(
      makeSourceChangeAdd({issueId: 'issue4', labelId: 'label1'}),
    ),
  );

  const fresh = delegate.materialize(q);
  // IVM invariant: maintained view == fresh query of post-push state.
  expect(view.data).toEqual(fresh.data);
  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3', 'issue4']);
});

test('U2: add issue5 that already has a label -> issue5 should appear', async () => {
  const delegate = await setup();
  const q = makeQuery(delegate);
  const view = delegate.materialize(q);

  // Pre-existing label link for an issue that does not exist yet.
  consume(
    must(delegate.getSource('issueLabel')).push(
      makeSourceChangeAdd({issueId: 'issue5', labelId: 'label1'}),
    ),
  );
  // Now add the issue itself; it matches EXISTS(labels) immediately.
  consume(
    must(delegate.getSource('issues')).push(
      makeSourceChangeAdd({
        id: 'issue5',
        title: 'Issue 5',
        description: 'd',
        closed: false,
        owner_id: 'other',
        createdAt: 982355920000,
      }),
    ),
  );

  const fresh = delegate.materialize(q);
  expect(view.data).toEqual(fresh.data);
  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3', 'issue5']);
});

test("U3: remove issue3's only label -> issue3 should leave", async () => {
  const delegate = await setup();
  const q = makeQuery(delegate);
  const view = delegate.materialize(q);

  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3']);

  consume(
    must(delegate.getSource('issueLabel')).push(
      makeSourceChangeRemove({issueId: 'issue3', labelId: 'label1'}),
    ),
  );

  const fresh = delegate.materialize(q);
  expect(view.data).toEqual(fresh.data);
  expect(ids(delegate, view.data)).toEqual(['issue1']);
});

test('U4: edit issue1 out of the simple branch -> issue1 should leave', async () => {
  const delegate = await setup();
  const q = makeQuery(delegate);
  const view = delegate.materialize(q);

  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3']);

  // issue1 only matched via ownerId='special' and has no comment/label.
  // Editing the owner away should drop it from the view.
  consume(
    must(delegate.getSource('issues')).push(
      makeSourceChangeEdit(
        {
          id: 'issue1',
          title: 'Issue 1',
          description: 'd',
          closed: false,
          owner_id: 'other',
          createdAt: 982355920000,
        },
        {
          id: 'issue1',
          title: 'Issue 1',
          description: 'd',
          closed: false,
          owner_id: 'special',
          createdAt: 982355920000,
        },
      ),
    ),
  );

  const fresh = delegate.materialize(q);
  expect(view.data).toEqual(fresh.data);
  expect(ids(delegate, view.data)).toEqual(['issue3']);
});

test('C1 (control): add a comment (the FLIPPED EXISTS) -> maintained correctly', async () => {
  const delegate = await setup();
  const q = makeQuery(delegate);
  const view = delegate.materialize(q);

  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3']);

  // Pushing to the flipped relationship (comments) is the control: this path
  // is maintained correctly, so the invariant holds here.
  consume(
    must(delegate.getSource('comments')).push(
      makeSourceChangeAdd({
        id: 'comment-new',
        authorId: 'user1',
        issue_id: 'issue4',
        text: 'New comment',
        createdAt: 1100000000000,
      }),
    ),
  );

  const fresh = delegate.materialize(q);
  expect(view.data).toEqual(fresh.data);
  expect(ids(delegate, view.data)).toEqual(['issue1', 'issue3', 'issue4']);
});
