/**
 * Client end-to-end test of the relationship-`count` path through the real
 * query builder API, for both direct relationships:
 *
 *   newQuery(schema, 'issue').related('comments', c => c.count())
 *
 * and junction (many-to-many) relationships:
 *
 *   newQuery(schema, 'issue').related('labels', l => l.count())
 *
 * goes Query -> AST(count:true) -> buildPipeline (Join over Count) -> ArrayView,
 * materializing a bare count number per issue that updates incrementally.
 */
import {expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';

function seed(queryDelegate: QueryDelegate) {
  const issueSource = must(queryDelegate.getSource('issue'));
  const commentSource = must(queryDelegate.getSource('comment'));
  for (const id of ['0001', '0002', '0003']) {
    consume(
      issueSource.push(
        makeSourceChangeAdd({
          id,
          title: `issue ${id}`,
          description: '',
          closed: false,
          ownerId: null,
          createdAt: Number(id),
        }),
      ),
    );
  }
  // issue 0001 -> 2 comments, 0002 -> 1 comment, 0003 -> 0 comments.
  for (const [id, issueId] of [
    ['0001', '0001'],
    ['0002', '0001'],
    ['0003', '0002'],
  ]) {
    consume(
      commentSource.push(
        makeSourceChangeAdd({
          id,
          authorId: '0001',
          issueId,
          text: 't',
          createdAt: Number(id),
        }),
      ),
    );
  }
  return {issueSource, commentSource};
}

const countOf = (view: {data: readonly unknown[]}, i: number) =>
  (view.data[i] as {comments: number}).comments;

test('related(..., c => c.count()) materializes a per-issue count', () => {
  const queryDelegate = new QueryDelegateImpl();
  const {commentSource} = seed(queryDelegate);

  const q = newQuery(schema, 'issue')
    .related('comments', c => c.count())
    .orderBy('id', 'asc');
  const view = queryDelegate.materialize(q);

  // The `comments` relationship materializes as the bare count number.
  expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "closed": false,
        "comments": 2,
        "createdAt": 1,
        "description": "",
        "id": "0001",
        "ownerId": null,
        "title": "issue 0001",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "comments": 1,
        "createdAt": 2,
        "description": "",
        "id": "0002",
        "ownerId": null,
        "title": "issue 0002",
        Symbol(rc): 1,
      },
      {
        "closed": false,
        "comments": 0,
        "createdAt": 3,
        "description": "",
        "id": "0003",
        "ownerId": null,
        "title": "issue 0003",
        Symbol(rc): 1,
      },
    ]
  `);

  expect(countOf(view, 0)).toBe(2);
  expect(countOf(view, 1)).toBe(1);
  expect(countOf(view, 2)).toBe(0);

  // Add a comment to issue 0003 -> its count goes 0 -> 1.
  consume(
    commentSource.push(
      makeSourceChangeAdd({
        id: '0100',
        authorId: '0001',
        issueId: '0003',
        text: 't',
        createdAt: 100,
      }),
    ),
  );
  expect(countOf(view, 2)).toBe(1);

  // Remove a comment from issue 0001 -> 2 -> 1.
  consume(
    commentSource.push(
      makeSourceChangeRemove({
        id: '0001',
        authorId: '0001',
        issueId: '0001',
        text: 't',
        createdAt: 1,
      }),
    ),
  );
  expect(countOf(view, 0)).toBe(1);
});

test('related(..., l => l.count()) counts junction (many-to-many) edges', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // issues 0001..0003
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  // issue 0001 -> 2 labels, 0002 -> 1 label, 0003 -> 0 labels.
  for (const [issueId, labelId] of [
    ['0001', '0001'],
    ['0001', '0002'],
    ['0002', '0001'],
  ]) {
    consume(issueLabelSource.push(makeSourceChangeAdd({issueId, labelId})));
  }

  const q = newQuery(schema, 'issue')
    .related('labels', l => l.count())
    .orderBy('id', 'asc');
  const view = queryDelegate.materialize(q);

  const labelsOf = (i: number) => (view.data[i] as {labels: number}).labels;
  expect(labelsOf(0)).toBe(2);
  expect(labelsOf(1)).toBe(1);
  expect(labelsOf(2)).toBe(0);

  // Add a label edge to issue 0003 -> 0 -> 1.
  consume(
    issueLabelSource.push(
      makeSourceChangeAdd({issueId: '0003', labelId: '0001'}),
    ),
  );
  expect(labelsOf(2)).toBe(1);

  // Remove a label edge from issue 0001 -> 2 -> 1.
  consume(
    issueLabelSource.push(
      makeSourceChangeRemove({issueId: '0001', labelId: '0001'}),
    ),
  );
  expect(labelsOf(0)).toBe(1);
});

test('where on the destination of a junction count filters which edges count', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate);
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  for (const [id, name] of [
    ['0001', 'bug'],
    ['0002', 'bug'],
    ['0003', 'feature'],
  ]) {
    consume(labelSource.push(makeSourceChangeAdd({id, name})));
  }
  // issue 0001 -> {bug, bug, feature}, 0002 -> {feature}, 0003 -> {}.
  for (const [issueId, labelId] of [
    ['0001', '0001'],
    ['0001', '0002'],
    ['0001', '0003'],
    ['0002', '0003'],
  ]) {
    consume(issueLabelSource.push(makeSourceChangeAdd({issueId, labelId})));
  }

  // count only the edges whose label is named 'bug' (applied as an EXISTS on the
  // junction row — the destination is never materialized).
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('labels', l => l.where('name', 'bug').count())
      .orderBy('id', 'asc'),
  );
  const bugCountOf = (i: number) => (view.data[i] as {labels: number}).labels;
  expect(bugCountOf(0)).toBe(2); // bug, bug
  expect(bugCountOf(1)).toBe(0); // only feature
  expect(bugCountOf(2)).toBe(0); // no labels

  // Flip a label feature -> bug: issue 0002's edge now matches.
  consume(
    labelSource.push(
      makeSourceChangeEdit(
        {id: '0003', name: 'bug'},
        {id: '0003', name: 'feature'},
      ),
    ),
  );
  expect(bugCountOf(0)).toBe(3); // bug, bug, bug
  expect(bugCountOf(1)).toBe(1); // now matches
});

test('where on the destination of a junction min/max filters the field', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate);
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  for (const [id, name] of [
    ['0001', 'bug'],
    ['0002', 'feature'],
    ['0003', 'wontfix'],
  ]) {
    consume(labelSource.push(makeSourceChangeAdd({id, name})));
  }
  // issue 0001 -> {bug, feature, wontfix}.
  for (const labelId of ['0001', '0002', '0003']) {
    consume(
      issueLabelSource.push(makeSourceChangeAdd({issueId: '0001', labelId})),
    );
  }

  // max('name') but only over labels whose name is not 'wontfix'. The filter is
  // applied one hop past the junction, before the lift+aggregate.
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('labels', l => l.where('name', '!=', 'wontfix').max('name'))
      .orderBy('id', 'asc'),
  );
  const maxOf = (i: number) => (view.data[i] as {labels: string | null}).labels;
  expect(maxOf(0)).toBe('feature'); // max of {bug, feature} (wontfix excluded)

  // A label leaving the filter (feature -> wontfix) drops it from the aggregate.
  consume(
    labelSource.push(
      makeSourceChangeEdit(
        {id: '0002', name: 'wontfix'},
        {id: '0002', name: 'feature'},
      ),
    ),
  );
  expect(maxOf(0)).toBe('bug'); // only {bug} matches now
});

test('related(..., c => c.sum(field)) materializes a per-issue sum (null when empty)', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // comment createdAt: issue1 -> [1,2], issue2 -> [3], issue3 -> []
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.sum('createdAt'))
      .orderBy('id', 'asc'),
  );
  const sumOf = (i: number) =>
    (view.data[i] as {comments: number | null}).comments;
  expect(sumOf(0)).toBe(3); // 1 + 2
  expect(sumOf(1)).toBe(3); // 3
  expect(sumOf(2)).toBe(null); // empty -> null
});

test('related(..., c => c.avg(field)) materializes a per-issue avg (null when empty)', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate);
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.avg('createdAt'))
      .orderBy('id', 'asc'),
  );
  const avgOf = (i: number) =>
    (view.data[i] as {comments: number | null}).comments;
  expect(avgOf(0)).toBe(1.5); // (1 + 2) / 2
  expect(avgOf(1)).toBe(3);
  expect(avgOf(2)).toBe(null);
});

test('junction (many-to-many) min/max over the destination field', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // issues 0001..0003
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));
  for (const [id, name] of [
    ['0001', 'bug'],
    ['0002', 'feature'],
    ['0003', 'wontfix'],
  ]) {
    consume(labelSource.push(makeSourceChangeAdd({id, name})));
  }
  // issue 0001 -> {bug, feature}, 0002 -> {wontfix}, 0003 -> {}.
  for (const [issueId, labelId] of [
    ['0001', '0001'],
    ['0001', '0002'],
    ['0002', '0003'],
  ]) {
    consume(issueLabelSource.push(makeSourceChangeAdd({issueId, labelId})));
  }

  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('labels', l => l.max('name'))
      .orderBy('id', 'asc'),
  );
  const maxOf = (i: number) => (view.data[i] as {labels: string | null}).labels;
  expect(maxOf(0)).toBe('feature'); // max('bug', 'feature')
  expect(maxOf(1)).toBe('wontfix');
  expect(maxOf(2)).toBe(null); // no labels

  // Edit a destination field through the junction (a CHILD change on the
  // junction row): 'feature' -> 'aaa'. issue 0001 was at the extreme, so this
  // re-fetches the new max ('bug') — exercises LiftField's CHILD path + the
  // Aggregate's non-invertible re-fetch.
  consume(
    labelSource.push(
      makeSourceChangeEdit(
        {id: '0002', name: 'aaa'},
        {id: '0002', name: 'feature'},
      ),
    ),
  );
  expect(maxOf(0)).toBe('bug');

  // Remove an edge (issue 0001 loses 'bug') -> max of {aaa} = 'aaa'.
  consume(
    issueLabelSource.push(
      makeSourceChangeRemove({issueId: '0001', labelId: '0001'}),
    ),
  );
  expect(maxOf(0)).toBe('aaa');
});

test('where on the aggregated relationship filters which rows are counted', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // issue1 comments createdAt [1,2]; issue2 [3]; issue3 []
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.where('createdAt', '>', 1).count())
      .orderBy('id', 'asc'),
  );
  const countOf = (i: number) => (view.data[i] as {comments: number}).comments;
  expect(countOf(0)).toBe(1); // createdAt 1 excluded, 2 included
  expect(countOf(1)).toBe(1); // createdAt 3
  expect(countOf(2)).toBe(0);

  const commentSource = must(queryDelegate.getSource('comment'));
  // a filtered-OUT comment does not change the count
  consume(
    commentSource.push(
      makeSourceChangeAdd({
        id: '0500',
        authorId: '0001',
        issueId: '0001',
        text: 't',
        createdAt: 0,
      }),
    ),
  );
  expect(countOf(0)).toBe(1);
  // a passing comment does
  consume(
    commentSource.push(
      makeSourceChangeAdd({
        id: '0501',
        authorId: '0001',
        issueId: '0001',
        text: 't',
        createdAt: 9,
      }),
    ),
  );
  expect(countOf(0)).toBe(2);
});

test('related(..., c => c.max(field)) materializes a per-issue max, re-fetching on removal', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // createdAt: issue1 [1,2], issue2 [3], issue3 []
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.max('createdAt'))
      .orderBy('id', 'asc'),
  );
  const maxOf = (i: number) =>
    (view.data[i] as {comments: number | null}).comments;
  expect(maxOf(0)).toBe(2);
  expect(maxOf(1)).toBe(3);
  expect(maxOf(2)).toBe(null);

  // remove the current max of issue 1 (comment 0002, createdAt 2) -> re-fetch -> 1
  const commentSource = must(queryDelegate.getSource('comment'));
  consume(
    commentSource.push(
      makeSourceChangeRemove({
        id: '0002',
        authorId: '0001',
        issueId: '0001',
        text: 't',
        createdAt: 2,
      }),
    ),
  );
  expect(maxOf(0)).toBe(1);
});

test('related(..., c => c.min(field)) materializes a per-issue min', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate);
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.min('createdAt'))
      .orderBy('id', 'asc'),
  );
  const minOf = (i: number) =>
    (view.data[i] as {comments: number | null}).comments;
  expect(minOf(0)).toBe(1);
  expect(minOf(1)).toBe(3);
  expect(minOf(2)).toBe(null);
});

test('where on the aggregated relationship filters which rows are summed', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate);
  const view = queryDelegate.materialize(
    newQuery(schema, 'issue')
      .related('comments', c => c.where('createdAt', '>', 1).sum('createdAt'))
      .orderBy('id', 'asc'),
  );
  const sumOf = (i: number) =>
    (view.data[i] as {comments: number | null}).comments;
  expect(sumOf(0)).toBe(2); // only createdAt 2 (1 excluded)
  expect(sumOf(1)).toBe(3);
  expect(sumOf(2)).toBe(null); // nothing passes -> empty -> null
});

test('top-level count() returns a scalar and updates incrementally', () => {
  const queryDelegate = new QueryDelegateImpl();
  const {issueSource} = seed(queryDelegate); // 3 issues
  const view = queryDelegate.materialize(newQuery(schema, 'issue').count());
  expect(view.data).toBe(3);

  consume(
    issueSource.push(
      makeSourceChangeAdd({
        id: '0004',
        title: 'issue 0004',
        description: '',
        closed: false,
        ownerId: null,
        createdAt: 4,
      }),
    ),
  );
  expect(view.data).toBe(4);

  consume(
    issueSource.push(
      makeSourceChangeRemove({
        id: '0001',
        title: 'issue 0001',
        description: '',
        closed: false,
        ownerId: null,
        createdAt: 1,
      }),
    ),
  );
  expect(view.data).toBe(3);
});

test('top-level count() respects where', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // 3 issues, all closed:false
  expect(
    queryDelegate.materialize(
      newQuery(schema, 'issue').where('closed', false).count(),
    ).data,
  ).toBe(3);
  expect(
    queryDelegate.materialize(
      newQuery(schema, 'issue').where('closed', true).count(),
    ).data,
  ).toBe(0);
});

test('top-level sum()/avg() are scalars; null for an empty result', () => {
  const queryDelegate = new QueryDelegateImpl();
  seed(queryDelegate); // issues createdAt 1,2,3
  expect(
    queryDelegate.materialize(newQuery(schema, 'issue').sum('createdAt')).data,
  ).toBe(6);
  expect(
    queryDelegate.materialize(newQuery(schema, 'issue').avg('createdAt')).data,
  ).toBe(2);
  // empty result -> null
  expect(
    queryDelegate.materialize(
      newQuery(schema, 'issue').where('id', 'nope').sum('createdAt'),
    ).data,
  ).toBe(null);
});
