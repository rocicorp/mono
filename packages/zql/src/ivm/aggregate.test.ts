import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AggregateFunction} from '../../../zero-protocol/src/ast.ts';
import {Aggregate} from './aggregate.ts';
import {Catch} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

function setup(fn: AggregateFunction, field?: string) {
  const comments = createSource(
    lc,
    testLogConfig,
    'comment',
    {
      id: {type: 'number'},
      issueID: {type: 'number'},
      points: {type: 'number', optional: true},
    },
    ['id'],
  );
  // issue 1 -> points 10, 20 ; issue 2 -> points 5 ; issue 3 -> none
  consume(comments.push(makeSourceChangeAdd({id: 1, issueID: 1, points: 10})));
  consume(comments.push(makeSourceChangeAdd({id: 2, issueID: 1, points: 20})));
  consume(comments.push(makeSourceChangeAdd({id: 3, issueID: 2, points: 5})));
  const agg = new Aggregate(
    comments.connect([['id', 'asc']]),
    new MemoryStorage(),
    ['issueID'],
    fn,
    field,
  );
  const out = new Catch(agg);
  return {comments, out};
}

const row = (issueID: number, value: number | null) => ({
  row: {issueID, value},
  relationships: {},
});

test('count(*) per group; empty group is 0', () => {
  const {out} = setup('count');
  expect(out.fetch({constraint: {issueID: 1}})).toEqual([row(1, 2)]);
  expect(out.fetch({constraint: {issueID: 2}})).toEqual([row(2, 1)]);
  expect(out.fetch({constraint: {issueID: 3}})).toEqual([row(3, 0)]);
});

test('sum(field) per group; empty group is null', () => {
  const {out} = setup('sum', 'points');
  expect(out.fetch({constraint: {issueID: 1}})).toEqual([row(1, 30)]);
  expect(out.fetch({constraint: {issueID: 2}})).toEqual([row(2, 5)]);
  expect(out.fetch({constraint: {issueID: 3}})).toEqual([row(3, null)]);
});

test('avg(field) per group; empty group is null', () => {
  const {out} = setup('avg', 'points');
  // avg rows carry their sum + non-null count components (for optimistic deltas
  // on the synced client); `value` is the average.
  const avgRow = (
    issueID: number,
    value: number | null,
    sum: number,
    count: number,
  ) => ({row: {issueID, value, sum, count}, relationships: {}});
  expect(out.fetch({constraint: {issueID: 1}})).toEqual([avgRow(1, 15, 30, 2)]);
  expect(out.fetch({constraint: {issueID: 2}})).toEqual([avgRow(2, 5, 5, 1)]);
  expect(out.fetch({constraint: {issueID: 3}})).toEqual([
    avgRow(3, null, 0, 0),
  ]);
});

test('sum updates on add; an edit changes the sum (but never the count)', () => {
  const {comments, out} = setup('sum', 'points');
  out.fetch({constraint: {issueID: 1}}); // materialize: sum 30

  consume(comments.push(makeSourceChangeAdd({id: 4, issueID: 1, points: 5})));
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 30},
      row: {issueID: 1, value: 35},
    },
  ]);

  out.reset();
  // edit a contributing row's field 10 -> 100
  consume(
    comments.push(
      makeSourceChangeEdit(
        {id: 1, issueID: 1, points: 100},
        {id: 1, issueID: 1, points: 10},
      ),
    ),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 35},
      row: {issueID: 1, value: 125},
    },
  ]);
});

test('sum ignores null fields (no spurious edit)', () => {
  const {comments, out} = setup('sum', 'points');
  out.fetch({constraint: {issueID: 1}});
  consume(
    comments.push(makeSourceChangeAdd({id: 5, issueID: 1, points: null})),
  );
  expect(out.pushes).toEqual([]);
});

test('removing the last contributing row makes sum null again', () => {
  const {comments, out} = setup('sum', 'points');
  out.fetch({constraint: {issueID: 2}}); // sum 5

  consume(
    comments.push(makeSourceChangeRemove({id: 3, issueID: 2, points: 5})),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 2, value: 5},
      row: {issueID: 2, value: null},
    },
  ]);
});

test('count is unaffected by edits', () => {
  const {comments, out} = setup('count');
  out.fetch({constraint: {issueID: 1}});
  consume(
    comments.push(
      makeSourceChangeEdit(
        {id: 1, issueID: 1, points: 999},
        {id: 1, issueID: 1, points: 10},
      ),
    ),
  );
  expect(out.pushes).toEqual([]);
});

test('changes to an unmaterialized group are a no-op', () => {
  const {comments, out} = setup('sum', 'points');
  consume(comments.push(makeSourceChangeAdd({id: 6, issueID: 2, points: 9})));
  expect(out.pushes).toEqual([]);
  // Once fetched, computes the true current sum (source already applied it): 5 + 9.
  expect(out.fetch({constraint: {issueID: 2}})).toEqual([row(2, 14)]);
});

test('min/max(field) per group; empty group is null', () => {
  const min = setup('min', 'points');
  expect(min.out.fetch({constraint: {issueID: 1}})).toEqual([row(1, 10)]);
  expect(min.out.fetch({constraint: {issueID: 3}})).toEqual([row(3, null)]);

  const max = setup('max', 'points');
  expect(max.out.fetch({constraint: {issueID: 1}})).toEqual([row(1, 20)]);
  expect(max.out.fetch({constraint: {issueID: 3}})).toEqual([row(3, null)]);
});

test('max: add extends the extreme outward (O(1))', () => {
  const {comments, out} = setup('max', 'points');
  out.fetch({constraint: {issueID: 1}}); // max 20
  consume(comments.push(makeSourceChangeAdd({id: 4, issueID: 1, points: 30})));
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 20},
      row: {issueID: 1, value: 30},
    },
  ]);
});

test('max: removing a non-extreme row leaves the max unchanged (no re-fetch)', () => {
  const {comments, out} = setup('max', 'points');
  out.fetch({constraint: {issueID: 1}}); // max 20
  consume(
    comments.push(makeSourceChangeRemove({id: 1, issueID: 1, points: 10})),
  );
  expect(out.pushes).toEqual([]); // 20 still present
});

test('max: removing the current extreme re-fetches the new one', () => {
  const {comments, out} = setup('max', 'points');
  out.fetch({constraint: {issueID: 1}}); // max 20
  consume(
    comments.push(makeSourceChangeRemove({id: 2, issueID: 1, points: 20})),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 20},
      row: {issueID: 1, value: 10},
    },
  ]);
});

test('min: removing the current extreme re-fetches the new one', () => {
  const {comments, out} = setup('min', 'points');
  out.fetch({constraint: {issueID: 1}}); // min 10
  consume(
    comments.push(makeSourceChangeRemove({id: 1, issueID: 1, points: 10})),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 10},
      row: {issueID: 1, value: 20},
    },
  ]);
});

test('max: removing the last row makes the value null', () => {
  const {comments, out} = setup('max', 'points');
  out.fetch({constraint: {issueID: 2}}); // max 5 (single row)
  consume(
    comments.push(makeSourceChangeRemove({id: 3, issueID: 2, points: 5})),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 2, value: 5},
      row: {issueID: 2, value: null},
    },
  ]);
});

test('max: editing the extreme down re-fetches; editing it up is O(1)', () => {
  const {comments, out} = setup('max', 'points');
  out.fetch({constraint: {issueID: 1}}); // max 20 (rows 10, 20)

  // edit the max row 20 -> 12: new max is the other row (10)? no, 12 > 10 -> 12
  consume(
    comments.push(
      makeSourceChangeEdit(
        {id: 2, issueID: 1, points: 12},
        {id: 2, issueID: 1, points: 20},
      ),
    ),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 20},
      row: {issueID: 1, value: 12},
    },
  ]);

  out.reset();
  // edit the non-max row 10 -> 99: new max is 99
  consume(
    comments.push(
      makeSourceChangeEdit(
        {id: 1, issueID: 1, points: 99},
        {id: 1, issueID: 1, points: 10},
      ),
    ),
  );
  expect(out.pushes).toEqual([
    {
      type: 'edit',
      oldRow: {issueID: 1, value: 12},
      row: {issueID: 1, value: 99},
    },
  ]);
});

test('ungrouped (top-level) aggregate over all rows; no constraint', () => {
  // all comments: points 10, 20 (issue 1) and 5 (issue 2)
  const mk = (fn: AggregateFunction, field?: string) => {
    const comments = createSource(
      lc,
      testLogConfig,
      'comment',
      {
        id: {type: 'number'},
        issueID: {type: 'number'},
        points: {type: 'number', optional: true},
      },
      ['id'],
    );
    consume(
      comments.push(makeSourceChangeAdd({id: 1, issueID: 1, points: 10})),
    );
    consume(
      comments.push(makeSourceChangeAdd({id: 2, issueID: 1, points: 20})),
    );
    consume(comments.push(makeSourceChangeAdd({id: 3, issueID: 2, points: 5})));
    return {
      comments,
      out: new Catch(
        new Aggregate(
          comments.connect([['id', 'asc']]),
          new MemoryStorage(),
          [], // ungrouped
          fn,
          field,
        ),
      ),
    };
  };

  // fetch with no constraint -> a single global row. The ungrouped row carries
  // the synthetic constant key column (AGGREGATE_KEY_COLUMN === '') so it has a
  // non-empty primary key for the synced path; the value is in `value`.
  expect(mk('count').out.fetch({})).toEqual([
    {row: {'': 0, 'value': 3}, relationships: {}},
  ]);
  expect(mk('sum', 'points').out.fetch({})).toEqual([
    {row: {'': 0, 'value': 35}, relationships: {}},
  ]);
  expect(mk('min', 'points').out.fetch({})).toEqual([
    {row: {'': 0, 'value': 5}, relationships: {}},
  ]);
  expect(mk('max', 'points').out.fetch({})).toEqual([
    {row: {'': 0, 'value': 20}, relationships: {}},
  ]);

  // incremental: add a row -> the single value updates
  const {comments, out} = mk('count');
  out.fetch({});
  consume(comments.push(makeSourceChangeAdd({id: 4, issueID: 9, points: 1})));
  expect(out.pushes).toEqual([
    {type: 'edit', oldRow: {'': 0, 'value': 3}, row: {'': 0, 'value': 4}},
  ]);
});
