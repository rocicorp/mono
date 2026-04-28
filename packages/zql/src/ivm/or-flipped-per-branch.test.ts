import {describe, expect, test} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
import {
  runFetchTest,
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import type {Format} from './view.ts';

/**
 * Tests for OR conditions where one branch is a simple comparison and another
 * is an EXISTS with `flip: true`. The builder splits the OR into per-branch
 * `source.connect`s so each branch can push its filter to the source instead
 * of relying on the OR-wide pushdown (which strips when any branch contains
 * a subquery).
 *
 * The tests focus on the parent-row identity (which issues survive the OR)
 * and on dedup: a row matching both branches must appear exactly once.
 */
describe('OR with simple branch + flipped EXISTS', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        title: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  const sourceContents: SourceContents = {
    issue: [
      {id: 'i1', title: 'first'},
      {id: 'i2', title: 'second'},
      {id: 'i3', title: 'third'},
      {id: 'i4', title: 'fourth'},
    ],
    comment: [
      {id: 'c1', issueID: 'i2'},
      {id: 'c2', issueID: 'i3'},
    ],
  };

  // issue.where(or(cmp('id', 'i1'), exists('comments', { flip: true })))
  // Expected matches: i1 (cmp), i2 (has comment), i3 (has comment).
  // i4 should NOT appear (no cmp match, no comments).
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 'i1'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['issueID']},
            subquery: {
              table: 'comment',
              alias: 'comments',
              orderBy: [['id', 'asc']],
            },
          },
          flip: true,
        },
      ],
    },
  };

  // Format omits the `comments` relationship: this test focuses on which
  // parent rows survive the OR, not on the (partial) child enrichment that's
  // an inherent property of the simple branch's source connection not having
  // a flipped-join relationship.
  const format: Format = {
    singular: false,
    relationships: {},
  };

  function ids(data: unknown): string[] {
    return (data as {id: string}[]).map(r => r.id).sort();
  }

  test('initial fetch returns the union of both branches', () => {
    const {data} = runFetchTest({sources, sourceContents, ast, format});
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
  });

  test('add of a comment makes a previously-non-matching issue match', () => {
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['comment', makeSourceChangeAdd({id: 'c3', issueID: 'i4'})]],
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3', 'i4']);
  });

  test('remove of a comment removes the issue when no other branch matches', () => {
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['comment', makeSourceChangeRemove({id: 'c2', issueID: 'i3'})]],
    });
    expect(ids(data)).toEqual(['i1', 'i2']);
  });

  test('add of a row matching neither branch does not surface', () => {
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['issue', makeSourceChangeAdd({id: 'i5', title: 'fifth'})]],
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
  });

  test('row matching both branches is emitted exactly once (UFI dedup)', () => {
    // i1 matches via cmp. Add a comment for i1 so it ALSO matches the flipped
    // exists branch. Without UFI dedup we would see two i1 entries; with dedup
    // we should see exactly one.
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['comment', makeSourceChangeAdd({id: 'c4', issueID: 'i1'})]],
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
    const i1s = (data as unknown as {id: string}[]).filter(r => r.id === 'i1');
    expect(i1s).toHaveLength(1);
  });

  test('EDIT on a parent row preserves a single emission', () => {
    // EDIT issue i2 (which has comment c1 — branch B match).
    // i2 still matches via the flipped exists branch.
    const {data} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'issue',
          makeSourceChangeEdit(
            {id: 'i2', title: 'SECOND-edited'},
            {id: 'i2', title: 'second'},
          ),
        ],
      ],
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
    const i2 = (data as unknown as {id: string; title: string}[]).find(
      r => r.id === 'i2',
    );
    expect(i2?.title).toBe('SECOND-edited');
  });
});
