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

  test('nested OR with flipped composes correctly', () => {
    // issue.where(or(
    //   cmp('id', 'i1'),
    //   or(
    //     cmp('id', 'i2'),
    //     exists('comments', {flip: true}),
    //   ),
    // ))
    // Inner OR also has a simple branch + flipped, so shouldSplitRootOr fires
    // recursively. The fix: inner's endPush must run before outer's so the
    // inner UFI's emission lands in the outer UFI's accumulation window.
    const nestedAst: AST = {
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
            type: 'or',
            conditions: [
              {
                type: 'simple',
                left: {type: 'column', name: 'id'},
                op: '=',
                right: {type: 'literal', value: 'i2'},
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
        ],
      },
    };

    // Initial fetch: i1 (cmp), i2 (cmp), i3 (has comment).
    const {data: initial} = runFetchTest({
      sources,
      sourceContents,
      ast: nestedAst,
      format,
    });
    expect(ids(initial)).toEqual(['i1', 'i2', 'i3']);

    // Push: add a comment for i4 — only the deepest nested branch matches.
    // The change must propagate through inner UFI → outer UFI → consumer.
    const {data: afterPush} = runPushTest({
      sources,
      sourceContents,
      ast: nestedAst,
      format,
      pushes: [['comment', makeSourceChangeAdd({id: 'c3', issueID: 'i4'})]],
    });
    expect(ids(afterPush)).toEqual(['i1', 'i2', 'i3', 'i4']);

    // Push: add a NEW issue that matches only the inner-OR's simple branch
    // (id='i2' is taken; use a mid-graph case by removing an issue). This
    // exercises the parent-table push path with two stacked coordinators.
    const {data: afterAdd} = runPushTest({
      sources,
      sourceContents,
      ast: nestedAst,
      format,
      pushes: [
        // Add a comment that pulls i4 in through the deepest branch.
        ['comment', makeSourceChangeAdd({id: 'c3', issueID: 'i4'})],
      ],
    });
    expect(ids(afterAdd)).toEqual(['i1', 'i2', 'i3', 'i4']);
  });

  test('nested OR: EDIT on parent with no key change preserves dedup', () => {
    // EDIT triggers a parent-table push, which fires BOTH coordinators'
    // begin/end. If endPush ordering is wrong, the inner UFI's emission
    // hits the outer UFI's #pushInternalChange path (which asserts
    // ADD/REMOVE/CHILD only) — causing a crash on EDIT.
    const nestedAst: AST = {
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
            type: 'or',
            conditions: [
              {
                type: 'simple',
                left: {type: 'column', name: 'id'},
                op: '=',
                right: {type: 'literal', value: 'i2'},
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
        ],
      },
    };

    // Edit a row that matches multiple branches — i2 matches inner cmp(id=i2)
    // AND has no comment (so doesn't match flipped exists). Edit just `title`.
    const {data} = runPushTest({
      sources,
      sourceContents: {
        ...sourceContents,
        // Make i2 ALSO have a comment so it matches both inner branches.
        comment: [...(sourceContents.comment ?? []), {id: 'c0', issueID: 'i2'}],
      },
      ast: nestedAst,
      format,
      pushes: [
        [
          'issue',
          makeSourceChangeEdit(
            {id: 'i2', title: 'edited'},
            {id: 'i2', title: 'second'},
          ),
        ],
      ],
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
  });

  test('three-way OR: simple + two flipped EXISTS', () => {
    // Two parallel flipped existences against different child tables.
    const sources3: Sources = {
      issue: {
        columns: {id: {type: 'string'}, title: {type: 'string'}},
        primaryKeys: ['id'],
      },
      comment: {
        columns: {id: {type: 'string'}, issueID: {type: 'string'}},
        primaryKeys: ['id'],
      },
      label: {
        columns: {id: {type: 'string'}, issueID: {type: 'string'}},
        primaryKeys: ['id'],
      },
    };
    const sourceContents3: SourceContents = {
      issue: [
        {id: 'i1', title: 'first'},
        {id: 'i2', title: 'second'},
        {id: 'i3', title: 'third'},
        {id: 'i4', title: 'fourth'},
        {id: 'i5', title: 'fifth'},
      ],
      comment: [{id: 'c1', issueID: 'i2'}],
      label: [{id: 'l1', issueID: 'i3'}],
    };
    const ast3: AST = {
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
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              system: 'client',
              correlation: {parentField: ['id'], childField: ['issueID']},
              subquery: {
                table: 'label',
                alias: 'labels',
                orderBy: [['id', 'asc']],
              },
            },
            flip: true,
          },
        ],
      },
    };
    const {data} = runFetchTest({
      sources: sources3,
      sourceContents: sourceContents3,
      ast: ast3,
      format,
    });
    expect(ids(data)).toEqual(['i1', 'i2', 'i3']);
  });

  test('OR with flipped inside ast.related is independent of root OR', () => {
    // The OR-with-flipped optimization should compose with `related` —
    // each related subquery has its own pipeline / coordinator, no
    // cross-contamination.
    const sourcesR: Sources = {
      user: {
        columns: {id: {type: 'string'}},
        primaryKeys: ['id'],
      },
      issue: {
        columns: {
          id: {type: 'string'},
          ownerID: {type: 'string'},
          title: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      comment: {
        columns: {id: {type: 'string'}, issueID: {type: 'string'}},
        primaryKeys: ['id'],
      },
    };
    const sourceContentsR: SourceContents = {
      user: [{id: 'u1'}],
      issue: [
        {id: 'i1', ownerID: 'u1', title: 'a'},
        {id: 'i2', ownerID: 'u1', title: 'b'},
      ],
      comment: [{id: 'c1', issueID: 'i2'}],
    };
    // user.related('issues' = issue.where(or(cmp(title='a'), exists(comments, flip:true))))
    const astR: AST = {
      table: 'user',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['ownerID']},
          subquery: {
            table: 'issue',
            alias: 'issues',
            orderBy: [['id', 'asc']],
            where: {
              type: 'or',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'title'},
                  op: '=',
                  right: {type: 'literal', value: 'a'},
                },
                {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    system: 'client',
                    correlation: {
                      parentField: ['id'],
                      childField: ['issueID'],
                    },
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
          },
        },
      ],
    };
    const formatR: Format = {
      singular: false,
      relationships: {
        issues: {singular: false, relationships: {}},
      },
    };
    const {data} = runFetchTest({
      sources: sourcesR,
      sourceContents: sourceContentsR,
      ast: astR,
      format: formatR,
    });
    const u = (data as unknown as {id: string; issues: {id: string}[]}[])[0];
    expect(u.id).toBe('u1');
    expect(u.issues.map(i => i.id).sort()).toEqual(['i1', 'i2']);
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
