import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {Catch, type CaughtChange, type CaughtNode} from './catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  type Source,
  type SourceChange,
} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

// The parent index of an EXISTS join holds the parents the query emits, not
// every parent the join scans. These tests check what the index holds, and
// that child changes still reach the parents they must reach.

const lc = createSilentLogContext();

const sources = {
  issue: {
    columns: {id: {type: 'string'}, open: {type: 'boolean'}},
    primaryKeys: ['id'],
  },
  comment: {
    columns: {
      id: {type: 'string'},
      issueID: {type: 'string'},
      text: {type: 'string'},
    },
    primaryKeys: ['id'],
  },
  label: {
    columns: {id: {type: 'string'}, issueID: {type: 'string'}},
    primaryKeys: ['id'],
  },
} as const;

type Table = keyof typeof sources;

// Ten issues. Only i3 and i7 have comments; only i3 and i5 have labels. i7 is
// the only closed issue.
const sourceContents: Record<Table, Row[]> = {
  issue: Array.from({length: 10}, (_, i) => ({
    id: `i${i}`,
    open: i !== 7,
  })),
  comment: [
    {id: 'c1', issueID: 'i3', text: 'a'},
    {id: 'c2', issueID: 'i3', text: 'b'},
    {id: 'c3', issueID: 'i7', text: 'c'},
  ],
  label: [
    {id: 'l1', issueID: 'i3'},
    {id: 'l2', issueID: 'i5'},
  ],
};

function exists(
  table: 'comment' | 'label',
  op: 'EXISTS' | 'NOT EXISTS' = 'EXISTS',
  flip = false,
): CorrelatedSubqueryCondition {
  return {
    type: 'correlatedSubquery',
    op,
    flip,
    related: {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table,
        alias: `${table}s`,
        orderBy: [['id', 'asc']],
      },
    },
  };
}

function issues(where: Condition, limit?: number): AST {
  return {table: 'issue', orderBy: [['id', 'asc']], where, limit};
}

/**
 * Hydrates `ast` into a Catch, which walks every relationship of the nodes
 * it receives like zero-cache does, and applies `pushes`.
 */
function run(
  ast: AST,
  pushes: [Table, SourceChange][] = [],
  enableNotExists = false,
) {
  const sourcesByName: Record<string, Source> = {};
  for (const [name, {columns, primaryKeys}] of Object.entries(sources)) {
    const source = createSource(lc, testLogConfig, name, columns, [
      ...primaryKeys,
    ]);
    for (const row of sourceContents[name as Table]) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sourcesByName[name] = source;
  }
  const delegate = new TestBuilderDelegate(
    sourcesByName,
    true,
    enableNotExists,
  );
  const sink = new Catch(buildPipeline(ast, delegate, 'query-id'));
  sink.fetch();
  delegate.clearLog();
  for (const [name, change] of pushes) {
    consume(must(delegate.getSource(name)).push(change));
  }
  return {
    // The parents in each join's index, by join.
    indexed: Object.fromEntries(
      Object.entries(delegate.clonedStorage)
        .filter(
          ([name]) =>
            name.includes(':join(') || name.includes(':flipped-join('),
        )
        .map(([name, entries]) => [
          name,
          Object.keys(entries)
            .map(k => k.slice(k.lastIndexOf('\x00') + 2))
            .sort(),
        ]),
    ),
    // The fetches the pushes caused on the issue source.
    issueFetches: delegate.log.filter(
      ([name, type]) => name === ':source(issue)' && type === 'fetch',
    ).length,
    pushes: sink.pushes.map(summarize),
  };
}

function summarize(change: CaughtChange): string {
  switch (change.type) {
    case 'add':
    case 'remove':
      return `${change.type} ${rowID(change.node)}`;
    case 'edit':
      return `edit ${String(change.row.id)}`;
    case 'child':
      return `child ${String(change.row.id)} ${change.child.relationshipName} ${summarize(change.child.change)}`;
  }
}

function rowID(node: CaughtNode): string {
  return node === 'yield' ? 'yield' : String(node.row.id);
}

describe('hydration', () => {
  test('an unlimited EXISTS indexes its result, not every issue', () => {
    expect(run(issues(exists('comment'))).indexed).toEqual({
      ':join(comments)': ['i3', 'i7'],
    });
  });

  test('a limited EXISTS indexes the window, not the issues scanned', () => {
    expect(run(issues(exists('comment'), 1)).indexed).toEqual({
      ':join(comments)': ['i3'],
    });
  });

  test('parents another condition rejects are not indexed', () => {
    expect(
      run(
        issues({
          type: 'and',
          conditions: [exists('comment'), exists('label')],
        }),
      ).indexed,
    ).toEqual({
      ':join(comments_0)': ['i3'],
      ':join(labels_1)': ['i3'],
    });
  });

  test('every EXISTS join of an OR indexes the whole result', () => {
    expect(
      run(
        issues({
          type: 'or',
          conditions: [exists('comment'), exists('label')],
        }),
      ).indexed,
    ).toEqual({
      ':join(comments_0)': ['i3', 'i5', 'i7'],
      ':join(labels_1)': ['i3', 'i5', 'i7'],
    });
  });

  test('a limited flipped EXISTS indexes the window, not the issues scanned', () => {
    expect(run(issues(exists('comment', 'EXISTS', true), 1)).indexed).toEqual({
      ':flipped-join(comments)': ['i3'],
    });
  });

  test('every flipped EXISTS join of an OR indexes the whole result', () => {
    expect(
      run(
        issues({
          type: 'or',
          conditions: [
            exists('comment', 'EXISTS', true),
            exists('label', 'EXISTS', true),
          ],
        }),
      ).indexed,
    ).toEqual({
      ':flipped-join(comments_0)': ['i3', 'i5', 'i7'],
      ':flipped-join(labels_1)': ['i3', 'i5', 'i7'],
    });
  });
});

describe('EXISTS', () => {
  test('a child add qualifies an issue that is not indexed', () => {
    const {pushes, indexed} = run(issues(exists('comment')), [
      ['comment', makeSourceChangeAdd({id: 'c9', issueID: 'i5', text: 'x'})],
    ]);
    expect(pushes).toEqual(['add i5']);
    expect(indexed).toEqual({':join(comments)': ['i3', 'i5', 'i7']});
  });

  test('removing the last child removes the issue from the index', () => {
    const {pushes, indexed} = run(issues(exists('comment')), [
      ['comment', makeSourceChangeRemove({id: 'c3', issueID: 'i7', text: 'c'})],
    ]);
    expect(pushes).toEqual(['remove i7']);
    expect(indexed).toEqual({':join(comments)': ['i3']});
  });

  test('a child edit reaches an indexed issue', () => {
    const {pushes} = run(issues(exists('comment')), [
      [
        'comment',
        makeSourceChangeEdit(
          {id: 'c3', issueID: 'i7', text: 'c2'},
          {id: 'c3', issueID: 'i7', text: 'c'},
        ),
      ],
    ]);
    expect(pushes).toEqual(['child i7 comments edit c3']);
  });

  test('a limit eviction removes the evicted issue from the index', () => {
    const {pushes, indexed} = run(issues(exists('comment'), 2), [
      ['comment', makeSourceChangeAdd({id: 'c9', issueID: 'i1', text: 'x'})],
    ]);
    expect(pushes).toEqual(['remove i7', 'add i1']);
    expect(indexed).toEqual({':join(comments)': ['i1', 'i3']});
  });

  test('a child edit of an issue another condition rejects fetches nothing', () => {
    // i7 has a comment but is closed, so the query does not emit it.
    const {pushes, issueFetches} = run(
      issues({
        type: 'and',
        conditions: [
          exists('comment'),
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'open'},
            right: {type: 'literal', value: true},
          },
        ],
      }),
      [
        [
          'comment',
          makeSourceChangeEdit(
            {id: 'c3', issueID: 'i7', text: 'c2'},
            {id: 'c3', issueID: 'i7', text: 'c'},
          ),
        ],
      ],
    );
    expect(pushes).toEqual([]);
    expect(issueFetches).toBe(0);
  });

  test('in an OR, a child edit reaches an issue that passed the other branch', () => {
    // i3 passes the comments branch, so the labels branch never evaluates it,
    // but its labels are still part of the result.
    const {pushes} = run(
      issues({
        type: 'or',
        conditions: [exists('comment'), exists('label')],
      }),
      [
        [
          'label',
          makeSourceChangeEdit(
            {id: 'l1', issueID: 'i3'},
            {id: 'l1', issueID: 'i3'},
          ),
        ],
      ],
    );
    expect(pushes).toEqual(['child i3 labels_1 edit l1']);
  });
});

describe('NOT EXISTS', () => {
  const notExists = issues(exists('comment', 'NOT EXISTS'));

  test('indexes the issues without children', () => {
    expect(run(notExists, [], true).indexed).toEqual({
      ':join(comments)': ['i0', 'i1', 'i2', 'i4', 'i5', 'i6', 'i8', 'i9'],
    });
  });

  test('removing the last child qualifies an issue that is not indexed', () => {
    const {pushes, indexed} = run(
      notExists,
      [
        [
          'comment',
          makeSourceChangeRemove({id: 'c3', issueID: 'i7', text: 'c'}),
        ],
      ],
      true,
    );
    expect(pushes).toEqual(['add i7']);
    expect(indexed[':join(comments)']).toContain('i7');
  });

  test('a child add removes an indexed issue', () => {
    const {pushes, indexed} = run(
      notExists,
      [['comment', makeSourceChangeAdd({id: 'c9', issueID: 'i5', text: 'x'})]],
      true,
    );
    expect(pushes).toEqual(['remove i5']);
    expect(indexed[':join(comments)']).not.toContain('i5');
  });

  test('a child add or edit of an issue that has children fetches nothing', () => {
    const {pushes, issueFetches} = run(
      notExists,
      [
        ['comment', makeSourceChangeAdd({id: 'c9', issueID: 'i3', text: 'x'})],
        [
          'comment',
          makeSourceChangeEdit(
            {id: 'c3', issueID: 'i7', text: 'c2'},
            {id: 'c3', issueID: 'i7', text: 'c'},
          ),
        ],
      ],
      true,
    );
    expect(pushes).toEqual([]);
    expect(issueFetches).toBe(0);
  });
});

describe('flipped EXISTS', () => {
  test('a limit eviction removes the evicted issue from the flipped join index', () => {
    const {pushes, indexed} = run(
      issues(exists('comment', 'EXISTS', true), 2),
      [['comment', makeSourceChangeAdd({id: 'c9', issueID: 'i1', text: 'x'})]],
    );
    expect(pushes).toEqual(['remove i7', 'add i1']);
    expect(indexed).toEqual({':flipped-join(comments)': ['i1', 'i3']});
  });

  test('parents beyond the limit window are not added to the flipped join index', () => {
    // i8 exists but has no comment. Adding a comment qualifies i8,
    // but because i3 is already in the limit(1) window and sorts before i8,
    // i8 is rejected by the limit and must not be added to the flipped join index.
    const {pushes, indexed} = run(
      issues(exists('comment', 'EXISTS', true), 1),
      [['comment', makeSourceChangeAdd({id: 'c8', issueID: 'i8', text: 'z'})]],
    );
    expect(pushes).toEqual([]);
    expect(indexed).toEqual({':flipped-join(comments)': ['i3']});
  });

  test('a parent add beyond the limit window is not added to the flipped join index', () => {
    // Parent add where parent already has a matching child.
    // Downstream Take(1) rejects the parent, so it must not enter the index.
    const {pushes, indexed} = run(
      issues(exists('comment', 'EXISTS', true), 1),
      [
        [
          'comment',
          makeSourceChangeAdd({id: 'c99', issueID: 'i99', text: 'z'}),
        ],
        ['issue', makeSourceChangeAdd({id: 'i99', open: true})],
      ],
    );
    expect(pushes).toEqual([]);
    expect(indexed).toEqual({':flipped-join(comments)': ['i3']});
  });

  test('removing the last child removes the issue from the flipped join index', () => {
    const {pushes, indexed} = run(issues(exists('comment', 'EXISTS', true)), [
      ['comment', makeSourceChangeRemove({id: 'c3', issueID: 'i7', text: 'c'})],
    ]);
    expect(pushes).toEqual(['remove i7']);
    expect(indexed).toEqual({':flipped-join(comments)': ['i3']});
  });

  test('a child edit reaches an indexed issue', () => {
    const {pushes} = run(issues(exists('comment', 'EXISTS', true)), [
      [
        'comment',
        makeSourceChangeEdit(
          {id: 'c3', issueID: 'i7', text: 'c2'},
          {id: 'c3', issueID: 'i7', text: 'c'},
        ),
      ],
    ]);
    expect(pushes).toEqual(['child i7 comments edit c3']);
  });

  test('a child edit of an issue another condition rejects fetches nothing', () => {
    // i7 has a comment but is closed, so the query does not emit it.
    const {pushes, issueFetches} = run(
      issues({
        type: 'and',
        conditions: [
          exists('comment', 'EXISTS', true),
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'open'},
            right: {type: 'literal', value: true},
          },
        ],
      }),
      [
        [
          'comment',
          makeSourceChangeEdit(
            {id: 'c3', issueID: 'i7', text: 'c2'},
            {id: 'c3', issueID: 'i7', text: 'c'},
          ),
        ],
      ],
    );
    expect(pushes).toEqual([]);
    expect(issueFetches).toBe(0);
  });

  test('in an OR, a child edit reaches an issue that passed the other branch', () => {
    // i3 passes the comments branch, but both branches' joins must index the
    // emitted row so child edits reach it.
    const {pushes} = run(
      issues({
        type: 'or',
        conditions: [
          exists('comment', 'EXISTS', true),
          exists('label', 'EXISTS', true),
        ],
      }),
      [
        [
          'label',
          makeSourceChangeEdit(
            {id: 'l1', issueID: 'i3'},
            {id: 'l1', issueID: 'i3'},
          ),
        ],
      ],
    );
    expect(pushes).toEqual(['child i3 labels_1 edit l1']);
  });
});
