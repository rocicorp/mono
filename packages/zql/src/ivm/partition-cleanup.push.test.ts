import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {Catch} from './catch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
  type Source,
} from './source.ts';
import {consume} from './stream.ts';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import {createSource} from './test/source-factory.ts';
import type {Format} from './view.ts';

/**
 * Regression tests for per-partition operator state cleanup.
 *
 * Take and Cap create one state entry per distinct parent join-key value
 * ever fetched. These tests verify that the state is deleted when the
 * partition's parent key is removed from the join's parent input, and that
 * operator storage is cleared when a pipeline is destroyed.
 */

const issueCommentSources: Sources = {
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

const issueCommentContents: SourceContents = {
  issue: [
    {id: 'i1', title: 'issue 1'},
    {id: 'i2', title: 'issue 2'},
    {id: 'i3', title: 'issue 3'},
  ],
  comment: [
    {id: 'c1', issueID: 'i1'},
    {id: 'c2', issueID: 'i1'},
    {id: 'c3', issueID: 'i2'},
  ],
};

const issueCommentAST: AST = {
  table: 'issue',
  orderBy: [['id', 'asc']],
  related: [
    {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comment',
        alias: 'comments',
        orderBy: [['id', 'asc']],
        limit: 2,
      },
    },
  ],
} as const;

const issueCommentFormat: Format = {
  singular: false,
  relationships: {
    comments: {
      singular: false,
      relationships: {},
    },
  },
};

describe('take partition cleanup (related with limit)', () => {
  test('parent remove deletes its partition state', () => {
    const {data, actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents: issueCommentContents,
      ast: issueCommentAST,
      format: issueCommentFormat,
      pushes: [['issue', makeSourceChangeRemove({id: 'i1', title: 'issue 1'})]],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c3",
              "issueID": "i2",
              Symbol(rc): 1,
            },
          ],
          "id": "i2",
          "title": "issue 2",
          Symbol(rc): 1,
        },
        {
          "comments": [],
          "id": "i3",
          "title": "issue 3",
          Symbol(rc): 1,
        },
      ]
    `);

    // The '["take","i1"]' entry is deleted. maxBound is a monotonic
    // high-water mark and is retained.
    expect(actualStorage['.comments:take']).toMatchInlineSnapshot(`
      {
        "["take","i2"]": {
          "bound": {
            "id": "c3",
            "issueID": "i2",
          },
          "size": 1,
        },
        "["take","i3"]": {
          "bound": undefined,
          "size": 0,
        },
        "maxBound": {
          "id": "c3",
          "issueID": "i2",
        },
      }
    `);
  });

  test('parent remove deletes its empty partition state', () => {
    // i3 has no comments; its takeState is {size: 0, bound: undefined} and
    // must also be deleted when i3 is removed.
    const {actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents: issueCommentContents,
      ast: issueCommentAST,
      format: issueCommentFormat,
      pushes: [['issue', makeSourceChangeRemove({id: 'i3', title: 'issue 3'})]],
    });

    expect(Object.keys(actualStorage['.comments:take'])).toMatchInlineSnapshot(`
      [
        "["take","i1"]",
        "["take","i2"]",
        "maxBound",
      ]
    `);
  });

  test('partition state is kept while another parent shares the join key', () => {
    // Reversed direction: comment.related('issue'). The join key
    // (comment.issueID -> issue.id) is not unique among parents, so the
    // issue partition may only be cleaned up when the last comment
    // referencing it is removed.
    const sources: Sources = issueCommentSources;
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', title: 'issue 1'}],
      comment: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i1'},
      ],
    };
    const ast: AST = {
      table: 'comment',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['issueID'], childField: ['id']},
          subquery: {
            table: 'issue',
            alias: 'issue',
            orderBy: [['id', 'asc']],
            limit: 1,
          },
        },
      ],
    } as const;
    const format: Format = {
      singular: false,
      relationships: {
        issue: {
          singular: false,
          relationships: {},
        },
      },
    };

    // Removing c1 keeps the partition alive: c2 still references i1.
    const afterFirstRemove = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['comment', makeSourceChangeRemove({id: 'c1', issueID: 'i1'})]],
    });
    expect(Object.keys(afterFirstRemove.actualStorage['.issue:take']))
      .toMatchInlineSnapshot(`
      [
        "["take","i1"]",
        "maxBound",
      ]
    `);

    // Removing both comments deletes the partition.
    const afterBothRemoved = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        ['comment', makeSourceChangeRemove({id: 'c1', issueID: 'i1'})],
        ['comment', makeSourceChangeRemove({id: 'c2', issueID: 'i1'})],
      ],
    });
    expect(Object.keys(afterBothRemoved.actualStorage['.issue:take']))
      .toMatchInlineSnapshot(`
      [
        "maxBound",
      ]
    `);
  });

  test('child pushes for a cleaned up partition are dropped', () => {
    const {actualStorage, pushes} = runPushTest({
      sources: issueCommentSources,
      sourceContents: issueCommentContents,
      ast: issueCommentAST,
      format: issueCommentFormat,
      pushes: [
        ['issue', makeSourceChangeRemove({id: 'i1', title: 'issue 1'})],
        // i1's partition state is gone; this add must be dropped without
        // resurrecting state.
        ['comment', makeSourceChangeAdd({id: 'c0', issueID: 'i1'})],
      ],
    });

    expect(Object.keys(actualStorage['.comments:take'])).toMatchInlineSnapshot(`
      [
        "["take","i2"]",
        "["take","i3"]",
        "maxBound",
      ]
    `);
    // Only the remove of i1 reaches the output; the comment add is dropped.
    expect(pushes).toHaveLength(1);
    expect(pushes[0].type).toBe('remove');
  });

  test('parent re-add after cleanup hydrates fresh partition state', () => {
    const {data, actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents: issueCommentContents,
      ast: issueCommentAST,
      format: issueCommentFormat,
      pushes: [
        ['issue', makeSourceChangeRemove({id: 'i1', title: 'issue 1'})],
        // Dropped: i1's partition has no state while i1 is absent.
        ['comment', makeSourceChangeAdd({id: 'c0', issueID: 'i1'})],
        // Re-hydrates the partition from the source, including c0.
        ['issue', makeSourceChangeAdd({id: 'i1', title: 'issue 1'})],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c0",
              "issueID": "i1",
              Symbol(rc): 1,
            },
            {
              "id": "c1",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "title": "issue 1",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c3",
              "issueID": "i2",
              Symbol(rc): 1,
            },
          ],
          "id": "i2",
          "title": "issue 2",
          Symbol(rc): 1,
        },
        {
          "comments": [],
          "id": "i3",
          "title": "issue 3",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(actualStorage['.comments:take']['["take","i1"]'])
      .toMatchInlineSnapshot(`
      {
        "bound": {
          "id": "c1",
          "issueID": "i1",
        },
        "size": 2,
      }
    `);
  });

  test('window displacement cleans up the displaced parent partition', () => {
    // The parent query has its own limit. When a new issue enters the
    // window and displaces another, the take above the join pushes a
    // remove for the displaced issue and its comments partition must be
    // cleaned up.
    const sourceContents: SourceContents = {
      issue: [
        {id: 'i1', title: 'a'},
        {id: 'i2', title: 'b'},
        {id: 'i3', title: 'c'},
      ],
      comment: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i2'},
        {id: 'c3', issueID: 'i3'},
      ],
    };
    const ast: AST = {
      ...issueCommentAST,
      limit: 2,
    };

    const {data, actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents,
      ast,
      format: issueCommentFormat,
      pushes: [['issue', makeSourceChangeAdd({id: 'i0', title: 'z'})]],
    });

    // i0 displaced i2 from the window.
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i0",
          "title": "z",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "title": "a",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(Object.keys(actualStorage['.comments:take'])).toMatchInlineSnapshot(`
      [
        "["take","i0"]",
        "["take","i1"]",
        "maxBound",
      ]
    `);
  });

  test('nested limited relationships are cleaned up transitively', () => {
    const sources: Sources = {
      ...issueCommentSources,
      reaction: {
        columns: {
          id: {type: 'string'},
          commentID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    };
    const sourceContents: SourceContents = {
      issue: [
        {id: 'i1', title: 'issue 1'},
        {id: 'i2', title: 'issue 2'},
      ],
      comment: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i2'},
      ],
      reaction: [
        {id: 'r1', commentID: 'c1'},
        {id: 'r2', commentID: 'c2'},
      ],
    };
    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comment',
            alias: 'comments',
            orderBy: [['id', 'asc']],
            limit: 2,
            related: [
              {
                system: 'client',
                correlation: {parentField: ['id'], childField: ['commentID']},
                subquery: {
                  table: 'reaction',
                  alias: 'reactions',
                  orderBy: [['id', 'asc']],
                  limit: 2,
                },
              },
            ],
          },
        },
      ],
    } as const;
    const format: Format = {
      singular: false,
      relationships: {
        comments: {
          singular: false,
          relationships: {
            reactions: {
              singular: false,
              relationships: {},
            },
          },
        },
      },
    };

    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [['issue', makeSourceChangeRemove({id: 'i1', title: 'issue 1'})]],
    });

    // Both the comments partition for i1 and the reactions partition for
    // i1's comment c1 are deleted.
    expect(Object.keys(actualStorage['.comments:take'])).toMatchInlineSnapshot(`
      [
        "["take","i2"]",
        "maxBound",
      ]
    `);
    expect(Object.keys(actualStorage['.comments.reactions:take']))
      .toMatchInlineSnapshot(`
      [
        "["take","c2"]",
        "maxBound",
      ]
    `);
  });

  test('parent remove with null join key is a no-op', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', title: 'issue 1'}],
      comment: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: null},
      ],
    };
    const ast: AST = {
      table: 'comment',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['issueID'], childField: ['id']},
          subquery: {
            table: 'issue',
            alias: 'issue',
            orderBy: [['id', 'asc']],
            limit: 1,
          },
        },
      ],
    } as const;
    const format: Format = {
      singular: false,
      relationships: {
        issue: {
          singular: false,
          relationships: {},
        },
      },
    };

    const {actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents,
      ast,
      format,
      pushes: [['comment', makeSourceChangeRemove({id: 'c2', issueID: null})]],
    });

    expect(Object.keys(actualStorage['.issue:take'])).toMatchInlineSnapshot(`
      [
        "["take","i1"]",
        "maxBound",
      ]
    `);
  });
});

describe('cap partition cleanup (exists)', () => {
  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
    },
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      comments: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('parent remove deletes its cap partition state', () => {
    const {actualStorage} = runPushTest({
      sources: issueCommentSources,
      sourceContents: issueCommentContents,
      ast,
      format,
      pushes: [['issue', makeSourceChangeRemove({id: 'i1', title: 'issue 1'})]],
    });

    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i2"]": {
          "pks": [
            "["c3"]",
          ],
          "size": 1,
        },
        "["cap","i3"]": {
          "pks": [],
          "size": 0,
        },
      }
    `);
  });

  test('cap partition is kept while another parent shares the correlation value', () => {
    // comments filtered by EXISTS issue: two comments share issueID i1, so
    // the cap partition for i1 may only be deleted with the last one.
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', title: 'issue 1'}],
      comment: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i1'},
      ],
    };
    const existsAst: AST = {
      table: 'comment',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        related: {
          system: 'client',
          correlation: {parentField: ['issueID'], childField: ['id']},
          subquery: {
            table: 'issue',
            alias: 'issue',
            orderBy: [['id', 'asc']],
          },
        },
        op: 'EXISTS',
      },
    } as const;
    const existsFormat: Format = {
      singular: false,
      relationships: {
        issue: {
          singular: false,
          relationships: {},
        },
      },
    };

    const afterFirstRemove = runPushTest({
      sources: issueCommentSources,
      sourceContents,
      ast: existsAst,
      format: existsFormat,
      pushes: [['comment', makeSourceChangeRemove({id: 'c1', issueID: 'i1'})]],
    });
    expect(Object.keys(afterFirstRemove.actualStorage['.issue:cap']))
      .toMatchInlineSnapshot(`
      [
        "["cap","i1"]",
      ]
    `);

    const afterBothRemoved = runPushTest({
      sources: issueCommentSources,
      sourceContents,
      ast: existsAst,
      format: existsFormat,
      pushes: [
        ['comment', makeSourceChangeRemove({id: 'c1', issueID: 'i1'})],
        ['comment', makeSourceChangeRemove({id: 'c2', issueID: 'i1'})],
      ],
    });
    expect(afterBothRemoved.actualStorage['.issue:cap']).toMatchInlineSnapshot(
      `{}`,
    );
  });
});

describe('destroy clears operator storage', () => {
  test('take and cap storage is emptied on destroy', () => {
    const lc = createSilentLogContext();
    const sources: Record<string, Source> = {};
    for (const [name, {columns, primaryKeys}] of Object.entries(
      issueCommentSources,
    )) {
      sources[name] = createSource(
        lc,
        testLogConfig,
        name,
        columns,
        primaryKeys,
      );
    }
    for (const [name, rows] of Object.entries(issueCommentContents)) {
      for (const row of rows) {
        consume(sources[name].push(makeSourceChangeAdd(row as Row)));
      }
    }

    const ast: AST = {
      ...issueCommentAST,
      where: {
        type: 'correlatedSubquery',
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['issueID']},
          subquery: {
            table: 'comment',
            alias: 'existsComments',
            orderBy: [['id', 'asc']],
          },
        },
        op: 'EXISTS',
      },
    };

    const delegate = new TestBuilderDelegate(sources);
    const pipeline = buildPipeline(ast, delegate, 'query-id');
    const catchOp = new Catch(pipeline);
    catchOp.fetch();

    // Hydration created take and cap state.
    expect(
      Object.entries(delegate.clonedStorage).some(
        ([, storage]) => Object.keys(storage).length > 0,
      ),
    ).toBe(true);

    catchOp.destroy();

    expect(delegate.clonedStorage).toEqual(
      Object.fromEntries(
        Object.keys(delegate.clonedStorage).map(name => [name, {}]),
      ),
    );
  });
});
