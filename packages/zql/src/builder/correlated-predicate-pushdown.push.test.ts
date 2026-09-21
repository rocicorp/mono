import {describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Catch} from '../ivm/catch.ts';
import type {Input} from '../ivm/operator.ts';
import {Snitch, type SnitchMessage} from '../ivm/snitch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  type Source,
} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {
  runPushTest,
  type PushTest,
  type Sources,
} from '../ivm/test/fetch-and-push-tests.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import type {Format} from '../ivm/view.ts';
import {buildPipeline} from './builder.ts';
import {TestBuilderDelegate} from './test-builder-delegate.ts';

const lc = createSilentLogContext();

/**
 * Runs `t` with the pass on and off. Asserts that the view and the pushes out
 * of the pipeline are the same.
 */
function runWithAndWithoutPushdown(t: PushTest) {
  const on = runPushTest(t);
  const off = runPushTest({...t, disableCorrelatedPredicatePushdown: true});
  expect(on.data).toEqual(off.data);
  expect(on.pushes).toEqual(off.pushes);
  return {on, off};
}

/**
 * Hydrates `t.ast`, applies `t.pushes`, and returns the number of rows that
 * each source returned to the fetches that the pushes caused.
 */
function rowsReadByPushes(t: PushTest): Record<string, number> {
  const sources: Record<string, Source> = {};
  for (const [name, {columns, primaryKeys}] of Object.entries(t.sources)) {
    const source = createSource(lc, testLogConfig, name, columns, primaryKeys);
    for (const row of t.sourceContents[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  const log: SnitchMessage[] = [];
  class CountingDelegate extends TestBuilderDelegate {
    override decorateInput(input: Input, name: string): Input {
      return name.includes(':source(')
        ? new Snitch(input, name, log, ['fetchCount'])
        : input;
    }
  }
  const delegate = new CountingDelegate(
    sources,
    false,
    t.enableNotExists,
    t.disableCorrelatedPredicatePushdown,
  );
  new Catch(buildPipeline(t.ast, delegate, 'query-id')).fetch();
  log.length = 0;
  for (const [name, change] of t.pushes) {
    consume(sources[name].push(change));
  }
  const counts: Record<string, number> = {};
  for (const m of log) {
    if (m[1] === 'fetchCount') {
      counts[m[0]] = (counts[m[0]] ?? 0) + m[3];
    }
  }
  return counts;
}

function fetches(log: SnitchMessage[], name: string) {
  return log.filter(m => m[0] === name && m[1] === 'fetch').length;
}

function childPushes(log: SnitchMessage[], name: string) {
  return log.filter(
    m => m[0] === name && m[1] === 'push' && m[2].type === 'child',
  ).length;
}

function cmp(column: string, value: string | number): Condition {
  return {
    type: 'simple',
    left: {type: 'column', name: column},
    op: '=',
    right: {type: 'literal', value},
  };
}

describe('user -> reading -> works -> covers', () => {
  const sources: Sources = {
    user: {columns: {userID: {type: 'string'}}, primaryKeys: ['userID']},
    reading: {
      columns: {
        id: {type: 'string'},
        userID: {type: 'string'},
        workID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    works: {columns: {id: {type: 'string'}}, primaryKeys: ['id']},
    covers: {
      columns: {id: {type: 'string'}, workID: {type: 'string'}},
      primaryKeys: ['id'],
    },
  };

  const ast: AST = {
    table: 'user',
    where: cmp('userID', 'u0'),
    related: [
      {
        correlation: {parentField: ['userID'], childField: ['userID']},
        subquery: {
          table: 'reading',
          alias: 'reading',
          related: [
            {
              correlation: {parentField: ['workID'], childField: ['id']},
              subquery: {
                table: 'works',
                alias: 'works',
                related: [
                  {
                    correlation: {parentField: ['id'], childField: ['workID']},
                    subquery: {table: 'covers', alias: 'covers'},
                  },
                ],
              },
            },
          ],
        },
      },
    ],
  };

  const format: Format = {
    singular: false,
    relationships: {
      reading: {
        singular: false,
        relationships: {
          works: {
            singular: true,
            relationships: {covers: {singular: false, relationships: {}}},
          },
        },
      },
    },
  };

  test.each([3, 30])(
    'a cover push reads one reading and one user with %i readers',
    readers => {
      const users: Row[] = [];
      const readings: Row[] = [];
      for (let i = 0; i < readers; i++) {
        users.push({userID: `u${i}`});
        readings.push({id: `r${i}`, userID: `u${i}`, workID: 'w1'});
      }

      const t: PushTest = {
        sources,
        sourceContents: {
          user: users,
          reading: readings,
          works: [{id: 'w1'}],
          covers: [{id: 'c1', workID: 'w1'}],
        },
        ast,
        format,
        pushes: [['covers', makeSourceChangeAdd({id: 'c2', workID: 'w1'})]],
      };
      const {on, off} = runWithAndWithoutPushdown(t);

      // One `child` change per reading of w1, and one parent lookup each.
      expect(childPushes(on.log, '.reading:join(works)')).toBe(1);
      expect(fetches(on.log, ':source(user)')).toBe(1);
      expect(childPushes(off.log, '.reading:join(works)')).toBe(readers);
      expect(fetches(off.log, ':source(user)')).toBe(readers);

      expect(rowsReadByPushes(t)).toEqual({
        '.reading.works:source(works)': 1,
        '.reading:source(reading)': 1,
        ':source(user)': 1,
      });
      expect(
        rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
      ).toEqual({
        '.reading.works:source(works)': 1,
        '.reading:source(reading)': readers,
        ':source(user)': 1,
      });

      expect(on.pushes).toHaveLength(1);
    },
  );
});

describe('posts.where(id, 42).related(comments)', () => {
  const sources: Sources = {
    posts: {columns: {id: {type: 'number'}}, primaryKeys: ['id']},
    comments: {
      columns: {
        id: {type: 'string'},
        postID: {type: 'number'},
        text: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  const ast: AST = {
    table: 'posts',
    where: cmp('id', 42),
    related: [
      {
        correlation: {parentField: ['id'], childField: ['postID']},
        subquery: {table: 'comments', alias: 'comments'},
      },
    ],
  };

  const format: Format = {
    singular: false,
    relationships: {comments: {singular: false, relationships: {}}},
  };

  const sourceContents = {
    posts: [{id: 7}, {id: 42}],
    comments: [
      {id: 'c1', postID: 42, text: 'a'},
      {id: 'c2', postID: 7, text: 'b'},
    ],
  };

  test('the comments connection filters on postID', () => {
    const posts = createSource(
      lc,
      testLogConfig,
      'posts',
      sources.posts.columns,
      ['id'],
    );
    const comments = createSource(
      lc,
      testLogConfig,
      'comments',
      sources.comments.columns,
      ['id'],
    );
    const connect = vi.spyOn(comments, 'connect');
    consume(
      buildPipeline(
        ast,
        new TestBuilderDelegate({posts, comments}),
        'query-id',
      ).fetch({}),
    );
    expect(connect).toHaveBeenCalledOnce();
    expect(connect.mock.calls[0][1]).toEqual(cmp('postID', 42));
  });

  test('a comment on another post stops at the source', () => {
    const {on, off} = runWithAndWithoutPushdown({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comments',
          makeSourceChangeEdit(
            {id: 'c2', postID: 7, text: 'c'},
            {id: 'c2', postID: 7, text: 'b'},
          ),
        ],
      ],
    });

    expect(on.log).toEqual([]);
    expect(off.log).toMatchInlineSnapshot(`
      [
        [
          ".comments:source(comments)",
          "push",
          {
            "oldRow": {
              "id": "c2",
              "postID": 7,
              "text": "b",
            },
            "row": {
              "id": "c2",
              "postID": 7,
              "text": "c",
            },
            "type": "edit",
          },
        ],
        [
          ":source(posts)",
          "fetch",
          {
            "constraint": {
              "id": 7,
            },
          },
        ],
      ]
    `);
    expect(on.pushes).toEqual([]);
  });

  test('a comment on the post still arrives', () => {
    const {on} = runWithAndWithoutPushdown({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        ['comments', makeSourceChangeAdd({id: 'c3', postID: 42, text: 'c'})],
        [
          'comments',
          makeSourceChangeEdit(
            {id: 'c2', postID: 42, text: 'b'},
            {id: 'c2', postID: 7, text: 'b'},
          ),
        ],
      ],
    });
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "postID": 42,
              "text": "a",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "postID": 42,
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "postID": 42,
              "text": "c",
              Symbol(rc): 1,
            },
          ],
          "id": 42,
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('issue.where(id).related(comments.limit().related(creator))', () => {
  // The zbugs `issueDetail` shape. A change to a user fetches the user's
  // comments through `Take` with no partition constraint.
  const sources: Sources = {
    issue: {columns: {id: {type: 'string'}}, primaryKeys: ['id']},
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
        creatorID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    user: {
      columns: {id: {type: 'string'}, name: {type: 'string'}},
      primaryKeys: ['id'],
    },
  };

  const ast: AST = {
    table: 'issue',
    where: cmp('id', 'i1'),
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'comments',
          limit: 2,
          related: [
            {
              correlation: {parentField: ['creatorID'], childField: ['id']},
              subquery: {table: 'user', alias: 'creator'},
            },
          ],
        },
      },
    ],
  };

  const format: Format = {
    singular: false,
    relationships: {
      comments: {
        singular: false,
        relationships: {creator: {singular: true, relationships: {}}},
      },
    },
  };

  test('a user edit reads only the comments on the issue', () => {
    // `Take` reads the user's comments up to its largest bound. The comment on
    // i1 sorts last, so without the pass that is every comment by the user.
    const comments: Row[] = [{id: 'z', issueID: 'i1', creatorID: 'u1'}];
    for (let i = 1; i < 20; i++) {
      comments.push({id: `c${i}`, issueID: `other${i}`, creatorID: 'u1'});
    }

    const t: PushTest = {
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: comments,
        user: [{id: 'u1', name: 'alice'}],
      },
      ast,
      format,
      pushes: [
        [
          'user',
          makeSourceChangeEdit(
            {id: 'u1', name: 'bob'},
            {id: 'u1', name: 'alice'},
          ),
        ],
      ],
    };
    const {on} = runWithAndWithoutPushdown(t);

    expect(rowsReadByPushes(t)).toEqual({
      '.comments:source(comment)': 1,
      ':source(issue)': 1,
    });
    expect(
      rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
    ).toEqual({
      '.comments:source(comment)': 20,
      ':source(issue)': 1,
    });
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "creator": {
                "id": "u1",
                "name": "bob",
                Symbol(rc): 1,
              },
              "creatorID": "u1",
              "id": "z",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('issue.where(projectID).whereExists(project)', () => {
  const sources: Sources = {
    issue: {
      columns: {id: {type: 'string'}, projectID: {type: 'string'}},
      primaryKeys: ['id'],
    },
    project: {
      columns: {id: {type: 'string'}, visibility: {type: 'string'}},
      primaryKeys: ['id'],
    },
  };

  const sourceContents = {
    issue: [
      {id: 'i1', projectID: 'p1'},
      {id: 'i2', projectID: 'p2'},
    ],
    project: [
      {id: 'p1', visibility: 'public'},
      {id: 'p2', visibility: 'public'},
    ],
  };

  const format: Format = {singular: false, relationships: {}};

  function makeAST(flip: boolean): AST {
    return {
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          cmp('projectID', 'p1'),
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            flip,
            related: {
              correlation: {parentField: ['projectID'], childField: ['id']},
              subquery: {
                table: 'project',
                alias: 'project',
                where: cmp('visibility', 'public'),
              },
            },
          },
        ],
      },
    };
  }

  test.each([false, true])(
    'a change to another project stops at the source (flip: %s)',
    flip => {
      const {on, off} = runWithAndWithoutPushdown({
        sources,
        sourceContents,
        ast: makeAST(flip),
        format,
        pushes: [
          [
            'project',
            makeSourceChangeEdit(
              {id: 'p2', visibility: 'private'},
              {id: 'p2', visibility: 'public'},
            ),
          ],
        ],
      });
      expect(on.log).toEqual([]);
      expect(off.log).not.toEqual([]);
    },
  );

  test.each([false, true])(
    'a change to the project still arrives (flip: %s)',
    flip => {
      const {on} = runWithAndWithoutPushdown({
        sources,
        sourceContents,
        ast: makeAST(flip),
        format,
        pushes: [
          [
            'project',
            makeSourceChangeEdit(
              {id: 'p1', visibility: 'private'},
              {id: 'p1', visibility: 'public'},
            ),
          ],
        ],
      });
      expect(on.data).toEqual([]);
      expect(on.pushes).toEqual([
        {
          type: 'remove',
          node: {
            row: {id: 'i1', projectID: 'p1'},
            relationships: {
              project_0: [
                {row: {id: 'p1', visibility: 'public'}, relationships: {}},
              ],
            },
          },
        },
      ]);
    },
  );
});
