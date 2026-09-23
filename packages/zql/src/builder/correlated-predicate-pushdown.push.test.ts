import {describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Catch, type CaughtChange} from '../ivm/catch.ts';
import type {Input} from '../ivm/operator.ts';
import {Snitch, type SnitchMessage} from '../ivm/snitch.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
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

function makeSources(t: PushTest): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const [name, {columns, primaryKeys}] of Object.entries(t.sources)) {
    const source = createSource(lc, testLogConfig, name, columns, primaryKeys);
    for (const row of t.sourceContents[name] ?? []) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

/**
 * Builds `t.ast` and returns the filter that each connection to `table` got,
 * in the order that the builder connected them.
 */
function connectFilters(t: PushTest, table: string): (Condition | undefined)[] {
  const sources = makeSources(t);
  const connect = vi.spyOn(sources[table], 'connect');
  buildPipeline(
    t.ast,
    new TestBuilderDelegate(
      sources,
      false,
      t.enableNotExists,
      t.disableCorrelatedPredicatePushdown,
    ),
    'query-id',
  );
  return connect.mock.calls.map(call => call[1]);
}

/**
 * The simple conditions that are top-level conjuncts of the filters that
 * `table`'s connections got. The pushed conditions are among them.
 */
function connectConjuncts(t: PushTest, table: string): Condition[] {
  const conjuncts = (c: Condition | undefined): Condition[] =>
    c === undefined
      ? []
      : c.type === 'and'
        ? c.conditions.flatMap(conjuncts)
        : c.type === 'simple'
          ? [c]
          : [];
  return connectFilters(t, table).flatMap(conjuncts);
}

/**
 * Hydrates `t.ast`, applies `t.pushes`, and returns the number of rows that
 * each source returned to the fetches that the pushes caused.
 */
function rowsReadByPushes(t: PushTest): Record<string, number> {
  const sources = makeSources(t);
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

function sourcePushes(log: SnitchMessage[], name: string) {
  return log.flatMap(m => (m[0] === name && m[1] === 'push' ? [m[2]] : []));
}

function childPushes(log: SnitchMessage[], name: string) {
  return log.filter(
    m => m[0] === name && m[1] === 'push' && m[2].type === 'child',
  ).length;
}

function cmp(column: string, value: string | number | boolean): Condition {
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

      // With partitionMap in Join, reading:join(works) indexes resident
      // parents by junction key, so even without static pushdown only the
      // 1 resident reading (u0) is pushed/fetched rather than all readers.
      expect(childPushes(on.log, '.reading:join(works)')).toBe(1);
      expect(fetches(on.log, ':source(user)')).toBe(1);
      expect(childPushes(off.log, '.reading:join(works)')).toBe(1);
      expect(fetches(off.log, ':source(user)')).toBe(1);

      expect(rowsReadByPushes(t)).toEqual({
        '.reading.works:source(works)': 1,
        '.reading:source(reading)': 1,
        ':source(user)': 1,
      });
      expect(
        rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
      ).toEqual({
        '.reading.works:source(works)': 1,
        '.reading:source(reading)': 1,
        ':source(user)': 1,
      });

      expect(on.pushes).toHaveLength(1);
    },
  );

  test('a reading of another user stops at the source', () => {
    const {on, off} = runWithAndWithoutPushdown({
      sources,
      sourceContents: {
        user: [{userID: 'u0'}],
        reading: [{id: 'r0', userID: 'u0', workID: 'w1'}],
        works: [{id: 'w1'}],
        covers: [{id: 'c1', workID: 'w1'}],
      },
      ast,
      format,
      pushes: [
        [
          'reading',
          makeSourceChangeAdd({id: 'r1', userID: 'u1', workID: 'w1'}),
        ],
      ],
    });
    expect(on.log).toEqual([]);
    expect(off.log).not.toEqual([]);
    expect(on.pushes).toEqual([]);
  });
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
    // With partitionMap in Join, comments:join(creator) indexes resident
    // comments for u1 (only comment z on i1), so even without static pushdown
    // only 1 comment is read instead of scanning all comments by u1.
    expect(
      rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
    ).toEqual({
      '.comments:source(comment)': 1,
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

describe('user.where(userID).related(reading.related(shelf)), all on userID', () => {
  // A chain whose hops all correlate on the same column. The pass copies the
  // root's pin into `reading`, and from there into `shelf`.
  const sources: Sources = {
    user: {columns: {userID: {type: 'string'}}, primaryKeys: ['userID']},
    reading: {
      columns: {id: {type: 'string'}, userID: {type: 'string'}},
      primaryKeys: ['id'],
    },
    shelf: {
      columns: {
        id: {type: 'string'},
        userID: {type: 'string'},
        name: {type: 'string'},
      },
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
              correlation: {parentField: ['userID'], childField: ['userID']},
              subquery: {table: 'shelf', alias: 'shelf'},
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
        relationships: {shelf: {singular: false, relationships: {}}},
      },
    },
  };

  const sourceContents = {
    user: [{userID: 'u0'}, {userID: 'u1'}],
    reading: [
      {id: 'r0', userID: 'u0'},
      {id: 'r1', userID: 'u1'},
    ],
    shelf: [
      {id: 's0', userID: 'u0', name: 'a'},
      {id: 's1', userID: 'u1', name: 'b'},
    ],
  };

  function makeTest(pushes: PushTest['pushes']): PushTest {
    return {sources, sourceContents, ast, format, pushes};
  }

  test('the pin reaches the grandchild connection', () => {
    const t = makeTest([]);
    expect(connectFilters(t, 'reading')).toEqual([cmp('userID', 'u0')]);
    expect(connectFilters(t, 'shelf')).toEqual([cmp('userID', 'u0')]);
    const off = {...t, disableCorrelatedPredicatePushdown: true};
    expect(connectFilters(off, 'reading')).toEqual([undefined]);
    expect(connectFilters(off, 'shelf')).toEqual([undefined]);
  });

  test('a shelf of another user stops at the source', () => {
    const t = makeTest([
      ['shelf', makeSourceChangeAdd({id: 's2', userID: 'u1', name: 'c'})],
    ]);
    const {on, off} = runWithAndWithoutPushdown(t);
    expect(on.log).toEqual([]);
    expect(off.log).not.toEqual([]);
    expect(on.pushes).toEqual([]);
    // Without the pass, the push is emitted by shelf into the pipeline
    // (off.log is non-empty), but reading:join(shelf)'s partitionMap knows
    // u1 is not resident, avoiding any source fetch. With the pass, shelf
    // filters at the source, so on.log is completely empty.
    expect(rowsReadByPushes(t)).toEqual({});
    expect(
      rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
    ).toEqual({});
  });

  test('a shelf of the pinned user arrives', () => {
    const {on} = runWithAndWithoutPushdown(
      makeTest([
        ['shelf', makeSourceChangeAdd({id: 's2', userID: 'u0', name: 'c'})],
      ]),
    );
    expect(on.pushes).not.toEqual([]);
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "reading": [
            {
              "id": "r0",
              "shelf": [
                {
                  "id": "s0",
                  "name": "a",
                  "userID": "u0",
                  Symbol(rc): 1,
                },
                {
                  "id": "s2",
                  "name": "c",
                  "userID": "u0",
                  Symbol(rc): 1,
                },
              ],
              "userID": "u0",
              Symbol(rc): 1,
            },
          ],
          "userID": "u0",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('a reading that moves to the pinned user arrives with its shelves', () => {
    const {on, off} = runWithAndWithoutPushdown(
      makeTest([
        [
          'reading',
          makeSourceChangeEdit(
            {id: 'r1', userID: 'u0'},
            {id: 'r1', userID: 'u1'},
          ),
        ],
      ]),
    );
    // The edit changes a join key, so the source splits it. The pushed filter
    // drops the half for u1.
    expect(sourcePushes(on.log, '.reading:source(reading)')).toEqual([
      {type: 'add', row: {id: 'r1', userID: 'u0'}},
    ]);
    expect(sourcePushes(off.log, '.reading:source(reading)')).toEqual([
      {type: 'remove', row: {id: 'r1', userID: 'u1'}},
      {type: 'add', row: {id: 'r1', userID: 'u0'}},
    ]);
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "reading": [
            {
              "id": "r0",
              "shelf": [
                {
                  "id": "s0",
                  "name": "a",
                  "userID": "u0",
                  Symbol(rc): 1,
                },
              ],
              "userID": "u0",
              Symbol(rc): 1,
            },
            {
              "id": "r1",
              "shelf": [
                {
                  "id": "s0",
                  "name": "a",
                  "userID": "u0",
                  Symbol(rc): 1,
                },
              ],
              "userID": "u0",
              Symbol(rc): 1,
            },
          ],
          "userID": "u0",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('a reading that moves away from the pinned user leaves', () => {
    const {on, off} = runWithAndWithoutPushdown(
      makeTest([
        [
          'reading',
          makeSourceChangeEdit(
            {id: 'r0', userID: 'u1'},
            {id: 'r0', userID: 'u0'},
          ),
        ],
      ]),
    );
    expect(sourcePushes(on.log, '.reading:source(reading)')).toEqual([
      {type: 'remove', row: {id: 'r0', userID: 'u0'}},
    ]);
    expect(sourcePushes(off.log, '.reading:source(reading)')).toEqual([
      {type: 'remove', row: {id: 'r0', userID: 'u0'}},
      {type: 'add', row: {id: 'r0', userID: 'u1'}},
    ]);
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "reading": [],
          "userID": "u0",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('user.related(reading.where(workID).related(works.related(covers)))', () => {
  // The pin is inside a subquery, not at the root. The pass copies it from
  // `reading` into `works` as `id`, and from there into `covers`.
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
    related: [
      {
        correlation: {parentField: ['userID'], childField: ['userID']},
        subquery: {
          table: 'reading',
          alias: 'reading',
          where: cmp('workID', 'w1'),
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

  const sourceContents = {
    user: [{userID: 'u0'}, {userID: 'u1'}, {userID: 'u2'}],
    reading: [
      {id: 'r0', userID: 'u0', workID: 'w1'},
      {id: 'r1', userID: 'u1', workID: 'w1'},
      {id: 'r2', userID: 'u2', workID: 'w2'},
    ],
    works: [{id: 'w1'}, {id: 'w2'}],
    covers: [
      {id: 'c1', workID: 'w1'},
      {id: 'c2', workID: 'w2'},
    ],
  };

  function makeTest(pushes: PushTest['pushes']): PushTest {
    return {sources, sourceContents, ast, format, pushes};
  }

  test('the pin reaches the child and grandchild connections', () => {
    const t = makeTest([]);
    expect(connectFilters(t, 'works')).toEqual([cmp('id', 'w1')]);
    expect(connectFilters(t, 'covers')).toEqual([cmp('workID', 'w1')]);
    const off = {...t, disableCorrelatedPredicatePushdown: true};
    expect(connectFilters(off, 'works')).toEqual([undefined]);
    expect(connectFilters(off, 'covers')).toEqual([undefined]);
  });

  test('a cover of another work stops at the source', () => {
    const t = makeTest([
      ['covers', makeSourceChangeAdd({id: 'c3', workID: 'w2'})],
    ]);
    const {on, off} = runWithAndWithoutPushdown(t);
    expect(on.log).toEqual([]);
    expect(off.log).not.toEqual([]);
    expect(on.pushes).toEqual([]);
    // Without the pass, the push enters the pipeline (off.log is non-empty),
    // but works:join(covers)'s partitionMap knows w2 is not resident, avoiding
    // any source fetch. With the pass, covers filters at the source (on.log is []).
    expect(rowsReadByPushes(t)).toEqual({});
    expect(
      rowsReadByPushes({...t, disableCorrelatedPredicatePushdown: true}),
    ).toEqual({});
  });

  test('a cover of the pinned work arrives under every reader', () => {
    const {on} = runWithAndWithoutPushdown(
      makeTest([['covers', makeSourceChangeAdd({id: 'c3', workID: 'w1'})]]),
    );
    expect(on.pushes).toHaveLength(2);
    expect(on.data).toMatchInlineSnapshot(`
      [
        {
          "reading": [
            {
              "id": "r0",
              "userID": "u0",
              "workID": "w1",
              "works": {
                "covers": [
                  {
                    "id": "c1",
                    "workID": "w1",
                    Symbol(rc): 1,
                  },
                  {
                    "id": "c3",
                    "workID": "w1",
                    Symbol(rc): 1,
                  },
                ],
                "id": "w1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          "userID": "u0",
          Symbol(rc): 1,
        },
        {
          "reading": [
            {
              "id": "r1",
              "userID": "u1",
              "workID": "w1",
              "works": {
                "covers": [
                  {
                    "id": "c1",
                    "workID": "w1",
                    Symbol(rc): 1,
                  },
                  {
                    "id": "c3",
                    "workID": "w1",
                    Symbol(rc): 1,
                  },
                ],
                "id": "w1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          "userID": "u1",
          Symbol(rc): 1,
        },
        {
          "reading": [],
          "userID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('issue.where(projectID).where(or(closed, EXISTS project.EXISTS members))', () => {
  // The pin sits above an OR, and the EXISTS is under the OR. The pass copies
  // `projectID = p1` into `project` as `id`, and from there into `member`.
  // Flipping a gate under an OR builds a UnionFanOut / UnionFanIn pair.
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        projectID: {type: 'string'},
        closed: {type: 'boolean'},
      },
      primaryKeys: ['id'],
    },
    project: {
      columns: {id: {type: 'string'}, visibility: {type: 'string'}},
      primaryKeys: ['id'],
    },
    member: {
      columns: {
        id: {type: 'string'},
        projectID: {type: 'string'},
        role: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  // i1 is open, so it is in the output only through the gate. i2 is closed,
  // so it is always in the output. i3 is in another project.
  const sourceContents = {
    issue: [
      {id: 'i1', projectID: 'p1', closed: false},
      {id: 'i2', projectID: 'p1', closed: true},
      {id: 'i3', projectID: 'p2', closed: false},
    ],
    project: [
      {id: 'p1', visibility: 'public'},
      {id: 'p2', visibility: 'public'},
    ],
    member: [
      {id: 'm1', projectID: 'p1', role: 'viewer'},
      {id: 'm2', projectID: 'p2', role: 'admin'},
    ],
  };

  const format: Format = {singular: false, relationships: {}};

  type Gate = {
    op: 'EXISTS' | 'NOT EXISTS';
    flipProject: boolean;
    flipMembers: boolean;
  };

  function makeAST({op, flipProject, flipMembers}: Gate): AST {
    return {
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          cmp('projectID', 'p1'),
          {
            type: 'or',
            conditions: [
              cmp('closed', true),
              {
                type: 'correlatedSubquery',
                op,
                flip: flipProject,
                related: {
                  correlation: {parentField: ['projectID'], childField: ['id']},
                  subquery: {
                    table: 'project',
                    alias: 'project',
                    where: {
                      type: 'and',
                      conditions: [
                        cmp('visibility', 'public'),
                        {
                          type: 'correlatedSubquery',
                          op: 'EXISTS',
                          flip: flipMembers,
                          related: {
                            correlation: {
                              parentField: ['id'],
                              childField: ['projectID'],
                            },
                            subquery: {
                              table: 'member',
                              alias: 'members',
                              where: cmp('role', 'admin'),
                            },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            ],
          },
        ],
      },
    };
  }

  // Only a positive EXISTS can be flipped.
  const gates: Gate[] = [
    {op: 'EXISTS', flipProject: false, flipMembers: false},
    {op: 'EXISTS', flipProject: true, flipMembers: false},
    {op: 'EXISTS', flipProject: false, flipMembers: true},
    {op: 'EXISTS', flipProject: true, flipMembers: true},
    {op: 'NOT EXISTS', flipProject: false, flipMembers: false},
    {op: 'NOT EXISTS', flipProject: false, flipMembers: true},
  ];

  function makeTest(gate: Gate, pushes: PushTest['pushes']): PushTest {
    return {
      sources,
      sourceContents,
      ast: makeAST(gate),
      format,
      pushes,
      enableNotExists: gate.op === 'NOT EXISTS',
    };
  }

  function ids(data: unknown): string[] {
    return (data as {id: string}[]).map(r => r.id);
  }

  // The issues that enter and leave the output. A `child` change is also
  // pushed for i2, which is in the output through `closed`.
  function membership(pushes: CaughtChange[]): [string, unknown][] {
    return pushes.flatMap(p =>
      (p.type === 'add' || p.type === 'remove') && p.node !== 'yield'
        ? [[p.type, p.node.row.id] as [string, unknown]]
        : [],
    );
  }

  describe.each(gates)(
    '$op (flip project: $flipProject, flip members: $flipMembers)',
    gate => {
      // The issue that the gate decides is in the output when the gate is true.
      const gated = (gateIsTrue: boolean) =>
        gateIsTrue === (gate.op === 'EXISTS') ? ['i1', 'i2'] : ['i2'];

      test('the pin reaches the gate and the gate under it', () => {
        const t = makeTest(gate, []);
        expect(connectConjuncts(t, 'project')).toEqual([
          cmp('visibility', 'public'),
          cmp('id', 'p1'),
        ]);
        expect(connectConjuncts(t, 'member')).toEqual([
          cmp('role', 'admin'),
          cmp('projectID', 'p1'),
        ]);
        const off = {...t, disableCorrelatedPredicatePushdown: true};
        expect(connectConjuncts(off, 'project')).toEqual([
          cmp('visibility', 'public'),
        ]);
        expect(connectConjuncts(off, 'member')).toEqual([cmp('role', 'admin')]);
      });

      test('an admin of another project stops at the source', () => {
        const {on, off} = runWithAndWithoutPushdown(
          makeTest(gate, [
            [
              'member',
              makeSourceChangeAdd({id: 'm3', projectID: 'p2', role: 'admin'}),
            ],
            [
              'member',
              makeSourceChangeRemove({
                id: 'm2',
                projectID: 'p2',
                role: 'admin',
              }),
            ],
          ]),
        );
        expect(on.log).toEqual([]);
        expect(off.log).not.toEqual([]);
        expect(on.pushes).toEqual([]);
        expect(ids(on.data)).toEqual(gated(false));
      });

      test('an admin of the pinned project opens the gate', () => {
        const {on} = runWithAndWithoutPushdown(
          makeTest(gate, [
            [
              'member',
              makeSourceChangeAdd({id: 'm3', projectID: 'p1', role: 'admin'}),
            ],
          ]),
        );
        expect(membership(on.pushes)).toEqual([
          gate.op === 'EXISTS' ? ['add', 'i1'] : ['remove', 'i1'],
        ]);
        expect(ids(on.data)).toEqual(gated(true));
        // A flipped gate under the OR sends the push through a UnionFanIn.
        expect(on.log.some(m => m[0] === ':ufi')).toBe(gate.flipProject);
      });

      test('a member of the pinned project opens and closes the gate', () => {
        const {on} = runWithAndWithoutPushdown(
          makeTest(gate, [
            // Opens: m1 becomes an admin.
            [
              'member',
              makeSourceChangeEdit(
                {id: 'm1', projectID: 'p1', role: 'admin'},
                {id: 'm1', projectID: 'p1', role: 'viewer'},
              ),
            ],
            // Still open: a second admin.
            [
              'member',
              makeSourceChangeAdd({id: 'm3', projectID: 'p1', role: 'admin'}),
            ],
            // Still open: m1 moves to p2, which leaves m3.
            [
              'member',
              makeSourceChangeEdit(
                {id: 'm1', projectID: 'p2', role: 'admin'},
                {id: 'm1', projectID: 'p1', role: 'admin'},
              ),
            ],
            // Closes: the last admin of p1 leaves.
            [
              'member',
              makeSourceChangeRemove({
                id: 'm3',
                projectID: 'p1',
                role: 'admin',
              }),
            ],
          ]),
        );
        expect(membership(on.pushes)).toEqual([
          gate.op === 'EXISTS' ? ['add', 'i1'] : ['remove', 'i1'],
          gate.op === 'EXISTS' ? ['remove', 'i1'] : ['add', 'i1'],
        ]);
        expect(ids(on.data)).toEqual(gated(false));
      });
    },
  );
});
