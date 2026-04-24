import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {Cap} from './cap.ts';
import {Catch} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import {consume} from './stream.ts';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import {createSource} from './test/source-factory.ts';
import type {Format} from './view.ts';

import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
describe('Cap push - basic behavior', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        text: {type: 'string'},
      },
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
  };

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

  test('child add below cap limit is forwarded', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [{id: 'c1', issueID: 'i1', text: 'c1'}],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        ['comment', makeSourceChangeAdd({id: 'c2', issueID: 'i1', text: 'c2'})],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
          ],
          "size": 2,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".comments:cap",
          "push",
          {
            "row": {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
            },
            "type": "add",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c2",
                  "issueID": "i1",
                  "text": "c2",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child add at cap limit is dropped', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
        {id: 'c3', issueID: 'i1', text: 'c3'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        ['comment', makeSourceChangeAdd({id: 'c4', issueID: 'i1', text: 'c4'})],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i1",
              "text": "c3",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
            "["c3"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap')).toMatchInlineSnapshot(
      `[]`,
    );
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('child remove with refill', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
        {id: 'c3', issueID: 'i1', text: 'c3'},
        {id: 'c4', issueID: 'i1', text: 'c4'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeRemove({id: 'c2', issueID: 'i1', text: 'c2'}),
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i1",
              "text": "c3",
              Symbol(rc): 1,
            },
            {
              "id": "c4",
              "issueID": "i1",
              "text": "c4",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c3"]",
            "["c4"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".comments:cap",
          "push",
          {
            "row": {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments:cap",
          "push",
          {
            "row": {
              "id": "c4",
              "issueID": "i1",
              "text": "c4",
            },
            "type": "add",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c2",
                  "issueID": "i1",
                  "text": "c2",
                },
              },
              "type": "remove",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "i1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c4",
                  "issueID": "i1",
                  "text": "c4",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child remove without refill', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeRemove({id: 'c1', issueID: 'i1', text: 'c1'}),
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c2"]",
          ],
          "size": 1,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".comments:cap",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "c1",
                },
              },
              "type": "remove",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child remove of last row causes parent retraction', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [{id: 'c1', issueID: 'i1', text: 'c1'}],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeRemove({id: 'c1', issueID: 'i1', text: 'c1'}),
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [],
          "size": 0,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".comments:cap",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c1",
                    "issueID": "i1",
                    "text": "c1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
              "text": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('child remove of untracked PK is dropped', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
        {id: 'c3', issueID: 'i1', text: 'c3'},
        {id: 'c4', issueID: 'i1', text: 'c4'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeRemove({id: 'c4', issueID: 'i1', text: 'c4'}),
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i1",
              "text": "c3",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
            "["c3"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap')).toMatchInlineSnapshot(
      `[]`,
    );
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('child edit of tracked PK is forwarded', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeEdit(
            {id: 'c1', issueID: 'i1', text: 'c1 updated'},
            {id: 'c1', issueID: 'i1', text: 'c1'},
          ),
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "c1 updated",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
          ],
          "size": 2,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.comments:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".comments:cap",
          "push",
          {
            "oldRow": {
              "id": "c1",
              "issueID": "i1",
              "text": "c1",
            },
            "row": {
              "id": "c1",
              "issueID": "i1",
              "text": "c1 updated",
            },
            "type": "edit",
          },
        ],
        [
          ".comments:cap",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "oldRow": {
                "id": "c1",
                "issueID": "i1",
                "text": "c1",
              },
              "row": {
                "id": "c1",
                "issueID": "i1",
                "text": "c1 updated",
              },
              "type": "edit",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child edit that changes PK updates tracked set', () => {
    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
      ],
    };
    const {data, actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'comment',
          makeSourceChangeEdit(
            {id: 'c1_renamed', issueID: 'i1', text: 'c1'},
            {id: 'c1', issueID: 'i1', text: 'c1'},
          ),
        ],
      ],
    });

    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1_renamed"]",
            "["c2"]",
          ],
          "size": 2,
        },
      }
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1_renamed",
              "issueID": "i1",
              "text": "c1",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              "text": "c2",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});

describe('Cap push - unordered overlay in join', () => {
  const sources: Sources = {
    parent: {
      columns: {
        id: {type: 'string'},
        group: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    child: {
      columns: {
        id: {type: 'string'},
        group: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  const ast: AST = {
    table: 'parent',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['group'], childField: ['group']},
        subquery: {
          table: 'child',
          alias: 'children',
          orderBy: [['id', 'asc']],
        },
      },
      op: 'EXISTS',
    },
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      children: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('child remove with refill, multiple parents per child (unordered overlay)', () => {
    const sourceContents: SourceContents = {
      parent: [
        {id: 'p1', group: 'g1'},
        {id: 'p2', group: 'g1'},
      ],
      child: [
        {id: 'x1', group: 'g1', text: 'x1'},
        {id: 'x2', group: 'g1', text: 'x2'},
        {id: 'x3', group: 'g1', text: 'x3'},
        {id: 'x4', group: 'g1', text: 'x4'},
      ],
    };
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        ['child', makeSourceChangeRemove({id: 'x1', group: 'g1', text: 'x1'})],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "children": [
            {
              "group": "g1",
              "id": "x2",
              "text": "x2",
              Symbol(rc): 1,
            },
            {
              "group": "g1",
              "id": "x3",
              "text": "x3",
              Symbol(rc): 1,
            },
            {
              "group": "g1",
              "id": "x4",
              "text": "x4",
              Symbol(rc): 1,
            },
          ],
          "group": "g1",
          "id": "p1",
          Symbol(rc): 1,
        },
        {
          "children": [
            {
              "group": "g1",
              "id": "x2",
              "text": "x2",
              Symbol(rc): 1,
            },
            {
              "group": "g1",
              "id": "x3",
              "text": "x3",
              Symbol(rc): 1,
            },
            {
              "group": "g1",
              "id": "x4",
              "text": "x4",
              Symbol(rc): 1,
            },
          ],
          "group": "g1",
          "id": "p2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","g1"]": {
          "pks": [
            "["x2"]",
            "["x3"]",
            "["x4"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(log.filter(msg => msg[0] === '.children:cap'))
      .toMatchInlineSnapshot(`
      [
        [
          ".children:cap",
          "push",
          {
            "row": {
              "group": "g1",
              "id": "x1",
              "text": "x1",
            },
            "type": "remove",
          },
        ],
        [
          ".children:cap",
          "fetch",
          {
            "constraint": {
              "group": "g1",
            },
          },
        ],
        [
          ".children:cap",
          "fetch",
          {
            "constraint": {
              "group": "g1",
            },
          },
        ],
        [
          ".children:cap",
          "push",
          {
            "row": {
              "group": "g1",
              "id": "x4",
              "text": "x4",
            },
            "type": "add",
          },
        ],
        [
          ".children:cap",
          "fetch",
          {
            "constraint": {
              "group": "g1",
            },
          },
        ],
        [
          ".children:cap",
          "fetch",
          {
            "constraint": {
              "group": "g1",
            },
          },
        ],
      ]
    `);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "group": "g1",
                  "id": "x1",
                  "text": "x1",
                },
              },
              "type": "remove",
            },
            "relationshipName": "children",
          },
          "row": {
            "group": "g1",
            "id": "p1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "group": "g1",
                  "id": "x1",
                  "text": "x1",
                },
              },
              "type": "remove",
            },
            "relationshipName": "children",
          },
          "row": {
            "group": "g1",
            "id": "p2",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "group": "g1",
                  "id": "x4",
                  "text": "x4",
                },
              },
              "type": "add",
            },
            "relationshipName": "children",
          },
          "row": {
            "group": "g1",
            "id": "p1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "group": "g1",
                  "id": "x4",
                  "text": "x4",
                },
              },
              "type": "add",
            },
            "relationshipName": "children",
          },
          "row": {
            "group": "g1",
            "id": "p2",
          },
          "type": "child",
        },
      ]
    `);
  });
});

describe('Cap limit 0', () => {
  // Reproduces the bug where Cap#initialFetch asserted
  // "Constraint should match partition key" before checking limit === 0.
  // When Cap has limit=0 and receives a fetch with a constraint that
  // doesn't match the (undefined) partition key, the assertion should
  // not fire — the limit=0 check should return early first.
  const lc = createSilentLogContext();

  test.for([
    {name: 'no partition key', partitionKey: undefined},
    {name: 'with partition key', partitionKey: ['group'] as const},
  ])(
    'fetch with constraint and $name does not trigger assert',
    ({partitionKey}) => {
      const source = createSource(
        lc,
        testLogConfig,
        'table',
        {id: {type: 'string'}, group: {type: 'string'}},
        ['id'],
      );
      consume(source.push(makeSourceChangeAdd({id: '1', group: 'g1'})));

      const storage = new MemoryStorage();
      const cap = new Cap(
        source.connect([['id', 'asc']]),
        storage,
        0,
        partitionKey,
      );
      const c = new Catch(cap);
      const result = c.fetch({constraint: {group: 'g1'}});
      expect(result).toEqual([]);
    },
  );
});

describe('Cap wiring', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        text: {type: 'string'},
      },
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
  };
  const format: Format = {
    singular: false,
    relationships: {
      comments: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('flipped EXISTS subquery is NOT wired through Cap', () => {
    // Flipped EXISTS children route through FlippedJoin, which depends
    // on ordering. If the builder ever accidentally sends them through
    // Cap instead, ordering guarantees silently break. This test pins
    // the wiring.
    const flippedAst: AST = {
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
        flip: true,
      },
    } as const;

    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
      ],
    };
    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast: flippedAst,
      format,
      pushes: [],
    });

    const capKeys = Object.keys(actualStorage).filter(k => k.endsWith(':cap'));
    expect(capKeys).toEqual([]);
  });

  test('permissions-system EXISTS uses cap limit=1', () => {
    // EXISTS on a subquery with system='permissions' must use the
    // PERMISSIONS_EXISTS_LIMIT (1) rather than the default EXISTS_LIMIT
    // (3). If this constant is ever changed, permissions checks could
    // overfetch or undercount — security-relevant.
    const permissionsAst: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        related: {
          system: 'permissions',
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

    const sourceContents: SourceContents = {
      issue: [{id: 'i1', text: 'i1'}],
      comment: [
        {id: 'c1', issueID: 'i1', text: 'c1'},
        {id: 'c2', issueID: 'i1', text: 'c2'},
        {id: 'c3', issueID: 'i1', text: 'c3'},
      ],
    };
    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast: permissionsAst,
      format,
      pushes: [],
    });

    expect(actualStorage['.comments:cap']).toMatchInlineSnapshot(`
      {
        "["cap","i1"]": {
          "pks": [
            "["c1"]",
          ],
          "size": 1,
        },
      }
    `);
  });
});
