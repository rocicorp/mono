import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {newQuery} from '../query/query-impl.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import type {AnyQuery} from '../query/query.ts';
import {schema as testSchema} from '../query/test/test-schemas.ts';
import {Cap} from './cap.ts';
import {Catch} from './catch.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {Source} from './source.ts';
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

  test('pushes are dropped when limit=0 (no capState ever set)', () => {
    // limit=0 short-circuits #initialFetch before capState is written.
    // Every subsequent push must therefore see capState===undefined and
    // silently drop. If capState ever leaked in (e.g. from a future
    // refactor that pre-seeds state), pushes would start forwarding and
    // produce rows that EXISTS-with-limit-0 must never emit.
    const source = createSource(
      lc,
      testLogConfig,
      'table',
      {
        id: {type: 'string'},
        group: {type: 'string'},
        text: {type: 'string'},
      },
      ['id'],
    );
    consume(
      source.push(makeSourceChangeAdd({id: '1', group: 'g1', text: 'a'})),
    );

    const storage = new MemoryStorage();
    const cap = new Cap(source.connect([['id', 'asc']]), storage, 0, ['group']);
    const c = new Catch(cap);

    // Fetch first — the early-return path that does NOT seed capState.
    expect(c.fetch({constraint: {group: 'g1'}})).toEqual([]);

    // ADD / REMOVE / EDIT must all be no-ops at the Cap level.
    consume(
      source.push(makeSourceChangeAdd({id: '2', group: 'g1', text: 'b'})),
    );
    consume(
      source.push(makeSourceChangeRemove({id: '1', group: 'g1', text: 'a'})),
    );
    consume(
      source.push(
        makeSourceChangeEdit(
          {id: '2', group: 'g1', text: 'b updated'},
          {id: '2', group: 'g1', text: 'b'},
        ),
      ),
    );

    expect(c.pushes).toEqual([]);
    expect(storage.cloneData()).toEqual({});
  });
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

  test('EXISTS subquery with start throws', () => {
    // builder.ts:294 asserts that non-flipped EXISTS children have no
    // `start` bound. If anything ever serializes a `start` into an
    // EXISTS subquery AST, the assert is the only thing standing between
    // us and a Cap pipeline that silently uses an unsupported bound.
    const astWithStart: AST = {
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
            start: {row: {id: 'c0'}, exclusive: false},
          },
        },
        op: 'EXISTS',
      },
    } as const;

    expect(() =>
      runPushTest({
        sources,
        sourceContents: {issue: [], comment: []},
        ast: astWithStart,
        format,
        pushes: [],
      }),
    ).toThrow('EXISTS subqueries must not have start');
  });

  test('EXISTS subquery with related throws', () => {
    // builder.ts:297 asserts that non-flipped EXISTS children have no
    // `related`. EXISTS is a presence check; nesting a `related` into
    // it would build a Join under Cap that's never observed, and the
    // companion partition assumptions would break.
    const astWithRelated: AST = {
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
            related: [
              {
                system: 'client',
                correlation: {
                  parentField: ['issueID'],
                  childField: ['id'],
                },
                subquery: {
                  table: 'issue',
                  alias: 'issue',
                  orderBy: [['id', 'asc']],
                },
              },
            ],
          },
        },
        op: 'EXISTS',
      },
    } as const;

    expect(() =>
      runPushTest({
        sources,
        sourceContents: {issue: [], comment: []},
        ast: astWithRelated,
        format,
        pushes: [],
      }),
    ).toThrow('EXISTS subqueries must not have related');
  });

  test('nested EXISTS produces a Cap at every level', () => {
    // Each non-flipped EXISTS in the tree gets its own Cap. The outer
    // Cap caps issues' direct comments; the inner Cap caps each
    // comment's revisions. If wiring ever flattens these, only one
    // Cap storage would appear and inner-level overfetch would be
    // invisible.
    const sourcesNested: Sources = {
      issue: {
        columns: {id: {type: 'string'}},
        primaryKeys: ['id'],
      },
      comment: {
        columns: {
          id: {type: 'string'},
          issueID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      revision: {
        columns: {
          id: {type: 'string'},
          commentID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    };

    const nestedAst: AST = {
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
            where: {
              type: 'correlatedSubquery',
              related: {
                system: 'client',
                correlation: {
                  parentField: ['id'],
                  childField: ['commentID'],
                },
                subquery: {
                  table: 'revision',
                  alias: 'revisions',
                  orderBy: [['id', 'asc']],
                },
              },
              op: 'EXISTS',
            },
          },
        },
        op: 'EXISTS',
      },
    } as const;

    const sourceContents: SourceContents = {
      issue: [{id: 'i1'}],
      comment: [{id: 'c1', issueID: 'i1'}],
      revision: [{id: 'r1', commentID: 'c1'}],
    };
    const {actualStorage} = runPushTest({
      sources: sourcesNested,
      sourceContents,
      ast: nestedAst,
      format: {singular: false, relationships: {}},
      pushes: [],
    });

    const capKeys = Object.keys(actualStorage)
      .filter(k => k.endsWith(':cap'))
      .sort();
    expect(capKeys).toEqual(['.comments.revisions:cap', '.comments:cap']);
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

  test('non-flipped EXISTS child with flipped OR branch falls back to Take', () => {
    // EXISTS-child source.connect goes unordered so SQLite can pick any
    // index, and Cap absorbs the unordered output. But if the child body
    // contains a flipped OR branch, applyFilterWithFlips builds a
    // UnionFanIn over that same source, and UnionFanIn's constructor
    // asserts the inputs have a sort. Without this fallback, building
    // the pipeline throws "UnionFanIn requires sorted input".
    const ast = astOf(
      newQuery(testSchema, 'issue').whereExists('comments', c =>
        c.where(({or, cmp, exists}) =>
          or(
            cmp('text', 'public'),
            exists('author', a => a.where('name', 'Alice'), {flip: true}),
          ),
        ),
      ),
    );

    const capKeys = capKeysFromBuild(ast);

    // No `:cap` storage because the EXISTS child took the Take path.
    expect(capKeys).toEqual([]);
  });

  test('non-flipped EXISTS child with OR(simple, non-flipped EXISTS) still uses Cap', () => {
    // Symmetric counterpart to the falls-back-to-Take test. An OR with
    // no flipped subqueries goes through the regular FanOut/FanIn path
    // (filter-level dedup, no sort requirement), so the Cap-vs-Take
    // gate must stay on Cap. If a future refactor broadened the flip
    // detection to fire on any subquery in the where, Cap would
    // silently disappear from this shape — this test pins it.
    const ast = astOf(
      newQuery(testSchema, 'issue').whereExists('comments', c =>
        c.where(({or, cmp, exists}) =>
          or(cmp('text', 'public'), exists('author')),
        ),
      ),
    );

    // The outer EXISTS child (`zsubq_comments`) keeps Cap because no
    // flips exist anywhere in its where. The non-flipped EXISTS inside
    // the OR also gets its own Cap as a nested EXISTS child. Aliases
    // are prefixed by the query builder (`zsubq_`) and the inside-where
    // ones are then uniquified (`_0`/`_1`/...).
    expect(capKeysFromBuild(ast)).toEqual([
      '.zsubq_comments.zsubq_author_0:cap',
      '.zsubq_comments:cap',
    ]);
  });

  test('non-flipped EXISTS child with OR(exists, exists, exists) still uses Cap at every level', () => {
    // Three non-flipped EXISTS branches in an OR. Asserts every level
    // got a Cap operator. Aliases inside `where` are uniquified
    // (`_0`/`_1`/`_2`) in source order.
    const ast = astOf(
      newQuery(testSchema, 'issue').whereExists('comments', c =>
        c.where(({or, exists}) =>
          or(exists('issue'), exists('revisions'), exists('author')),
        ),
      ),
    );

    expect(capKeysFromBuild(ast)).toEqual([
      '.zsubq_comments.zsubq_author_2:cap',
      '.zsubq_comments.zsubq_issue_0:cap',
      '.zsubq_comments.zsubq_revisions_1:cap',
      '.zsubq_comments:cap',
    ]);
  });
});

function astOf(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

// Builds the pipeline against the test schema (sources are empty;
// createStorage registers each operator's storage at build time, so we
// can read back the wiring without fetching). Avoids runPushTest here:
// its dual-materialization equality assertion is sensitive to OR
// short-circuit between Catch and ArrayView in shapes that have
// multiple non-flipped EXISTS branches under an OR.
function capKeysFromBuild(ast: AST): string[] {
  const sources: Record<string, Source> = {};
  for (const [name, table] of Object.entries(testSchema.tables)) {
    sources[name] = createSource(
      createSilentLogContext(),
      testLogConfig,
      name,
      table.columns,
      table.primaryKey,
    );
  }
  const delegate = new TestBuilderDelegate(sources, false);
  buildPipeline(ast, delegate, 'query-id');
  return Object.keys(delegate.clonedStorage)
    .filter(k => k.endsWith(':cap'))
    .sort();
}

describe('Cap push - compound partition key', () => {
  // Compound correlation: parent.(region,org) → child.(region,org).
  // Exercises multi-column partitioning in getCapStateKey, serializePK,
  // and makePartitionKeyComparator — paths the single-column tests skip.
  const sources: Sources = {
    parent: {
      columns: {
        id: {type: 'string'},
        region: {type: 'string'},
        org: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    child: {
      columns: {
        id: {type: 'string'},
        region: {type: 'string'},
        org: {type: 'string'},
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
        correlation: {
          parentField: ['region', 'org'],
          childField: ['region', 'org'],
        },
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

  test('partitions are keyed by all compound fields', () => {
    const sourceContents: SourceContents = {
      parent: [
        {id: 'p1', region: 'us', org: 'acme'},
        {id: 'p2', region: 'us', org: 'wayne'},
        {id: 'p3', region: 'eu', org: 'acme'},
      ],
      child: [
        {id: 'cA', region: 'us', org: 'acme', text: 'A'},
        {id: 'cB', region: 'us', org: 'wayne', text: 'B'},
      ],
    };
    const {data, actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [],
    });

    // p1 (us,acme) and p2 (us,wayne) each have their own child;
    // p3 (eu,acme) has no matching child → excluded.
    expect((data as unknown as readonly {id: string}[]).map(r => r.id)).toEqual(
      ['p1', 'p2'],
    );

    // Storage keys include BOTH partition columns — a single-column
    // key would collide (us,acme) with (us,wayne) and both parents
    // would appear to share a cap bucket.
    // p3 (eu,acme) has no matching children; its partition is still
    // hydrated with size=0 during the initial scan.
    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","eu","acme"]": {
          "pks": [],
          "size": 0,
        },
        "["cap","us","acme"]": {
          "pks": [
            "["cA"]",
          ],
          "size": 1,
        },
        "["cap","us","wayne"]": {
          "pks": [
            "["cB"]",
          ],
          "size": 1,
        },
      }
    `);
  });

  test('push to one compound partition does not affect another', () => {
    const sourceContents: SourceContents = {
      parent: [
        {id: 'p1', region: 'us', org: 'acme'},
        {id: 'p2', region: 'us', org: 'wayne'},
      ],
      child: [
        {id: 'c1', region: 'us', org: 'acme', text: 'x'},
        {id: 'c2', region: 'us', org: 'acme', text: 'y'},
        {id: 'c3', region: 'us', org: 'acme', text: 'z'},
      ],
    };
    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        // 4th child in (us,acme) — exceeds cap limit of 3 → dropped.
        [
          'child',
          makeSourceChangeAdd({
            id: 'c4',
            region: 'us',
            org: 'acme',
            text: 'w',
          }),
        ],
        // 1st child in (us,wayne) — independent bucket, accepted.
        [
          'child',
          makeSourceChangeAdd({
            id: 'c5',
            region: 'us',
            org: 'wayne',
            text: 'v',
          }),
        ],
      ],
    });

    // (us,acme) cap still has c1,c2,c3 (c4 dropped; overflow).
    // (us,wayne) cap now has c5.
    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","us","acme"]": {
          "pks": [
            "["c1"]",
            "["c2"]",
            "["c3"]",
          ],
          "size": 3,
        },
        "["cap","us","wayne"]": {
          "pks": [
            "["c5"]",
          ],
          "size": 1,
        },
      }
    `);
  });
});

describe('Cap push - compound primary key', () => {
  // Child PK is ['groupId','seq']. Exercises the multi-element loops in
  // serializePK and deserializePKToConstraint (cap.ts:315-329) — every
  // other test in this file uses single-column PK ['id'], so the
  // multi-element path is otherwise unverified. The PK-point-lookup
  // re-fetch (cap.ts:113-122) deserializes back into a constraint, so
  // a broken round-trip here would either return wrong rows from the
  // source or cause the source to throw on an unrecognized constraint.
  const sources: Sources = {
    parent: {
      columns: {
        id: {type: 'string'},
        groupId: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    child: {
      columns: {
        groupId: {type: 'string'},
        seq: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKeys: ['groupId', 'seq'],
    },
  };

  const ast: AST = {
    table: 'parent',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      related: {
        system: 'client',
        correlation: {parentField: ['groupId'], childField: ['groupId']},
        subquery: {
          table: 'child',
          alias: 'children',
          orderBy: [['seq', 'asc']],
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

  test('initial hydration tracks PKs as JSON-encoded compound arrays', () => {
    const sourceContents: SourceContents = {
      parent: [{id: 'p1', groupId: 'g1'}],
      child: [
        {groupId: 'g1', seq: 's1', text: 'a'},
        {groupId: 'g1', seq: 's2', text: 'b'},
      ],
    };
    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [],
    });

    // Each entry in pks is JSON.stringify of [groupId, seq] in PK order.
    // A bug that serialized only the first PK column would yield
    // duplicate '"g1"' entries instead of distinct compound arrays.
    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","g1"]": {
          "pks": [
            "["g1","s1"]",
            "["g1","s2"]",
          ],
          "size": 2,
        },
      }
    `);
  });

  test('remove + refill round-trips compound PKs through point lookup', () => {
    // Removing s1 forces Cap to refill: it scans the partition, skips
    // pks already in its set (deserialized from compound JSON), and
    // picks the next one. If deserializePKToConstraint built a wrong
    // constraint shape, the refill would either pick a wrong row or
    // re-pick the removed row.
    const sourceContents: SourceContents = {
      parent: [{id: 'p1', groupId: 'g1'}],
      child: [
        {groupId: 'g1', seq: 's1', text: 'a'},
        {groupId: 'g1', seq: 's2', text: 'b'},
        {groupId: 'g1', seq: 's3', text: 'c'},
        {groupId: 'g1', seq: 's4', text: 'd'},
      ],
    };
    const {data, actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'child',
          makeSourceChangeRemove({groupId: 'g1', seq: 's1', text: 'a'}),
        ],
      ],
    });

    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","g1"]": {
          "pks": [
            "["g1","s2"]",
            "["g1","s3"]",
            "["g1","s4"]",
          ],
          "size": 3,
        },
      }
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "children": [
            {
              "groupId": "g1",
              "seq": "s2",
              "text": "b",
              Symbol(rc): 1,
            },
            {
              "groupId": "g1",
              "seq": "s3",
              "text": "c",
              Symbol(rc): 1,
            },
            {
              "groupId": "g1",
              "seq": "s4",
              "text": "d",
              Symbol(rc): 1,
            },
          ],
          "groupId": "g1",
          "id": "p1",
          Symbol(rc): 1,
        },
      ]
    `);
  });

  test('edit that changes a non-leading PK column updates tracked set', () => {
    // The PK is ['groupId','seq']; we keep groupId stable (so the
    // partition assertion passes) but change seq. The new compound PK
    // must replace the old one in the tracked set with the columns in
    // the right order — a bug that swapped column order would emit
    // '["s1_new","g1"]' here.
    const sourceContents: SourceContents = {
      parent: [{id: 'p1', groupId: 'g1'}],
      child: [
        {groupId: 'g1', seq: 's1', text: 'a'},
        {groupId: 'g1', seq: 's2', text: 'b'},
      ],
    };
    const {actualStorage} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'child',
          makeSourceChangeEdit(
            {groupId: 'g1', seq: 's1_new', text: 'a'},
            {groupId: 'g1', seq: 's1', text: 'a'},
          ),
        ],
      ],
    });

    expect(actualStorage['.children:cap']).toMatchInlineSnapshot(`
      {
        "["cap","g1"]": {
          "pks": [
            "["g1","s1_new"]",
            "["g1","s2"]",
          ],
          "size": 2,
        },
      }
    `);
  });
});
