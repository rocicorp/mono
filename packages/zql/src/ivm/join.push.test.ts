import {describe, expect, suite, test} from 'vitest';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/fetch-and-push-tests.ts';
import type {Format} from './view.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';

suite('push one:many', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
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
  } as const;

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
        },
      },
    ],
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      comments: {
        singular: false,
        relationships: {},
      },
    },
  } as const;

  test('fetch one parent, remove parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comments: [],
      },
      ast,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('fetch one child, remove child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      ast,
      format,
      pushes: [['comment', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('fetch one child, add parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch two children, add parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [
          {id: 'c1', issueID: 'i1'},
          {id: 'c2', issueID: 'i1'},
        ],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              Symbol(rc): 1,
            },
            {
              "id": "c2",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": "c2",
                    "issueID": "i1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch one child, add wrong parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i2",
            },
            "type": "add",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i2",
            },
            "type": "add",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i2",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [],
            },
            "row": {
              "id": "i2",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch one parent, add child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
      },
      ast,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch one parent, add wrong child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
      },
      ast,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i2",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i2",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('fetch one parent, one child, remove parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('fetch one parent, two children, remove parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [
          {id: 'c1', issueID: 'i1'},
          {id: 'c2', issueID: 'i1'},
        ],
      },
      ast,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": "c2",
                    "issueID": "i1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('no fetch, add parent, add child, add child, remove child, remove parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [],
      },
      ast,
      format,
      pushes: [
        ['issue', {type: 'add', row: {id: 'i1'}}],
        ['comment', {type: 'add', row: {id: 'c1', issueID: 'i1'}}],
        ['comment', {type: 'add', row: {id: 'c2', issueID: 'i1'}}],
        ['comment', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}],
        ['issue', {type: 'remove', row: {id: 'i1'}}],
      ],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c2",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "row": {
                "id": "c2",
                "issueID": "i1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "remove",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "add",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c2",
                  "issueID": "i1",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                },
              },
              "type": "remove",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c2",
                    "issueID": "i1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  suite('edit', () => {
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
    } as const;

    test('edit issue text', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [{id: 'i1', text: 'issue 1'}],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i1', text: 'comment 2'},
          ],
        },
        ast,
        format,
        pushes: [
          [
            'issue',
            {
              type: 'edit',
              oldRow: {id: 'i1', text: 'issue 1'},
              row: {id: 'i1', text: 'issue 1 edited'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ":source(issue)",
            "push",
            {
              "oldRow": {
                "id": "i1",
                "text": "issue 1",
              },
              "row": {
                "id": "i1",
                "text": "issue 1 edited",
              },
              "type": "edit",
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "oldRow": {
                "id": "i1",
                "text": "issue 1",
              },
              "row": {
                "id": "i1",
                "text": "issue 1 edited",
              },
              "type": "edit",
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1",
                Symbol(rc): 1,
              },
              {
                "id": "c2",
                "issueID": "i1",
                "text": "comment 2",
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            "text": "issue 1 edited",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "oldRow": {
              "id": "i1",
              "text": "issue 1",
            },
            "row": {
              "id": "i1",
              "text": "issue 1 edited",
            },
            "type": "edit",
          },
        ]
      `);
    });

    test('edit comment text', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [{id: 'i1', text: 'issue 1'}],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i1', text: 'comment 2'},
          ],
        },
        ast,
        format,
        pushes: [
          [
            'comment',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
              row: {id: 'c1', issueID: 'i1', text: 'comment 1 edited'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "oldRow": {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1",
              },
              "row": {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1 edited",
              },
              "type": "edit",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "oldRow": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "comment 1",
                },
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "comment 1 edited",
                },
                "type": "edit",
              },
              "row": {
                "id": "i1",
                "text": "issue 1",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1 edited",
                Symbol(rc): 1,
              },
              {
                "id": "c2",
                "issueID": "i1",
                "text": "comment 2",
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            "text": "issue 1",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "oldRow": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "comment 1",
                },
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "comment 1 edited",
                },
                "type": "edit",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i1",
              "text": "issue 1",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('edit issueID of comment', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', text: 'issue 1'},
            {id: 'i2', text: 'issue 2'},
          ],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i1', text: 'comment 2'},
          ],
        },
        ast,
        format,
        pushes: [
          [
            'comment',
            {
              type: 'edit',
              oldRow: {id: 'c1', issueID: 'i1', text: 'comment 1'},
              row: {id: 'c1', issueID: 'i2', text: 'comment 1.2'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1",
              },
              "type": "remove",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                  "text": "comment 1",
                },
                "type": "remove",
              },
              "row": {
                "id": "i1",
                "text": "issue 1",
              },
              "type": "child",
            },
          ],
          [
            ".comments:source(comment)",
            "push",
            {
              "row": {
                "id": "c1",
                "issueID": "i2",
                "text": "comment 1.2",
              },
              "type": "add",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "id": "c1",
                  "issueID": "i2",
                  "text": "comment 1.2",
                },
                "type": "add",
              },
              "row": {
                "id": "i2",
                "text": "issue 2",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "id": "c2",
                "issueID": "i1",
                "text": "comment 2",
                Symbol(rc): 1,
              },
            ],
            "id": "i1",
            "text": "issue 1",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "id": "c1",
                "issueID": "i2",
                "text": "comment 1.2",
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            "text": "issue 2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                    "text": "comment 1",
                  },
                },
                "type": "remove",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i1",
              "text": "issue 1",
            },
            "type": "child",
          },
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "c1",
                    "issueID": "i2",
                    "text": "comment 1.2",
                  },
                },
                "type": "add",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i2",
              "text": "issue 2",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('edit id of issue', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', text: 'issue 1'},
            {id: 'i2', text: 'issue 2'},
          ],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i2', text: 'comment 2'},
            {id: 'c3', issueID: 'i3', text: 'comment 3'},
          ],
        },
        ast,
        format,
        pushes: [
          [
            'issue',
            {
              type: 'edit',
              oldRow: {id: 'i1', text: 'issue 1'},
              row: {id: 'i3', text: 'issue 1.3'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ":source(issue)",
            "push",
            {
              "row": {
                "id": "i1",
                "text": "issue 1",
              },
              "type": "remove",
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "row": {
                "id": "i1",
                "text": "issue 1",
              },
              "type": "remove",
            },
          ],
          [
            ".comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            ":source(issue)",
            "push",
            {
              "row": {
                "id": "i3",
                "text": "issue 1.3",
              },
              "type": "add",
            },
          ],
          [
            ":join(comments)",
            "push",
            {
              "row": {
                "id": "i3",
                "text": "issue 1.3",
              },
              "type": "add",
            },
          ],
          [
            ".comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "comments": [
              {
                "id": "c2",
                "issueID": "i2",
                "text": "comment 2",
                Symbol(rc): 1,
              },
            ],
            "id": "i2",
            "text": "issue 2",
            Symbol(rc): 1,
          },
          {
            "comments": [
              {
                "id": "c3",
                "issueID": "i3",
                "text": "comment 3",
                Symbol(rc): 1,
              },
            ],
            "id": "i3",
            "text": "issue 1.3",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
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
                      "text": "comment 1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "text": "issue 1",
              },
            },
            "type": "remove",
          },
          {
            "node": {
              "relationships": {
                "comments": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "c3",
                      "issueID": "i3",
                      "text": "comment 3",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "text": "issue 1.3",
              },
            },
            "type": "add",
          },
        ]
      `);
    });
  });
});

suite('push many:one', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        ownerID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    user: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['ownerID'], childField: ['id']},
        subquery: {
          table: 'user',
          alias: 'owner',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      owner: {
        singular: true,
        relationships: {},
      },
    },
  } as const;

  test('fetch one child, add parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        user: [{id: 'u1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1', ownerID: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "add",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "add",
          },
        ],
        [
          ".owner:source(user)",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "owner": [
                {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch one child, add wrong parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        user: [{id: 'u1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1', ownerID: 'u2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u2",
            },
            "type": "add",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u2",
            },
            "type": "add",
          },
        ],
        [
          ".owner:source(user)",
          "fetch",
          {
            "constraint": {
              "id": "u2",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "owner": undefined,
          "ownerID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "owner": [],
            },
            "row": {
              "id": "i1",
              "ownerID": "u2",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch one parent, add child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1', ownerID: 'u1'}],
        user: [],
      },
      ast,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner:source(user)",
          "push",
          {
            "row": {
              "id": "u1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
              "type": "add",
            },
            "relationshipName": "owner",
          },
          "row": {
            "id": "i1",
            "ownerID": "u1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch two parents, add one child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
        ],
        user: [],
      },
      ast,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner:source(user)",
          "push",
          {
            "row": {
              "id": "u1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
              },
              "type": "add",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "owner": {
            "id": "u1",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
              "type": "add",
            },
            "relationshipName": "owner",
          },
          "row": {
            "id": "i1",
            "ownerID": "u1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "id": "u1",
                },
              },
              "type": "add",
            },
            "relationshipName": "owner",
          },
          "row": {
            "id": "i2",
            "ownerID": "u1",
          },
          "type": "child",
        },
      ]
    `);
  });

  suite('edit', () => {
    const sources: Sources = {
      issue: {
        columns: {
          id: {type: 'string'},
          ownerID: {type: 'string'},
          text: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      user: {
        columns: {
          id: {type: 'string'},
          text: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    } as const;
    test('edit child to make it match to parents', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u2', text: 'user 2'}],
        },
        ast,
        format,
        pushes: [
          [
            'user',
            {
              type: 'edit',
              row: {id: 'u1', text: 'user 1'},
              oldRow: {id: 'u2', text: 'user 2'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".owner:source(user)",
            "push",
            {
              "row": {
                "id": "u2",
                "text": "user 2",
              },
              "type": "remove",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "ownerID": "u2",
              },
            },
          ],
          [
            ".owner:source(user)",
            "push",
            {
              "row": {
                "id": "u1",
                "text": "user 1",
              },
              "type": "add",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "ownerID": "u1",
              },
            },
          ],
          [
            ":join(owner)",
            "push",
            {
              "child": {
                "row": {
                  "id": "u1",
                  "text": "user 1",
                },
                "type": "add",
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
                "text": "item 1",
              },
              "type": "child",
            },
          ],
          [
            ":join(owner)",
            "push",
            {
              "child": {
                "row": {
                  "id": "u1",
                  "text": "user 1",
                },
                "type": "add",
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "id": "i1",
            "owner": {
              "id": "u1",
              "text": "user 1",
              Symbol(rc): 1,
            },
            "ownerID": "u1",
            "text": "item 1",
            Symbol(rc): 1,
          },
          {
            "id": "i2",
            "owner": {
              "id": "u1",
              "text": "user 1",
              Symbol(rc): 1,
            },
            "ownerID": "u1",
            "text": "item 2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "text": "user 1",
                  },
                },
                "type": "add",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
              "text": "item 1",
            },
            "type": "child",
          },
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "text": "user 1",
                  },
                },
                "type": "add",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
              "text": "item 2",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('edit child to make it match to parents, 1:many:many', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources: {
          user: {
            columns: {
              id: {type: 'string'},
              text: {type: 'string'},
            },
            primaryKeys: ['id'],
          },
          issue: {
            columns: {
              id: {type: 'string'},
              ownerID: {type: 'string'},
              text: {type: 'string'},
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
        },
        sourceContents: {
          user: [
            {id: 'u1', text: 'user 1'},
            {id: 'u2', text: 'user 2'},
          ],
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u2', text: 'item 2'},
          ],
          comment: [
            {id: 'c1', issueID: 'i1'},
            {id: 'c2', issueID: 'i2'},
          ],
        },
        ast: {
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
                related: [
                  {
                    system: 'client',
                    correlation: {parentField: ['id'], childField: ['issueID']},
                    subquery: {
                      table: 'comment',
                      alias: 'comments',
                      orderBy: [['id', 'asc']],
                    },
                  },
                ],
              },
            },
          ],
        },
        format: {
          singular: false,
          relationships: {
            issues: {
              singular: false,
              relationships: {
                comments: {
                  singular: false,
                  relationships: {},
                },
              },
            },
          },
        },
        pushes: [
          [
            'issue',
            {
              type: 'edit',
              row: {id: 'i2', ownerID: 'u1', text: 'item 2'},
              oldRow: {id: 'i2', ownerID: 'u2', text: 'item 2'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".issues:source(issue)",
            "push",
            {
              "row": {
                "id": "i2",
                "ownerID": "u2",
                "text": "item 2",
              },
              "type": "remove",
            },
          ],
          [
            ".issues:join(comments)",
            "push",
            {
              "row": {
                "id": "i2",
                "ownerID": "u2",
                "text": "item 2",
              },
              "type": "remove",
            },
          ],
          [
            ":source(user)",
            "fetch",
            {
              "constraint": {
                "id": "u2",
              },
            },
          ],
          [
            ":join(issues)",
            "push",
            {
              "child": {
                "row": {
                  "id": "i2",
                  "ownerID": "u2",
                  "text": "item 2",
                },
                "type": "remove",
              },
              "row": {
                "id": "u2",
                "text": "user 2",
              },
              "type": "child",
            },
          ],
          [
            ".issues.comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            ".issues:source(issue)",
            "push",
            {
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "add",
            },
          ],
          [
            ".issues:join(comments)",
            "push",
            {
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "add",
            },
          ],
          [
            ":source(user)",
            "fetch",
            {
              "constraint": {
                "id": "u1",
              },
            },
          ],
          [
            ":join(issues)",
            "push",
            {
              "child": {
                "row": {
                  "id": "i2",
                  "ownerID": "u1",
                  "text": "item 2",
                },
                "type": "add",
              },
              "row": {
                "id": "u1",
                "text": "user 1",
              },
              "type": "child",
            },
          ],
          [
            ".issues.comments:source(comment)",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "id": "u1",
            "issues": [
              {
                "comments": [
                  {
                    "id": "c1",
                    "issueID": "i1",
                    Symbol(rc): 1,
                  },
                ],
                "id": "i1",
                "ownerID": "u1",
                "text": "item 1",
                Symbol(rc): 1,
              },
              {
                "comments": [
                  {
                    "id": "c2",
                    "issueID": "i2",
                    Symbol(rc): 1,
                  },
                ],
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
                Symbol(rc): 1,
              },
            ],
            "text": "user 1",
            Symbol(rc): 1,
          },
          {
            "id": "u2",
            "issues": [],
            "text": "user 2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);

      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "id": "c2",
                          "issueID": "i2",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                    "ownerID": "u2",
                    "text": "item 2",
                  },
                },
                "type": "remove",
              },
              "relationshipName": "issues",
            },
            "row": {
              "id": "u2",
              "text": "user 2",
            },
            "type": "child",
          },
          {
            "child": {
              "change": {
                "node": {
                  "relationships": {
                    "comments": [
                      {
                        "relationships": {},
                        "row": {
                          "id": "c2",
                          "issueID": "i2",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "i2",
                    "ownerID": "u1",
                    "text": "item 2",
                  },
                },
                "type": "add",
              },
              "relationshipName": "issues",
            },
            "row": {
              "id": "u1",
              "text": "user 1",
            },
            "type": "child",
          },
        ]
      `);
    });

    test('edit non matching child', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u2', text: 'user 2'}],
        },
        ast,
        format,
        pushes: [
          [
            'user',
            {
              type: 'edit',
              row: {id: 'u2', text: 'user 2 changed'},
              oldRow: {id: 'u2', text: 'user 2'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".owner:source(user)",
            "push",
            {
              "oldRow": {
                "id": "u2",
                "text": "user 2",
              },
              "row": {
                "id": "u2",
                "text": "user 2 changed",
              },
              "type": "edit",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "ownerID": "u2",
              },
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "id": "i1",
            "owner": undefined,
            "ownerID": "u1",
            "text": "item 1",
            Symbol(rc): 1,
          },
          {
            "id": "i2",
            "owner": undefined,
            "ownerID": "u1",
            "text": "item 2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('edit matching child', () => {
      const {log, data, actualStorage, pushes} = runPushTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u1', text: 'user 1'}],
        },
        ast,
        format,
        pushes: [
          [
            'user',
            {
              type: 'edit',
              row: {id: 'u1', text: 'user 1 changed'},
              oldRow: {id: 'u1', text: 'user 1'},
            },
          ],
        ],
      });

      expect(log).toMatchInlineSnapshot(`
        [
          [
            ".owner:source(user)",
            "push",
            {
              "oldRow": {
                "id": "u1",
                "text": "user 1",
              },
              "row": {
                "id": "u1",
                "text": "user 1 changed",
              },
              "type": "edit",
            },
          ],
          [
            ":source(issue)",
            "fetch",
            {
              "constraint": {
                "ownerID": "u1",
              },
            },
          ],
          [
            ":join(owner)",
            "push",
            {
              "child": {
                "oldRow": {
                  "id": "u1",
                  "text": "user 1",
                },
                "row": {
                  "id": "u1",
                  "text": "user 1 changed",
                },
                "type": "edit",
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
                "text": "item 1",
              },
              "type": "child",
            },
          ],
          [
            ":join(owner)",
            "push",
            {
              "child": {
                "oldRow": {
                  "id": "u1",
                  "text": "user 1",
                },
                "row": {
                  "id": "u1",
                  "text": "user 1 changed",
                },
                "type": "edit",
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "child",
            },
          ],
        ]
      `);
      expect(data).toMatchInlineSnapshot(`
        [
          {
            "id": "i1",
            "owner": {
              "id": "u1",
              "text": "user 1 changed",
              Symbol(rc): 1,
            },
            "ownerID": "u1",
            "text": "item 1",
            Symbol(rc): 1,
          },
          {
            "id": "i2",
            "owner": {
              "id": "u1",
              "text": "user 1 changed",
              Symbol(rc): 1,
            },
            "ownerID": "u1",
            "text": "item 2",
            Symbol(rc): 1,
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`{}`);
      expect(pushes).toMatchInlineSnapshot(`
        [
          {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u1",
                  "text": "user 1",
                },
                "row": {
                  "id": "u1",
                  "text": "user 1 changed",
                },
                "type": "edit",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
              "text": "item 1",
            },
            "type": "child",
          },
          {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u1",
                  "text": "user 1",
                },
                "row": {
                  "id": "u1",
                  "text": "user 1 changed",
                },
                "type": "edit",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
              "text": "item 2",
            },
            "type": "child",
          },
        ]
      `);
    });
  });
});

suite('push one:many:many', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
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
    revision: {
      columns: {
        id: {type: 'string'},
        commentID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  } as const;

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
          related: [
            {
              system: 'client',
              correlation: {parentField: ['id'], childField: ['commentID']},
              subquery: {
                table: 'revision',
                alias: 'revisions',
                orderBy: [['id', 'asc']],
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
      comments: {
        singular: false,
        relationships: {
          revisions: {
            singular: false,
            relationships: {},
          },
        },
      },
    },
  } as const;

  test('fetch one parent, one child, add grandchild', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [],
      },
      ast,
      format,
      pushes: [['revision', {type: 'add', row: {id: 'r1', commentID: 'c1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".comments.revisions:source(revision)",
          "push",
          {
            "row": {
              "commentID": "c1",
              "id": "r1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "id": "c1",
            },
          },
        ],
        [
          ".comments:join(revisions)",
          "push",
          {
            "child": {
              "row": {
                "commentID": "c1",
                "id": "r1",
              },
              "type": "add",
            },
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "child": {
                "row": {
                  "commentID": "c1",
                  "id": "r1",
                },
                "type": "add",
              },
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "revisions": [
                {
                  "commentID": "c1",
                  "id": "r1",
                  Symbol(rc): 1,
                },
              ],
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "commentID": "c1",
                      "id": "r1",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "revisions",
              },
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "child",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch one parent, one grandchild, add child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      ast,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".comments:source(comment)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:join(revisions)",
          "push",
          {
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "child": {
              "row": {
                "id": "c1",
                "issueID": "i1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ".comments.revisions:source(revision)",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "revisions": [
                {
                  "commentID": "c1",
                  "id": "r1",
                  Symbol(rc): 1,
                },
              ],
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {
                  "revisions": [
                    {
                      "relationships": {},
                      "row": {
                        "commentID": "c1",
                        "id": "r1",
                      },
                    },
                  ],
                },
                "row": {
                  "id": "c1",
                  "issueID": "i1",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch one child, one grandchild, add parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          ".comments:join(revisions)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments.revisions:source(revision)",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "revisions": [
                {
                  "commentID": "c1",
                  "id": "r1",
                  Symbol(rc): 1,
                },
              ],
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {
                    "revisions": [
                      {
                        "relationships": {},
                        "row": {
                          "commentID": "c1",
                          "id": "r1",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "c1",
                    "issueID": "i1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('fetch one parent, one child, one grandchild, remove parent', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      ast,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":join(comments)",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ".comments:join(revisions)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments:source(comment)",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          ".comments.revisions:source(revision)",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "comments": [
                {
                  "relationships": {
                    "revisions": [
                      {
                        "relationships": {},
                        "row": {
                          "commentID": "c1",
                          "id": "r1",
                        },
                      },
                    ],
                  },
                  "row": {
                    "id": "c1",
                    "issueID": "i1",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });
});

suite('push one:many:one', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    issueLabel: {
      columns: {
        issueID: {type: 'string'},
        labelID: {type: 'string'},
      },
      primaryKeys: ['issueID', 'labelID'],
    },
    label: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'issueLabel',
          alias: 'issueLabels',
          orderBy: [
            ['issueID', 'asc'],
            ['labelID', 'asc'],
          ],
          related: [
            {
              system: 'client',
              correlation: {
                parentField: ['labelID'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
                orderBy: [['id', 'asc']],
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
      issueLabels: {
        singular: false,
        relationships: {
          labels: {
            singular: true,
            relationships: {},
          },
        },
      },
    },
  } as const;

  test('fetch one parent, one child, add grandchild', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        issueLabel: [{issueID: 'i1', labelID: 'l1'}],
        label: [],
      },
      ast,
      format,
      pushes: [['label', {type: 'add', row: {id: 'l1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".issueLabels.labels:source(label)",
          "push",
          {
            "row": {
              "id": "l1",
            },
            "type": "add",
          },
        ],
        [
          ".issueLabels:source(issueLabel)",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          ".issueLabels:join(labels)",
          "push",
          {
            "child": {
              "row": {
                "id": "l1",
              },
              "type": "add",
            },
            "row": {
              "issueID": "i1",
              "labelID": "l1",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(issueLabels)",
          "push",
          {
            "child": {
              "child": {
                "row": {
                  "id": "l1",
                },
                "type": "add",
              },
              "row": {
                "issueID": "i1",
                "labelID": "l1",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "issueLabels": [
            {
              "issueID": "i1",
              "labelID": "l1",
              "labels": {
                "id": "l1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "id": "l1",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "labels",
              },
              "row": {
                "issueID": "i1",
                "labelID": "l1",
              },
              "type": "child",
            },
            "relationshipName": "issueLabels",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch one parent, one grandchild, add child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        issueLabel: [],
        label: [{id: 'l1'}],
      },
      ast,
      format,
      pushes: [
        ['issueLabel', {type: 'add', row: {issueID: 'i1', labelID: 'l1'}}],
      ],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".issueLabels:source(issueLabel)",
          "push",
          {
            "row": {
              "issueID": "i1",
              "labelID": "l1",
            },
            "type": "add",
          },
        ],
        [
          ".issueLabels:join(labels)",
          "push",
          {
            "row": {
              "issueID": "i1",
              "labelID": "l1",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(issueLabels)",
          "push",
          {
            "child": {
              "row": {
                "issueID": "i1",
                "labelID": "l1",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ".issueLabels.labels:source(label)",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "issueLabels": [
            {
              "issueID": "i1",
              "labelID": "l1",
              "labels": {
                "id": "l1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "node": {
                "relationships": {
                  "labels": [
                    {
                      "relationships": {},
                      "row": {
                        "id": "l1",
                      },
                    },
                  ],
                },
                "row": {
                  "issueID": "i1",
                  "labelID": "l1",
                },
              },
              "type": "add",
            },
            "relationshipName": "issueLabels",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('fetch two parents, two children, add one grandchild', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}, {id: 'i2'}],
        issueLabel: [
          {issueID: 'i1', labelID: 'l1'},
          {issueID: 'i2', labelID: 'l1'},
        ],
        label: [],
      },
      ast,
      format,
      pushes: [['label', {type: 'add', row: {id: 'l1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".issueLabels.labels:source(label)",
          "push",
          {
            "row": {
              "id": "l1",
            },
            "type": "add",
          },
        ],
        [
          ".issueLabels:source(issueLabel)",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          ".issueLabels:join(labels)",
          "push",
          {
            "child": {
              "row": {
                "id": "l1",
              },
              "type": "add",
            },
            "row": {
              "issueID": "i1",
              "labelID": "l1",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          ":join(issueLabels)",
          "push",
          {
            "child": {
              "child": {
                "row": {
                  "id": "l1",
                },
                "type": "add",
              },
              "row": {
                "issueID": "i1",
                "labelID": "l1",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
            },
            "type": "child",
          },
        ],
        [
          ".issueLabels:join(labels)",
          "push",
          {
            "child": {
              "row": {
                "id": "l1",
              },
              "type": "add",
            },
            "row": {
              "issueID": "i2",
              "labelID": "l1",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "id": "i2",
            },
          },
        ],
        [
          ":join(issueLabels)",
          "push",
          {
            "child": {
              "child": {
                "row": {
                  "id": "l1",
                },
                "type": "add",
              },
              "row": {
                "issueID": "i2",
                "labelID": "l1",
              },
              "type": "child",
            },
            "row": {
              "id": "i2",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i1",
          "issueLabels": [
            {
              "issueID": "i1",
              "labelID": "l1",
              "labels": {
                "id": "l1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "issueLabels": [
            {
              "issueID": "i2",
              "labelID": "l1",
              "labels": {
                "id": "l1",
                Symbol(rc): 1,
              },
              Symbol(rc): 1,
            },
          ],
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "child": {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "id": "l1",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "labels",
              },
              "row": {
                "issueID": "i1",
                "labelID": "l1",
              },
              "type": "child",
            },
            "relationshipName": "issueLabels",
          },
          "row": {
            "id": "i1",
          },
          "type": "child",
        },
        {
          "child": {
            "change": {
              "child": {
                "change": {
                  "node": {
                    "relationships": {},
                    "row": {
                      "id": "l1",
                    },
                  },
                  "type": "add",
                },
                "relationshipName": "labels",
              },
              "row": {
                "issueID": "i2",
                "labelID": "l1",
              },
              "type": "child",
            },
            "relationshipName": "issueLabels",
          },
          "row": {
            "id": "i2",
          },
          "type": "child",
        },
      ]
    `);
  });
});

describe('edit assignee', () => {
  const sources: Sources = {
    issue: {
      columns: {
        issueID: {type: 'string'},
        text: {type: 'string'},
        assigneeID: {type: 'string', optional: true},
        creatorID: {type: 'string'},
      },
      primaryKeys: ['issueID'],
    },
    user: {
      columns: {
        userID: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['userID'],
    },
  };

  const sourceContents: SourceContents = {
    issue: [
      {
        issueID: 'i1',
        text: 'first issue',
        assigneeID: undefined,
        creatorID: 'u1',
      },
      {
        issueID: 'i2',
        text: 'second issue',
        assigneeID: 'u2',
        creatorID: 'u2',
      },
    ],
    user: [
      {userID: 'u1', name: 'user 1'},
      {userID: 'u2', name: 'user 2'},
    ],
  };

  const ast: AST = {
    table: 'issue',
    orderBy: [['issueID', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['creatorID'], childField: ['userID']},
        subquery: {
          table: 'user',
          alias: 'creator',
          orderBy: [['userID', 'asc']],
        },
      },
      {
        system: 'client',
        correlation: {
          parentField: ['assigneeID'],
          childField: ['userID'],
        },
        subquery: {
          table: 'user',
          alias: 'assignee',
          orderBy: [['userID', 'asc']],
        },
      },
    ],
  };

  const format: Format = {
    singular: false,
    relationships: {
      creator: {
        singular: false,
        relationships: {},
      },
      assignee: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('from none to one', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [
            {
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
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
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });

  test('from none to many', () => {
    const localSources: Sources = {
      ...sources,
      user: {
        columns: {
          userID: {type: 'string'},
          id: {type: 'number'},
          name: {type: 'string'},
        },
        primaryKeys: ['userID', 'id'],
      },
    };

    const localSourceContents = {
      ...sourceContents,
      user: [
        {userID: 'u1', id: 1, name: 'user 1'},
        {userID: 'u1', id: 1.5, name: 'user 1.5'},
        {userID: 'u2', id: 2, name: 'user 2'},
      ],
    };

    const localAst: AST = {
      table: 'issue',
      orderBy: [['issueID', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['creatorID'], childField: ['userID']},
          subquery: {
            table: 'user',
            alias: 'creator',
            orderBy: [
              ['userID', 'asc'],
              ['id', 'asc'],
            ],
          },
        },
        {
          system: 'client',
          correlation: {
            parentField: ['assigneeID'],
            childField: ['userID'],
          },
          subquery: {
            table: 'user',
            alias: 'assignee',
            orderBy: [
              ['userID', 'asc'],
              ['id', 'asc'],
            ],
          },
        },
      ],
    };

    const {log, data, actualStorage, pushes} = runPushTest({
      sources: localSources,
      sourceContents: localSourceContents,
      ast: localAst,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
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
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });

  test('from one to none', () => {
    const {issue, ...rest} = sourceContents;
    const localSourceContents = {
      issue: [{...issue[0], assigneeID: 'u1'}, ...issue.slice(1)],
      ...rest,
    } as const;

    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents: localSourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [],
          "assigneeID": undefined,
          "creator": [
            {
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
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
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });

  test('from many to none', () => {
    let {issue} = sourceContents;
    issue = [{...issue[0], assigneeID: 'u1'}, ...issue.slice(1)];

    const localSources: Sources = {
      issue: sources.issue,
      user: {
        columns: {
          userID: {type: 'string'},
          id: {type: 'number'},
          name: {type: 'string'},
        },
        primaryKeys: ['userID', 'id'],
      },
    };
    const localSourceContents: SourceContents = {
      issue,
      user: [
        {userID: 'u1', id: 1, name: 'user 1'},
        {userID: 'u1', id: 1.5, name: 'user 1.5'},
        {userID: 'u2', id: 2, name: 'user 2'},
      ],
    };
    const localAst: AST = {
      table: 'issue',
      orderBy: [['issueID', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['creatorID'], childField: ['userID']},
          subquery: {
            table: 'user',
            alias: 'creator',
            orderBy: [
              ['userID', 'asc'],
              ['id', 'asc'],
            ],
          },
        },
        {
          system: 'client',
          correlation: {
            parentField: ['assigneeID'],
            childField: ['userID'],
          },
          subquery: {
            table: 'user',
            alias: 'assignee',
            orderBy: [
              ['userID', 'asc'],
              ['id', 'asc'],
            ],
          },
        },
      ],
    };

    const {log, data, actualStorage, pushes} = runPushTest({
      sources: localSources,
      sourceContents: localSourceContents,
      ast: localAst,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: 'u1',
              creatorID: 'u1',
            },
            row: {
              issueID: 'i1',
              text: 'first issue',
              assigneeID: undefined,
              creatorID: 'u1',
            },
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "assignee": [],
          "assigneeID": undefined,
          "creator": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
              Symbol(rc): 1,
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
              Symbol(rc): 1,
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "remove",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ":source(issue)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(creator)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ":join(assignee)",
          "push",
          {
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          ".creator:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          ".assignee:source(user)",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
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
              "assignee": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "assignee": [],
              "creator": [
                {
                  "relationships": {},
                  "row": {
                    "id": 1,
                    "name": "user 1",
                    "userID": "u1",
                  },
                },
                {
                  "relationships": {},
                  "row": {
                    "id": 1.5,
                    "name": "user 1.5",
                    "userID": "u1",
                  },
                },
              ],
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });
});

describe('joins with compound join keys', () => {
  const sources: Sources = {
    a: {
      columns: {
        id: {type: 'number'},
        a1: {type: 'number'},
        a2: {type: 'number'},
        a3: {type: 'number'},
      },
      primaryKeys: ['id'],
    },
    b: {
      columns: {
        id: {type: 'number'},
        b1: {type: 'number'},
        b2: {type: 'number'},
        b3: {type: 'number'},
      },
      primaryKeys: ['id'],
    },
  };

  const sourceContents: SourceContents = {
    a: [
      {id: 0, a1: 1, a2: 2, a3: 3},
      {id: 1, a1: 4, a2: 5, a3: 6},
    ],
    b: [
      {id: 0, b1: 2, b2: 1, b3: 3},
      {id: 1, b1: 5, b2: 4, b3: 6},
    ],
  };

  const ast: AST = {
    table: 'a',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['a1', 'a2'], childField: ['b2', 'b1']},
        subquery: {
          table: 'b',
          alias: 'ab',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      ab: {
        singular: false,
        relationships: {},
      },
    },
  };

  test('add parent and child', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'a',
          {
            type: 'add',
            row: {id: 2, a1: 7, a2: 8, a3: 9},
          },
        ],
        [
          'b',
          {
            type: 'add',
            row: {id: 2, b1: 8, b2: 7, b3: 9},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "a1": 1,
          "a2": 2,
          "a3": 3,
          "ab": [
            {
              "b1": 2,
              "b2": 1,
              "b3": 3,
              "id": 0,
              Symbol(rc): 1,
            },
          ],
          "id": 0,
          Symbol(rc): 1,
        },
        {
          "a1": 4,
          "a2": 5,
          "a3": 6,
          "ab": [
            {
              "b1": 5,
              "b2": 4,
              "b3": 6,
              "id": 1,
              Symbol(rc): 1,
            },
          ],
          "id": 1,
          Symbol(rc): 1,
        },
        {
          "a1": 7,
          "a2": 8,
          "a3": 9,
          "ab": [
            {
              "b1": 8,
              "b2": 7,
              "b3": 9,
              "id": 2,
              Symbol(rc): 1,
            },
          ],
          "id": 2,
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(a)",
          "push",
          {
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          ":join(ab)",
          "push",
          {
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          ".ab:source(b)",
          "fetch",
          {
            "constraint": {
              "b1": 8,
              "b2": 7,
            },
          },
        ],
        [
          ".ab:source(b)",
          "push",
          {
            "row": {
              "b1": 8,
              "b2": 7,
              "b3": 9,
              "id": 2,
            },
            "type": "add",
          },
        ],
        [
          ":source(a)",
          "fetch",
          {
            "constraint": {
              "a1": 7,
              "a2": 8,
            },
          },
        ],
        [
          ":join(ab)",
          "push",
          {
            "child": {
              "row": {
                "b1": 8,
                "b2": 7,
                "b3": 9,
                "id": 2,
              },
              "type": "add",
            },
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
            "type": "child",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "ab": [],
            },
            "row": {
              "a1": 7,
              "a2": 8,
              "a3": 9,
              "id": 2,
            },
          },
          "type": "add",
        },
        {
          "child": {
            "change": {
              "node": {
                "relationships": {},
                "row": {
                  "b1": 8,
                  "b2": 7,
                  "b3": 9,
                  "id": 2,
                },
              },
              "type": "add",
            },
            "relationshipName": "ab",
          },
          "row": {
            "a1": 7,
            "a2": 8,
            "a3": 9,
            "id": 2,
          },
          "type": "child",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });

  test('edit child with moving it', () => {
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'a',
          {
            type: 'edit',
            oldRow: {id: 0, a1: 1, a2: 2, a3: 3},
            row: {id: 0, a1: 1, a2: 2, a3: 33},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "a1": 1,
          "a2": 2,
          "a3": 33,
          "ab": [
            {
              "b1": 2,
              "b2": 1,
              "b3": 3,
              "id": 0,
              Symbol(rc): 1,
            },
          ],
          "id": 0,
          Symbol(rc): 1,
        },
        {
          "a1": 4,
          "a2": 5,
          "a3": 6,
          "ab": [
            {
              "b1": 5,
              "b2": 4,
              "b3": 6,
              "id": 1,
              Symbol(rc): 1,
            },
          ],
          "id": 1,
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ":source(a)",
          "push",
          {
            "oldRow": {
              "a1": 1,
              "a2": 2,
              "a3": 3,
              "id": 0,
            },
            "row": {
              "a1": 1,
              "a2": 2,
              "a3": 33,
              "id": 0,
            },
            "type": "edit",
          },
        ],
        [
          ":join(ab)",
          "push",
          {
            "oldRow": {
              "a1": 1,
              "a2": 2,
              "a3": 3,
              "id": 0,
            },
            "row": {
              "a1": 1,
              "a2": 2,
              "a3": 33,
              "id": 0,
            },
            "type": "edit",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "oldRow": {
            "a1": 1,
            "a2": 2,
            "a3": 3,
            "id": 0,
          },
          "row": {
            "a1": 1,
            "a2": 2,
            "a3": 33,
            "id": 0,
          },
          "type": "edit",
        },
      ]
    `);

    expect(actualStorage).toMatchInlineSnapshot(`{}`);
  });
});

suite('test overlay on many:one pushes', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        ownerID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    user: {
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['ownerID'], childField: ['id']},
        subquery: {
          table: 'user',
          alias: 'owner',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      owner: {
        singular: true,
        relationships: {},
      },
    },
  } as const;

  test('add child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerID: 'u0'},
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
          {id: 'i3', ownerID: 'u2'},
        ],
        user: [
          {id: 'u0', name: 'Fritz'},
          {id: 'u2', name: 'Arv'},
        ],
      },
      ast,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u1', name: 'Aaron'}}]],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner:source(user)",
          "push",
          {
            "row": {
              "id": "u1",
              "name": "Aaron",
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
                "name": "Aaron",
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
                "name": "Aaron",
              },
              "type": "add",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "owner": {
            "id": "u0",
            "name": "Fritz",
            Symbol(rc): 1,
          },
          "ownerID": "u0",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            "name": "Aaron",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "owner": {
            "id": "u1",
            "name": "Aaron",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "owner": {
            "id": "u2",
            "name": "Arv",
            Symbol(rc): 1,
          },
          "ownerID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "name": "Aaron",
                  },
                },
                "type": "add",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "name": "Aaron",
                  },
                },
                "type": "add",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
      ]
    `);
  });

  test('remove child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerID: 'u0'},
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
          {id: 'i3', ownerID: 'u2'},
        ],
        user: [
          {id: 'u0', name: 'Fritz'},
          {id: 'u1', name: 'Aaron'},
          {id: 'u2', name: 'Arv'},
        ],
      },
      ast,
      format,
      pushes: [['user', {type: 'remove', row: {id: 'u1', name: 'Aaron'}}]],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner:source(user)",
          "push",
          {
            "row": {
              "id": "u1",
              "name": "Aaron",
            },
            "type": "remove",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
                "name": "Aaron",
              },
              "type": "remove",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "row": {
                "id": "u1",
                "name": "Aaron",
              },
              "type": "remove",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "owner": {
            "id": "u0",
            "name": "Fritz",
            Symbol(rc): 1,
          },
          "ownerID": "u0",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "owner": undefined,
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "owner": undefined,
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "owner": {
            "id": "u2",
            "name": "Arv",
            Symbol(rc): 1,
          },
          "ownerID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "name": "Aaron",
                  },
                },
                "type": "remove",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u1",
                    "name": "Aaron",
                  },
                },
                "type": "remove",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
      ]
    `);
  });

  test('edit child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerID: 'u0'},
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
          {id: 'i3', ownerID: 'u2'},
        ],
        user: [
          {id: 'u0', name: 'Fritz'},
          {id: 'u1', name: 'Aaron'},
          {id: 'u2', name: 'Arv'},
        ],
      },
      ast,
      format,
      pushes: [
        [
          'user',
          {
            type: 'edit',
            oldRow: {id: 'u1', name: 'Aaron'},
            row: {id: 'u1', name: 'Boogs'},
          },
        ],
      ],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner:source(user)",
          "push",
          {
            "oldRow": {
              "id": "u1",
              "name": "Aaron",
            },
            "row": {
              "id": "u1",
              "name": "Boogs",
            },
            "type": "edit",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "u1",
                "name": "Aaron",
              },
              "row": {
                "id": "u1",
                "name": "Boogs",
              },
              "type": "edit",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "u1",
                "name": "Aaron",
              },
              "row": {
                "id": "u1",
                "name": "Boogs",
              },
              "type": "edit",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "owner": {
            "id": "u0",
            "name": "Fritz",
            Symbol(rc): 1,
          },
          "ownerID": "u0",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            "name": "Boogs",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "owner": {
            "id": "u1",
            "name": "Boogs",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "owner": {
            "id": "u2",
            "name": "Arv",
            Symbol(rc): 1,
          },
          "ownerID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u1",
                  "name": "Aaron",
                },
                "row": {
                  "id": "u1",
                  "name": "Boogs",
                },
                "type": "edit",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Boogs",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u1",
                  "name": "Aaron",
                },
                "row": {
                  "id": "u1",
                  "name": "Boogs",
                },
                "type": "edit",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Boogs",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Boogs",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
      ]
    `);
  });

  test('edit grandchild', () => {
    const sources: Sources = {
      issue: {
        columns: {
          id: {type: 'string'},
          ownerID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      user: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
          stateID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      state: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    } as const;

    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['ownerID'], childField: ['id']},
          subquery: {
            table: 'user',
            alias: 'owner',
            orderBy: [['id', 'asc']],
            related: [
              {
                system: 'client',
                correlation: {parentField: ['stateID'], childField: ['id']},
                subquery: {
                  table: 'state',
                  alias: 'state',
                  orderBy: [['id', 'asc']],
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
        owner: {
          singular: true,
          relationships: {
            state: {
              singular: true,
              relationships: {},
            },
          },
        },
      },
    } as const;
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerID: 'u0'},
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
          {id: 'i3', ownerID: 'u2'},
        ],
        user: [
          {id: 'u0', name: 'Fritz', stateID: 's0'},
          {id: 'u1', name: 'Aaron', stateID: 's0'},
          {id: 'u2', name: 'Arv', stateID: 's1'},
        ],
        state: [
          {id: 's0', name: 'Hawaii'},
          {id: 's1', name: 'CA'},
        ],
      },
      ast,
      format,
      pushes: [
        [
          'state',
          {
            type: 'edit',
            oldRow: {id: 's0', name: 'Hawaii'},
            row: {id: 's0', name: 'HI'},
          },
        ],
      ],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".owner.state:source(state)",
          "push",
          {
            "oldRow": {
              "id": "s0",
              "name": "Hawaii",
            },
            "row": {
              "id": "s0",
              "name": "HI",
            },
            "type": "edit",
          },
        ],
        [
          ".owner:source(user)",
          "fetch",
          {
            "constraint": {
              "stateID": "s0",
            },
          },
        ],
        [
          ".owner:join(state)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "s0",
                "name": "Hawaii",
              },
              "row": {
                "id": "s0",
                "name": "HI",
              },
              "type": "edit",
            },
            "row": {
              "id": "u0",
              "name": "Fritz",
              "stateID": "s0",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u0",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u0",
                "name": "Fritz",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i0",
              "ownerID": "u0",
            },
            "type": "child",
          },
        ],
        [
          ".owner:join(state)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "s0",
                "name": "Hawaii",
              },
              "row": {
                "id": "s0",
                "name": "HI",
              },
              "type": "edit",
            },
            "row": {
              "id": "u1",
              "name": "Aaron",
              "stateID": "s0",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u1",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
        [
          ":join(owner)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u1",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "owner": {
            "id": "u0",
            "name": "Fritz",
            "state": {
              "id": "s0",
              "name": "HI",
              Symbol(rc): 1,
            },
            "stateID": "s0",
            Symbol(rc): 1,
          },
          "ownerID": "u0",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "owner": {
            "id": "u1",
            "name": "Aaron",
            "state": {
              "id": "s0",
              "name": "HI",
              Symbol(rc): 1,
            },
            "stateID": "s0",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "owner": {
            "id": "u1",
            "name": "Aaron",
            "state": {
              "id": "s0",
              "name": "HI",
              Symbol(rc): 1,
            },
            "stateID": "s0",
            Symbol(rc): 1,
          },
          "ownerID": "u1",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "owner": {
            "id": "u2",
            "name": "Arv",
            "state": {
              "id": "s1",
              "name": "CA",
              Symbol(rc): 1,
            },
            "stateID": "s1",
            Symbol(rc): 1,
          },
          "ownerID": "u2",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u0",
                  "name": "Fritz",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i0",
              "ownerID": "u0",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u1",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u1",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "owner",
            },
            "row": {
              "id": "i2",
              "ownerID": "u1",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerID": "u0",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
              },
            },
            {
              "relationships": {
                "owner": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerID": "u2",
              },
            },
          ],
        },
      ]
    `);
  });
});

suite('test overlay on many:many (no junction) pushes', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
        ownerName: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    user: {
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
        num: {type: 'number'},
      },
      primaryKeys: ['id'],
    },
  } as const;

  const ast: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['ownerName'], childField: ['name']},
        subquery: {
          table: 'user',
          alias: 'ownerByName',
          orderBy: [
            ['num', 'asc'],
            ['id', 'asc'],
          ],
        },
      },
    ],
  } as const;

  const format: Format = {
    singular: false,
    relationships: {
      ownerByName: {
        singular: false,
        relationships: {},
      },
    },
  } as const;

  test('add child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerName: 'Fritz'},
          {id: 'i1', ownerName: 'Aaron'},
          {id: 'i2', ownerName: 'Aaron'},
          {id: 'i3', ownerName: 'Arv'},
        ],
        user: [
          {id: 'u0', name: 'Fritz', num: 0},
          {id: 'u1', name: 'Aaron', num: 1},
          {id: 'u3', name: 'Aaron', num: 3},
          {id: 'u4', name: 'Arv', num: 4},
        ],
      },
      ast,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u2', name: 'Aaron', num: 2}}]],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".ownerByName:source(user)",
          "push",
          {
            "row": {
              "id": "u2",
              "name": "Aaron",
              "num": 2,
            },
            "type": "add",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerName": "Aaron",
            },
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "type": "add",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "type": "add",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "ownerByName": [
            {
              "id": "u0",
              "name": "Fritz",
              "num": 0,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Fritz",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "num": 2,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "num": 2,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "ownerByName": [
            {
              "id": "u4",
              "name": "Arv",
              "num": 4,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Arv",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u2",
                    "name": "Aaron",
                    "num": 2,
                  },
                },
                "type": "add",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 2,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u2",
                    "name": "Aaron",
                    "num": 2,
                  },
                },
                "type": "add",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 2,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 2,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
      ]
    `);
  });

  test('remove child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerName: 'Fritz'},
          {id: 'i1', ownerName: 'Aaron'},
          {id: 'i2', ownerName: 'Aaron'},
          {id: 'i3', ownerName: 'Arv'},
        ],
        user: [
          {id: 'u0', name: 'Fritz', num: 0},
          {id: 'u1', name: 'Aaron', num: 1},
          {id: 'u2', name: 'Aaron', num: 2},
          {id: 'u3', name: 'Aaron', num: 3},
          {id: 'u4', name: 'Arv', num: 4},
        ],
      },
      ast,
      format,
      pushes: [
        ['user', {type: 'remove', row: {id: 'u2', name: 'Aaron', num: 2}}],
      ],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".ownerByName:source(user)",
          "push",
          {
            "row": {
              "id": "u2",
              "name": "Aaron",
              "num": 2,
            },
            "type": "remove",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerName": "Aaron",
            },
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "type": "remove",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "type": "remove",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "ownerByName": [
            {
              "id": "u0",
              "name": "Fritz",
              "num": 0,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Fritz",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "ownerByName": [
            {
              "id": "u4",
              "name": "Arv",
              "num": 4,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Arv",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u2",
                    "name": "Aaron",
                    "num": 2,
                  },
                },
                "type": "remove",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 2,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "node": {
                  "relationships": {},
                  "row": {
                    "id": "u2",
                    "name": "Aaron",
                    "num": 2,
                  },
                },
                "type": "remove",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
      ]
    `);
  });

  test('edit child', () => {
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerName: 'Fritz'},
          {id: 'i1', ownerName: 'Aaron'},
          {id: 'i2', ownerName: 'Aaron'},
          {id: 'i3', ownerName: 'Arv'},
        ],
        user: [
          {id: 'u0', name: 'Fritz', num: 0},
          {id: 'u1', name: 'Aaron', num: 1},
          {id: 'u2', name: 'Aaron', num: 2},
          {id: 'u3', name: 'Aaron', num: 3},
          {id: 'u4', name: 'Arv', num: 4},
        ],
      },
      ast,
      format,
      pushes: [
        [
          'user',
          {
            type: 'edit',
            oldRow: {id: 'u2', name: 'Aaron', num: 2},
            row: {id: 'u2', name: 'Aaron', num: 5},
          },
        ],
      ],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".ownerByName:source(user)",
          "push",
          {
            "oldRow": {
              "id": "u2",
              "name": "Aaron",
              "num": 2,
            },
            "row": {
              "id": "u2",
              "name": "Aaron",
              "num": 5,
            },
            "type": "edit",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerName": "Aaron",
            },
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 5,
              },
              "type": "edit",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "u2",
                "name": "Aaron",
                "num": 2,
              },
              "row": {
                "id": "u2",
                "name": "Aaron",
                "num": 5,
              },
              "type": "edit",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "ownerByName": [
            {
              "id": "u0",
              "name": "Fritz",
              "num": 0,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Fritz",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "num": 5,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "num": 1,
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "num": 3,
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "num": 5,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "ownerByName": [
            {
              "id": "u4",
              "name": "Arv",
              "num": 4,
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Arv",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u2",
                  "name": "Aaron",
                  "num": 2,
                },
                "row": {
                  "id": "u2",
                  "name": "Aaron",
                  "num": 5,
                },
                "type": "edit",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 5,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 2,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "oldRow": {
                  "id": "u2",
                  "name": "Aaron",
                  "num": 2,
                },
                "row": {
                  "id": "u2",
                  "name": "Aaron",
                  "num": 5,
                },
                "type": "edit",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "num": 0,
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 5,
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "num": 1,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "num": 3,
                    },
                  },
                  {
                    "relationships": {},
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "num": 5,
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {},
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "num": 4,
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
      ]
    `);
  });

  test('edit grandchild', () => {
    const sources: Sources = {
      issue: {
        columns: {
          id: {type: 'string'},
          ownerName: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      user: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
          stateID: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
      state: {
        columns: {
          id: {type: 'string'},
          name: {type: 'string'},
        },
        primaryKeys: ['id'],
      },
    } as const;

    const ast: AST = {
      table: 'issue',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['ownerName'], childField: ['name']},
          subquery: {
            table: 'user',
            alias: 'ownerByName',
            orderBy: [['id', 'asc']],
            related: [
              {
                system: 'client',
                correlation: {parentField: ['stateID'], childField: ['id']},
                subquery: {
                  table: 'state',
                  alias: 'state',
                  orderBy: [['id', 'asc']],
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
        ownerByName: {
          singular: false,
          relationships: {
            state: {
              singular: true,
              relationships: {},
            },
          },
        },
      },
    } as const;
    const {log, data, actualStorage, pushesWithFetch} = runPushTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i0', ownerName: 'Fritz'},
          {id: 'i1', ownerName: 'Aaron'},
          {id: 'i2', ownerName: 'Aaron'},
          {id: 'i3', ownerName: 'Arv'},
        ],
        user: [
          {id: 'u0', name: 'Fritz', stateID: 's1'},
          {id: 'u1', name: 'Aaron', stateID: 's1'},
          {id: 'u2', name: 'Aaron', stateID: 's0'},
          {id: 'u3', name: 'Aaron', stateID: 's0'},
          {id: 'u4', name: 'Arv', stateID: 's1'},
        ],
        state: [
          {id: 's0', name: 'Hawaii'},
          {id: 's1', name: 'CA'},
        ],
      },
      ast,
      format,
      pushes: [
        [
          'state',
          {
            type: 'edit',
            oldRow: {id: 's0', name: 'Hawaii'},
            row: {id: 's0', name: 'HI'},
          },
        ],
      ],
      fetchOnPush: true,
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          ".ownerByName.state:source(state)",
          "push",
          {
            "oldRow": {
              "id": "s0",
              "name": "Hawaii",
            },
            "row": {
              "id": "s0",
              "name": "HI",
            },
            "type": "edit",
          },
        ],
        [
          ".ownerByName:source(user)",
          "fetch",
          {
            "constraint": {
              "stateID": "s0",
            },
          },
        ],
        [
          ".ownerByName:join(state)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "s0",
                "name": "Hawaii",
              },
              "row": {
                "id": "s0",
                "name": "HI",
              },
              "type": "edit",
            },
            "row": {
              "id": "u2",
              "name": "Aaron",
              "stateID": "s0",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerName": "Aaron",
            },
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u2",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u2",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ".ownerByName:join(state)",
          "push",
          {
            "child": {
              "oldRow": {
                "id": "s0",
                "name": "Hawaii",
              },
              "row": {
                "id": "s0",
                "name": "HI",
              },
              "type": "edit",
            },
            "row": {
              "id": "u3",
              "name": "Aaron",
              "stateID": "s0",
            },
            "type": "child",
          },
        ],
        [
          ":source(issue)",
          "fetch",
          {
            "constraint": {
              "ownerName": "Aaron",
            },
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u3",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
        [
          ":join(ownerByName)",
          "push",
          {
            "child": {
              "child": {
                "oldRow": {
                  "id": "s0",
                  "name": "Hawaii",
                },
                "row": {
                  "id": "s0",
                  "name": "HI",
                },
                "type": "edit",
              },
              "row": {
                "id": "u3",
                "name": "Aaron",
                "stateID": "s0",
              },
              "type": "child",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "i0",
          "ownerByName": [
            {
              "id": "u0",
              "name": "Fritz",
              "state": {
                "id": "s1",
                "name": "CA",
                Symbol(rc): 1,
              },
              "stateID": "s1",
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Fritz",
          Symbol(rc): 1,
        },
        {
          "id": "i1",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "state": {
                "id": "s1",
                "name": "CA",
                Symbol(rc): 1,
              },
              "stateID": "s1",
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "state": {
                "id": "s0",
                "name": "HI",
                Symbol(rc): 1,
              },
              "stateID": "s0",
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "state": {
                "id": "s0",
                "name": "HI",
                Symbol(rc): 1,
              },
              "stateID": "s0",
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i2",
          "ownerByName": [
            {
              "id": "u1",
              "name": "Aaron",
              "state": {
                "id": "s1",
                "name": "CA",
                Symbol(rc): 1,
              },
              "stateID": "s1",
              Symbol(rc): 1,
            },
            {
              "id": "u2",
              "name": "Aaron",
              "state": {
                "id": "s0",
                "name": "HI",
                Symbol(rc): 1,
              },
              "stateID": "s0",
              Symbol(rc): 1,
            },
            {
              "id": "u3",
              "name": "Aaron",
              "state": {
                "id": "s0",
                "name": "HI",
                Symbol(rc): 1,
              },
              "stateID": "s0",
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Aaron",
          Symbol(rc): 1,
        },
        {
          "id": "i3",
          "ownerByName": [
            {
              "id": "u4",
              "name": "Arv",
              "state": {
                "id": "s1",
                "name": "CA",
                Symbol(rc): 1,
              },
              "stateID": "s1",
              Symbol(rc): 1,
            },
          ],
          "ownerName": "Arv",
          Symbol(rc): 1,
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`{}`);
    expect(pushesWithFetch).toMatchInlineSnapshot(`
      [
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u2",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u2",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u3",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i1",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "Hawaii",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
        {
          "change": {
            "child": {
              "change": {
                "child": {
                  "change": {
                    "oldRow": {
                      "id": "s0",
                      "name": "Hawaii",
                    },
                    "row": {
                      "id": "s0",
                      "name": "HI",
                    },
                    "type": "edit",
                  },
                  "relationshipName": "state",
                },
                "row": {
                  "id": "u3",
                  "name": "Aaron",
                  "stateID": "s0",
                },
                "type": "child",
              },
              "relationshipName": "ownerByName",
            },
            "row": {
              "id": "i2",
              "ownerName": "Aaron",
            },
            "type": "child",
          },
          "fetch": [
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u0",
                      "name": "Fritz",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i0",
                "ownerName": "Fritz",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i1",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u1",
                      "name": "Aaron",
                      "stateID": "s1",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u2",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s0",
                            "name": "HI",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u3",
                      "name": "Aaron",
                      "stateID": "s0",
                    },
                  },
                ],
              },
              "row": {
                "id": "i2",
                "ownerName": "Aaron",
              },
            },
            {
              "relationships": {
                "ownerByName": [
                  {
                    "relationships": {
                      "state": [
                        {
                          "relationships": {},
                          "row": {
                            "id": "s1",
                            "name": "CA",
                          },
                        },
                      ],
                    },
                    "row": {
                      "id": "u4",
                      "name": "Arv",
                      "stateID": "s1",
                    },
                  },
                ],
              },
              "row": {
                "id": "i3",
                "ownerName": "Arv",
              },
            },
          ],
        },
      ]
    `);
  });
});
