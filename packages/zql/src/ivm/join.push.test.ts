import {describe, expect, suite, test} from 'vitest';
import {
  runJoinTest,
  type Joins,
  type SourceContents,
  type Sources,
} from './test/join-push-tests.js';
import type {Format} from './view.js';

suite('push one:many', () => {
  const sources: Sources = {
    issue: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
  } as const;

  const joins: Joins = {
    comments: {
      parentKey: ['id'],
      parentSource: 'issue',
      childKey: ['issueID'],
      childSource: 'comment',
      relationshipName: 'comments',
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
  } as const;

  test('fetch one parent, remove parent', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comments: [],
      },
      joins,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "comment",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      joins,
      format,
      pushes: [['comment', {type: 'remove', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "comment",
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
          "issue",
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
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
      }
    `);
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('fetch one child, add parent', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "comment",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
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
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
      }
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [
          {id: 'c1', issueID: 'i1'},
          {id: 'c2', issueID: 'i1'},
        ],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "comment",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
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
            },
            {
              "id": "c2",
              "issueID": "i1",
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
      }
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i2",
            },
            "type": "add",
          },
        ],
        [
          "comment",
          "fetch",
          {
            "constraint": {
              "issueID": "i2",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i2",
            },
            "type": "add",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i2","i2",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
      },
      joins,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "comment",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
      }
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
      },
      joins,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "comment",
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
          "issue",
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
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
      }
    `);
    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('fetch one parent, one child, remove parent', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "comment",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
      }
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [
          {id: 'c1', issueID: 'i1'},
          {id: 'c2', issueID: 'i1'},
        ],
      },
      joins,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "comment",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
      }
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [],
      },
      joins,
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
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "comment",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "comment",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
          "comment",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
          "comment",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "comment",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
      }
    `);
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
        sorts: [['id', 'asc']],
      },
      comment: {
        columns: {
          id: {type: 'string'},
          issueID: {type: 'string'},
          text: {type: 'string'},
        },
        primaryKeys: ['id'],
        sorts: [['id', 'asc']],
      },
    } as const;

    const joins: Joins = {
      comments: {
        parentKey: ['id'],
        parentSource: 'issue',
        childKey: ['issueID'],
        childSource: 'comment',
        relationshipName: 'comments',
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
    } as const;

    test('edit issue text', () => {
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources,
        sourceContents: {
          issue: [{id: 'i1', text: 'issue 1'}],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i1', text: 'comment 2'},
          ],
        },
        joins,
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
            "issue",
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
            "comment",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "comment",
            "fetch",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "comments",
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
              },
              {
                "id": "c2",
                "issueID": "i1",
                "text": "comment 2",
              },
            ],
            "id": "i1",
            "text": "issue 1 edited",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","i1","i1",": true,
          },
        }
      `);
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
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources,
        sourceContents: {
          issue: [{id: 'i1', text: 'issue 1'}],
          comment: [
            {id: 'c1', issueID: 'i1', text: 'comment 1'},
            {id: 'c2', issueID: 'i1', text: 'comment 2'},
          ],
        },
        joins,
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
            "comment",
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
            "issue",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "comments",
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
              },
              {
                "id": "c2",
                "issueID": "i1",
                "text": "comment 2",
              },
            ],
            "id": "i1",
            "text": "issue 1",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","i1","i1",": true,
          },
        }
      `);
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
      const {log, data, actualStorage, pushes} = runJoinTest({
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
        joins,
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
            "comment",
            "push",
            {
              "oldRow": {
                "id": "c1",
                "issueID": "i1",
                "text": "comment 1",
              },
              "row": {
                "id": "c1",
                "issueID": "i2",
                "text": "comment 1.2",
              },
              "type": "edit",
            },
          ],
          [
            "issue",
            "fetch",
            {
              "constraint": {
                "id": "i1",
              },
            },
          ],
          [
            "comments",
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
            "issue",
            "fetch",
            {
              "constraint": {
                "id": "i2",
              },
            },
          ],
          [
            "comments",
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
              },
            ],
            "id": "i1",
            "text": "issue 1",
          },
          {
            "comments": [
              {
                "id": "c1",
                "issueID": "i2",
                "text": "comment 1.2",
              },
            ],
            "id": "i2",
            "text": "issue 2",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","i1","i1",": true,
            ""pKeySet","i2","i2",": true,
          },
        }
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
      const {log, data, actualStorage, pushes} = runJoinTest({
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
        joins,
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
            "issue",
            "push",
            {
              "oldRow": {
                "id": "i1",
                "text": "issue 1",
              },
              "row": {
                "id": "i3",
                "text": "issue 1.3",
              },
              "type": "edit",
            },
          ],
          [
            "comment",
            "cleanup",
            {
              "constraint": {
                "issueID": "i1",
              },
            },
          ],
          [
            "comments",
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
            "comment",
            "fetch",
            {
              "constraint": {
                "issueID": "i3",
              },
            },
          ],
          [
            "comments",
            "push",
            {
              "row": {
                "id": "i3",
                "text": "issue 1.3",
              },
              "type": "add",
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
              },
            ],
            "id": "i2",
            "text": "issue 2",
          },
          {
            "comments": [
              {
                "id": "c3",
                "issueID": "i3",
                "text": "comment 3",
              },
            ],
            "id": "i3",
            "text": "issue 1.3",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","i2","i2",": true,
            ""pKeySet","i3","i3",": true,
          },
        }
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
      sorts: [['id', 'asc']],
    },
    user: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
  } as const;

  const joins: Joins = {
    comments: {
      parentKey: ['ownerID'],
      parentSource: 'issue',
      childKey: ['id'],
      childSource: 'user',
      relationshipName: 'owner',
    },
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        user: [{id: 'u1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1', ownerID: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
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
          "user",
          "fetch",
          {
            "constraint": {
              "id": "u1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u1",
            },
            "type": "add",
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
          },
          "ownerID": "u1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","u1","i1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        user: [{id: 'u1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1', ownerID: 'u2'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
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
          "user",
          "fetch",
          {
            "constraint": {
              "id": "u2",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
              "ownerID": "u2",
            },
            "type": "add",
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
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","u2","i1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1', ownerID: 'u1'}],
        user: [],
      },
      joins,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "user",
          "push",
          {
            "row": {
              "id": "u1",
            },
            "type": "add",
          },
        ],
        [
          "issue",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          "comments",
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
          },
          "ownerID": "u1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","u1","i1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [
          {id: 'i1', ownerID: 'u1'},
          {id: 'i2', ownerID: 'u1'},
        ],
        user: [],
      },
      joins,
      format,
      pushes: [['user', {type: 'add', row: {id: 'u1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "user",
          "push",
          {
            "row": {
              "id": "u1",
            },
            "type": "add",
          },
        ],
        [
          "issue",
          "fetch",
          {
            "constraint": {
              "ownerID": "u1",
            },
          },
        ],
        [
          "comments",
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
          "comments",
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
          },
          "ownerID": "u1",
        },
        {
          "id": "i2",
          "owner": {
            "id": "u1",
          },
          "ownerID": "u1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u1","i2",": true,
        },
      }
    `);
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
        sorts: [['id', 'asc']],
      },
      user: {
        columns: {
          id: {type: 'string'},
          text: {type: 'string'},
        },
        primaryKeys: ['id'],
        sorts: [['id', 'asc']],
      },
    } as const;

    const joins: Joins = {
      comments: {
        parentKey: ['ownerID'],
        parentSource: 'issue',
        childKey: ['id'],
        childSource: 'user',
        relationshipName: 'owner',
      },
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

    test('edit child to make it match to parents', () => {
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u2', text: 'user 2'}],
        },
        joins,
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
            "user",
            "push",
            {
              "oldRow": {
                "id": "u2",
                "text": "user 2",
              },
              "row": {
                "id": "u1",
                "text": "user 1",
              },
              "type": "edit",
            },
          ],
          [
            "issue",
            "fetch",
            {
              "constraint": {
                "ownerID": "u2",
              },
            },
          ],
          [
            "issue",
            "fetch",
            {
              "constraint": {
                "ownerID": "u1",
              },
            },
          ],
          [
            "comments",
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
            "comments",
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
            },
            "ownerID": "u1",
            "text": "item 1",
          },
          {
            "id": "i2",
            "owner": {
              "id": "u1",
              "text": "user 1",
            },
            "ownerID": "u1",
            "text": "item 2",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","u1","i1",": true,
            ""pKeySet","u1","i2",": true,
          },
        }
      `);
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
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources: {
          user: {
            columns: {
              id: {type: 'string'},
              text: {type: 'string'},
            },
            primaryKeys: ['id'],
            sorts: [['id', 'asc']],
          },
          issue: {
            columns: {
              id: {type: 'string'},
              ownerID: {type: 'string'},
              text: {type: 'string'},
            },
            primaryKeys: ['id'],
            sorts: [['id', 'asc']],
          },
          comment: {
            columns: {
              id: {type: 'string'},
              issueID: {type: 'string'},
            },
            primaryKeys: ['id'],
            sorts: [['id', 'asc']],
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
        joins: {
          comments: {
            parentKey: ['id'],
            parentSource: 'issue',
            childKey: ['issueID'],
            childSource: 'comment',
            relationshipName: 'comments',
          },
          issues: {
            parentKey: ['id'],
            parentSource: 'user',
            childKey: ['ownerID'],
            childSource: 'comments',
            relationshipName: 'issues',
          },
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
            "issue",
            "push",
            {
              "oldRow": {
                "id": "i2",
                "ownerID": "u2",
                "text": "item 2",
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "edit",
            },
          ],
          [
            "comment",
            "cleanup",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "comment",
            "fetch",
            {
              "constraint": {
                "issueID": "i2",
              },
            },
          ],
          [
            "comments",
            "push",
            {
              "oldRow": {
                "id": "i2",
                "ownerID": "u2",
                "text": "item 2",
              },
              "row": {
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
              "type": "edit",
            },
          ],
          [
            "user",
            "fetch",
            {
              "constraint": {
                "id": "u2",
              },
            },
          ],
          [
            "issues",
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
            "user",
            "fetch",
            {
              "constraint": {
                "id": "u1",
              },
            },
          ],
          [
            "issues",
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
                  },
                ],
                "id": "i1",
                "ownerID": "u1",
                "text": "item 1",
              },
              {
                "comments": [
                  {
                    "id": "c2",
                    "issueID": "i2",
                  },
                ],
                "id": "i2",
                "ownerID": "u1",
                "text": "item 2",
              },
            ],
            "text": "user 1",
          },
          {
            "id": "u2",
            "issues": [],
            "text": "user 2",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","i1","i1",": true,
            ""pKeySet","i2","i2",": true,
          },
          "issues": {
            ""pKeySet","u1","u1",": true,
            ""pKeySet","u2","u2",": true,
          },
        }
      `);

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
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u2', text: 'user 2'}],
        },
        joins,
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
            "user",
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
            "issue",
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
          },
          {
            "id": "i2",
            "owner": undefined,
            "ownerID": "u1",
            "text": "item 2",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","u1","i1",": true,
            ""pKeySet","u1","i2",": true,
          },
        }
      `);
      expect(pushes).toMatchInlineSnapshot(`[]`);
    });

    test('edit matching child', () => {
      const {log, data, actualStorage, pushes} = runJoinTest({
        sources,
        sourceContents: {
          issue: [
            {id: 'i1', ownerID: 'u1', text: 'item 1'},
            {id: 'i2', ownerID: 'u1', text: 'item 2'},
          ],
          user: [{id: 'u1', text: 'user 1'}],
        },
        joins,
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
            "user",
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
            "issue",
            "fetch",
            {
              "constraint": {
                "ownerID": "u1",
              },
            },
          ],
          [
            "comments",
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
            "comments",
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
            },
            "ownerID": "u1",
            "text": "item 1",
          },
          {
            "id": "i2",
            "owner": {
              "id": "u1",
              "text": "user 1 changed",
            },
            "ownerID": "u1",
            "text": "item 2",
          },
        ]
      `);
      expect(actualStorage).toMatchInlineSnapshot(`
        {
          "comments": {
            ""pKeySet","u1","i1",": true,
            ""pKeySet","u1","i2",": true,
          },
        }
      `);
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
      sorts: [['id', 'asc']],
    },
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
    revision: {
      columns: {
        id: {type: 'string'},
        commentID: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
  } as const;

  const joins: Joins = {
    revisions: {
      parentKey: ['id'],
      parentSource: 'comment',
      childKey: ['commentID'],
      childSource: 'revision',
      relationshipName: 'revisions',
    },
    comments: {
      parentKey: ['id'],
      parentSource: 'issue',
      childKey: ['issueID'],
      childSource: 'revisions',
      relationshipName: 'comments',
    },
  } as const;

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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [],
      },
      joins,
      format,
      pushes: [['revision', {type: 'add', row: {id: 'r1', commentID: 'c1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "revision",
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
          "comment",
          "fetch",
          {
            "constraint": {
              "id": "c1",
            },
          },
        ],
        [
          "revisions",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
                },
              ],
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
        "revisions": {
          ""pKeySet","c1","c1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      joins,
      format,
      pushes: [['comment', {type: 'add', row: {id: 'c1', issueID: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "comment",
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
          "revision",
          "fetch",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
        [
          "revisions",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "comments",
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
              "revisions": [
                {
                  "commentID": "c1",
                  "id": "r1",
                },
              ],
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
        "revisions": {
          ""pKeySet","c1","c1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'add', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "revisions",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "add",
          },
        ],
        [
          "comment",
          "fetch",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "revision",
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
                },
              ],
            },
          ],
          "id": "i1",
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {
          ""pKeySet","i1","i1",": true,
        },
        "revisions": {
          ""pKeySet","c1","c1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        comment: [{id: 'c1', issueID: 'i1'}],
        revision: [{id: 'r1', commentID: 'c1'}],
      },
      joins,
      format,
      pushes: [['issue', {type: 'remove', row: {id: 'i1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "revisions",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "comments",
          "push",
          {
            "row": {
              "id": "i1",
            },
            "type": "remove",
          },
        ],
        [
          "comment",
          "cleanup",
          {
            "constraint": {
              "issueID": "i1",
            },
          },
        ],
        [
          "revision",
          "cleanup",
          {
            "constraint": {
              "commentID": "c1",
            },
          },
        ],
      ]
    `);
    expect(data).toMatchInlineSnapshot(`[]`);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "comments": {},
        "revisions": {},
      }
    `);
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
      sorts: [['id', 'asc']],
    },
    issueLabel: {
      columns: {
        issueID: {type: 'string'},
        labelID: {type: 'string'},
      },
      primaryKeys: ['issueID', 'labelID'],
      sorts: [
        ['issueID', 'asc'],
        ['labelID', 'asc'],
      ],
    },
    label: {
      columns: {
        id: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
  } as const;

  const joins: Joins = {
    labels: {
      parentKey: ['labelID'],
      parentSource: 'issueLabel',
      childKey: ['id'],
      childSource: 'label',
      relationshipName: 'labels',
    },
    issueLabels: {
      parentKey: ['id'],
      parentSource: 'issue',
      childKey: ['issueID'],
      childSource: 'labels',
      relationshipName: 'issueLabels',
    },
  } as const;

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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        issueLabel: [{issueID: 'i1', labelID: 'l1'}],
        label: [],
      },
      joins,
      format,
      pushes: [['label', {type: 'add', row: {id: 'l1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "label",
          "push",
          {
            "row": {
              "id": "l1",
            },
            "type": "add",
          },
        ],
        [
          "issueLabel",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          "labels",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "issueLabels",
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
              },
            },
          ],
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "issueLabels": {
          ""pKeySet","i1","i1",": true,
        },
        "labels": {
          ""pKeySet","l1","i1","l1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}],
        issueLabel: [],
        label: [{id: 'l1'}],
      },
      joins,
      format,
      pushes: [
        ['issueLabel', {type: 'add', row: {issueID: 'i1', labelID: 'l1'}}],
      ],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issueLabel",
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
          "label",
          "fetch",
          {
            "constraint": {
              "id": "l1",
            },
          },
        ],
        [
          "labels",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "issueLabels",
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
              },
            },
          ],
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "issueLabels": {
          ""pKeySet","i1","i1",": true,
        },
        "labels": {
          ""pKeySet","l1","i1","l1",": true,
        },
      }
    `);
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: {
        issue: [{id: 'i1'}, {id: 'i2'}],
        issueLabel: [
          {issueID: 'i1', labelID: 'l1'},
          {issueID: 'i2', labelID: 'l1'},
        ],
        label: [],
      },
      joins,
      format,
      pushes: [['label', {type: 'add', row: {id: 'l1'}}]],
    });

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "label",
          "push",
          {
            "row": {
              "id": "l1",
            },
            "type": "add",
          },
        ],
        [
          "issueLabel",
          "fetch",
          {
            "constraint": {
              "labelID": "l1",
            },
          },
        ],
        [
          "labels",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i1",
            },
          },
        ],
        [
          "issueLabels",
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
          "labels",
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
          "issue",
          "fetch",
          {
            "constraint": {
              "id": "i2",
            },
          },
        ],
        [
          "issueLabels",
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
              },
            },
          ],
        },
        {
          "id": "i2",
          "issueLabels": [
            {
              "issueID": "i2",
              "labelID": "l1",
              "labels": {
                "id": "l1",
              },
            },
          ],
        },
      ]
    `);
    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "issueLabels": {
          ""pKeySet","i1","i1",": true,
          ""pKeySet","i2","i2",": true,
        },
        "labels": {
          ""pKeySet","l1","i1","l1",": true,
          ""pKeySet","l1","i2","l1",": true,
        },
      }
    `);
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
      sorts: [['issueID', 'asc']],
    },
    user: {
      columns: {
        userID: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKeys: ['userID'],
      sorts: [['userID', 'asc']],
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

  const joins: Joins = {
    creator: {
      parentKey: ['creatorID'],
      parentSource: 'issue',
      childKey: ['userID'],
      childSource: 'user',
      relationshipName: 'creator',
    },
    assignee: {
      parentKey: ['assigneeID'],
      parentSource: 'creator',
      childKey: ['userID'],
      childSource: 'user',
      relationshipName: 'assignee',
    },
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "name": "user 1",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
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
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
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
        sorts: [
          ['userID', 'asc'],
          ['id', 'asc'],
        ],
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

    const {log, data, actualStorage, pushes} = runJoinTest({
      sources: localSources,
      sourceContents: localSourceContents,
      joins,
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
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "assigneeID": "u1",
          "creator": [
            {
              "id": 1,
              "name": "user 1",
              "userID": "u1",
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
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
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
  });

  test('from one to none', () => {
    const {issue, ...rest} = sourceContents;
    const localSourceContents = {
      issue: [{...issue[0], assigneeID: 'u1'}, ...issue.slice(1)],
      ...rest,
    } as const;

    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents: localSourceContents,
      joins,
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
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
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
          "user",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u2","i2",": true,
          ""pKeySet",null,"i1",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
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
        sorts: [
          ['userID', 'asc'],
          ['id', 'asc'],
        ],
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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources: localSources,
      sourceContents: localSourceContents,
      joins,
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
            },
            {
              "id": 1.5,
              "name": "user 1.5",
              "userID": "u1",
            },
          ],
          "creatorID": "u1",
          "issueID": "i1",
          "text": "first issue",
        },
        {
          "assignee": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "assigneeID": "u2",
          "creator": [
            {
              "id": 2,
              "name": "user 2",
              "userID": "u2",
            },
          ],
          "creatorID": "u2",
          "issueID": "i2",
          "text": "second issue",
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "issue",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "user",
          "fetch",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "creator",
          "push",
          {
            "oldRow": {
              "assigneeID": "u1",
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "row": {
              "assigneeID": undefined,
              "creatorID": "u1",
              "issueID": "i1",
              "text": "first issue",
            },
            "type": "edit",
          },
        ],
        [
          "user",
          "cleanup",
          {
            "constraint": {
              "userID": "u1",
            },
          },
        ],
        [
          "assignee",
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
          "user",
          "fetch",
          {
            "constraint": {
              "userID": undefined,
            },
          },
        ],
        [
          "assignee",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "assignee": {
          ""pKeySet","u2","i2",": true,
          ""pKeySet",null,"i1",": true,
        },
        "creator": {
          ""pKeySet","u1","i1",": true,
          ""pKeySet","u2","i2",": true,
        },
      }
    `);
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
      sorts: [['id', 'asc']],
    },
    b: {
      columns: {
        id: {type: 'number'},
        b1: {type: 'number'},
        b2: {type: 'number'},
        b3: {type: 'number'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
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

  const joins: Joins = {
    ab: {
      parentSource: 'a',
      parentKey: ['a1', 'a2'],
      childSource: 'b',
      childKey: ['b2', 'b1'], // not the same order as parentKey
      relationshipName: 'ab',
    },
  };

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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
            },
          ],
          "id": 0,
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
            },
          ],
          "id": 1,
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
            },
          ],
          "id": 2,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "a",
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
          "b",
          "fetch",
          {
            "constraint": {
              "b1": 8,
              "b2": 7,
            },
          },
        ],
        [
          "ab",
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
          "b",
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
          "a",
          "fetch",
          {
            "constraint": {
              "a1": 7,
              "a2": 8,
            },
          },
        ],
        [
          "ab",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "ab": {
          ""pKeySet",1,2,0,": true,
          ""pKeySet",4,5,1,": true,
          ""pKeySet",7,8,2,": true,
        },
      }
    `);
  });

  test('edit child with moving it', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
            },
          ],
          "id": 0,
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
            },
          ],
          "id": 1,
        },
      ]
    `);

    expect(log).toMatchInlineSnapshot(`
      [
        [
          "a",
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
          "b",
          "cleanup",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
        [
          "b",
          "fetch",
          {
            "constraint": {
              "b1": 2,
              "b2": 1,
            },
          },
        ],
        [
          "ab",
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

    expect(actualStorage).toMatchInlineSnapshot(`
      {
        "ab": {
          ""pKeySet",1,2,0,": true,
          ""pKeySet",4,5,1,": true,
        },
      }
    `);
  });
});
