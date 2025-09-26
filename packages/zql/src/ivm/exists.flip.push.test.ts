import {expect, suite, test} from 'vitest';
import {
  runPushTest,
  type SourceContents,
  type Sources,
} from './test/push-tests.ts';
import type {Format} from './view.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';

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

const sourceContents: SourceContents = {
  issue: [
    {
      id: 'i1',
      text: 'first issue',
    },
    {
      id: 'i2',
      text: 'second issue',
    },
    {
      id: 'i3',
      text: 'third issue',
    },
    {
      id: 'i4',
      text: 'fourth issue',
    },
  ],
  comment: [
    {id: 'c1', issueID: 'i1', text: 'i1 c1 text'},
    {id: 'c2', issueID: 'i3', text: 'i3 c2 text'},
    {id: 'c3', issueID: 'i3', text: 'i3 c3 text'},
  ],
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
suite('EXISTS 1 to many', () => {
  const sources: Sources = {
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
    issue: {
      columns: {
        id: {type: 'string'},
        title: {type: 'string'},
      },
      primaryKeys: ['id'],
    },
  };

  const sourceContents: SourceContents = {
    comment: [
      {
        id: 'c1',
        issueID: 'i1',
      },
      {
        id: 'c2',
        issueID: 'i1',
      },
      {
        id: 'c3',
        issueID: 'i1',
      },
      {
        id: 'c4',
        issueID: 'i2',
      },
    ],
    issue: [
      {id: 'i1', title: 'issue 1'},
      {id: 'i2', title: 'issue 2'},
    ],
  };

  const ast: AST = {
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
      flip: true,
    },
    limit: 2,
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

  test('Remove of child that joins with multiple parents, interplay with take', () => {
    /**
     * The problem, exists receives `child.remove` events for relationships with 0 size:
     * 1. An issue is removed in a push
     * 2. `take` fetches, bringing `c3` into scope of `exists`
     * 3. `join` pushes a child remove for `c3`
     * 4. `exists` receives the child remove for `c3` and used to throw because the size is 0,
     * but this assert is currently disabled as a work around and the remove is just dropped
     */
    const {log, data, actualStorage, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      format,
      pushes: [
        [
          'issue',
          {
            type: 'remove',
            row: {id: 'i1', title: 'issue 1'},
          },
        ],
      ],
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "id": "c4",
          "issue": [
            {
              "id": "i2",
              "title": "issue 2",
              Symbol(rc): 1,
            },
          ],
          "issueID": "i2",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(issue)'))
      .toMatchInlineSnapshot(`
      [
        [
          ":flipped-join(issue)",
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
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "reverse": true,
            "start": {
              "basis": "after",
              "row": {
                "id": "c2",
                "issueID": "i1",
              },
            },
          },
        ],
        [
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "start": {
              "basis": "at",
              "row": {
                "id": "c2",
                "issueID": "i1",
              },
            },
          },
        ],
        [
          ":flipped-join(issue)",
          "push",
          {
            "row": {
              "id": "c2",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "reverse": true,
            "start": {
              "basis": "after",
              "row": {
                "id": "c3",
                "issueID": "i1",
              },
            },
          },
        ],
        [
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "start": {
              "basis": "at",
              "row": {
                "id": "c3",
                "issueID": "i1",
              },
            },
          },
        ],
        [
          ":flipped-join(issue)",
          "push",
          {
            "row": {
              "id": "c3",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "reverse": true,
            "start": {
              "basis": "after",
              "row": {
                "id": "c4",
                "issueID": "i2",
              },
            },
          },
        ],
        [
          ":flipped-join(issue)",
          "fetch",
          {
            "constraint": undefined,
            "start": {
              "basis": "at",
              "row": {
                "id": "c4",
                "issueID": "i2",
              },
            },
          },
        ],
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(issue)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(issue)",
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
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "id": "c2",
                  "issueID": "i1",
                },
              },
            },
          ],
          [
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "id": "c2",
                  "issueID": "i1",
                },
              },
            },
          ],
          [
            ":flipped-join(issue)",
            "push",
            {
              "row": {
                "id": "c2",
                "issueID": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "id": "c3",
                  "issueID": "i1",
                },
              },
            },
          ],
          [
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "id": "c3",
                  "issueID": "i1",
                },
              },
            },
          ],
          [
            ":flipped-join(issue)",
            "push",
            {
              "row": {
                "id": "c3",
                "issueID": "i1",
              },
              "type": "remove",
            },
          ],
          [
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "reverse": true,
              "start": {
                "basis": "after",
                "row": {
                  "id": "c4",
                  "issueID": "i2",
                },
              },
            },
          ],
          [
            ":flipped-join(issue)",
            "fetch",
            {
              "constraint": undefined,
              "start": {
                "basis": "at",
                "row": {
                  "id": "c4",
                  "issueID": "i2",
                },
              },
            },
          ],
        ]
      `);
    expect(log.filter(msg => msg[0] === ':take')).toMatchInlineSnapshot(`
      [
        [
          ":take",
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
          ":take",
          "push",
          {
            "row": {
              "id": "c3",
              "issueID": "i1",
            },
            "type": "add",
          },
        ],
        [
          ":take",
          "push",
          {
            "row": {
              "id": "c2",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
        [
          ":take",
          "push",
          {
            "row": {
              "id": "c4",
              "issueID": "i2",
            },
            "type": "add",
          },
        ],
        [
          ":take",
          "push",
          {
            "row": {
              "id": "c3",
              "issueID": "i1",
            },
            "type": "remove",
          },
        ],
      ]
    `);

    expect(pushes).toMatchInlineSnapshot(`
      [
        {
          "node": {
            "relationships": {
              "issue": [
                {
                  "relationships": {},
                  "row": {
                    "id": "i1",
                    "title": "issue 1",
                  },
                },
              ],
            },
            "row": {
              "id": "c1",
              "issueID": "i1",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "issue": [
                {
                  "relationships": {},
                  "row": {
                    "id": "i1",
                    "title": "issue 1",
                  },
                },
              ],
            },
            "row": {
              "id": "c3",
              "issueID": "i1",
            },
          },
          "type": "add",
        },
        {
          "node": {
            "relationships": {
              "issue": [
                {
                  "relationships": {},
                  "row": {
                    "id": "i1",
                    "title": "issue 1",
                  },
                },
              ],
            },
            "row": {
              "id": "c2",
              "issueID": "i1",
            },
          },
          "type": "remove",
        },
        {
          "node": {
            "relationships": {
              "issue": [
                {
                  "relationships": {},
                  "row": {
                    "id": "i2",
                    "title": "issue 2",
                  },
                },
              ],
            },
            "row": {
              "id": "c4",
              "issueID": "i2",
            },
          },
          "type": "add",
        },
        {
          "node": {
            "relationships": {
              "issue": [
                {
                  "relationships": {},
                  "row": {
                    "id": "i1",
                    "title": "issue 1",
                  },
                },
              ],
            },
            "row": {
              "id": "c3",
              "issueID": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage[':take']).toMatchInlineSnapshot(`
      {
        "["take"]": {
          "bound": {
            "id": "c4",
            "issueID": "i2",
          },
          "size": 1,
        },
        "maxBound": {
          "id": "c4",
          "issueID": "i2",
        },
      }
    `);
  });
});

suite('EXISTS', () => {
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
      flip: true,
    },
  } as const;
  test('parent add that has no children is not pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'add',
            row: {id: 'i5', text: 'fifth issue'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(
      log.filter(msg => msg[0] === ':flipped-join(comments)'),
    ).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('parent add that has children is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'add',
            row: {id: 'c4', issueID: 'i5', text: 'i2 c54 text'},
          },
        ],
        [
          'issue',
          {
            type: 'add',
            row: {id: 'i5', text: 'fifth issue'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c4",
              "issueID": "i5",
              "text": "i2 c54 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i5",
          "text": "fifth issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i5",
                "text": "fifth issue",
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
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c4",
                    "issueID": "i5",
                    "text": "i2 c54 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i5",
              "text": "fifth issue",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('parent remove that has no children is not pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'remove',
            row: {id: 'i2', text: 'first issue'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(
      log.filter(msg => msg[0] === ':flipped-join(comments)'),
    ).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('parent remove that has children is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'remove',
            row: {id: 'i1', text: 'first issue'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i1",
                "text": "first issue",
              },
              "type": "remove",
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
                    "text": "i1 c1 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('parent edit that has no children is not pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {id: 'i2', text: 'second issue'},
            row: {id: 'i2', text: 'second issue v2'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(
      log.filter(msg => msg[0] === ':flipped-join(comments)'),
    ).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);
  });

  test('parent edit that has children is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'issue',
          {
            type: 'edit',
            oldRow: {id: 'i1', text: 'first issue'},
            row: {id: 'i1', text: 'first issue v2'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue v2",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "oldRow": {
                "id": "i1",
                "text": "first issue",
              },
              "row": {
                "id": "i1",
                "text": "first issue v2",
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
            "id": "i1",
            "text": "first issue",
          },
          "row": {
            "id": "i1",
            "text": "first issue v2",
          },
          "type": "edit",
        },
      ]
    `);
  });

  test('child add resulting in one child causes push of parent add', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'add',
            row: {id: 'c4', issueID: 'i2', text: 'i2 c4 text'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c4",
              "issueID": "i2",
              "text": "i2 c4 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i2",
                "text": "second issue",
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
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c4",
                    "issueID": "i2",
                    "text": "i2 c4 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i2",
              "text": "second issue",
            },
          },
          "type": "add",
        },
      ]
    `);
  });

  test('child add resulting in > 1 child is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'add',
            row: {id: 'c4', issueID: 'i1', text: 'i1 c4 text'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
            {
              "id": "c4",
              "issueID": "i1",
              "text": "i1 c4 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "id": "c4",
                  "issueID": "i1",
                  "text": "i1 c4 text",
                },
                "type": "add",
              },
              "row": {
                "id": "i1",
                "text": "first issue",
              },
              "type": "child",
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
                  "id": "c4",
                  "issueID": "i1",
                  "text": "i1 c4 text",
                },
              },
              "type": "add",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i1",
            "text": "first issue",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child remove resulting in no children causes push of parent remove', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'remove',
            row: {id: 'c1', issueID: 'i1', text: 'i1 c1 text'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i1",
                "text": "first issue",
              },
              "type": "remove",
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
                    "text": "i1 c1 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
      ]
    `);
  });

  test('child remove resulting in > 0 children is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'remove',
            row: {id: 'c3', issueID: 'i3', text: 'i3 c3 text'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "child": {
                "row": {
                  "id": "c3",
                  "issueID": "i3",
                  "text": "i3 c3 text",
                },
                "type": "remove",
              },
              "row": {
                "id": "i3",
                "text": "third issue",
              },
              "type": "child",
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
                  "id": "c3",
                  "issueID": "i3",
                  "text": "i3 c3 text",
                },
              },
              "type": "remove",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i3",
            "text": "third issue",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child edit is pushed', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'edit',
            oldRow: {id: 'c3', issueID: 'i3', text: 'i3 c3 text'},
            row: {id: 'c3', issueID: 'i3', text: 'i3 c3 text v2'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i1",
          "text": "first issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text v2",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "child": {
                "oldRow": {
                  "id": "c3",
                  "issueID": "i3",
                  "text": "i3 c3 text",
                },
                "row": {
                  "id": "c3",
                  "issueID": "i3",
                  "text": "i3 c3 text v2",
                },
                "type": "edit",
              },
              "row": {
                "id": "i3",
                "text": "third issue",
              },
              "type": "child",
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
                "id": "c3",
                "issueID": "i3",
                "text": "i3 c3 text",
              },
              "row": {
                "id": "c3",
                "issueID": "i3",
                "text": "i3 c3 text v2",
              },
              "type": "edit",
            },
            "relationshipName": "comments",
          },
          "row": {
            "id": "i3",
            "text": "third issue",
          },
          "type": "child",
        },
      ]
    `);
  });

  test('child edit changes correlation', () => {
    const {log, data, pushes} = runPushTest({
      sources,
      sourceContents,
      ast,
      pushes: [
        [
          'comment',
          {
            type: 'edit',
            oldRow: {id: 'c1', issueID: 'i1', text: 'i1 c1 text'},
            row: {id: 'c1', issueID: 'i2', text: 'i2 c1 text'},
          },
        ],
      ],
      format,
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i2",
              "text": "i2 c1 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i2",
          "text": "second issue",
          Symbol(rc): 1,
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
              Symbol(rc): 1,
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
              Symbol(rc): 1,
            },
          ],
          "id": "i3",
          "text": "third issue",
          Symbol(rc): 1,
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === ':flipped-join(comments)'))
      .toMatchInlineSnapshot(`
        [
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i1",
                "text": "first issue",
              },
              "type": "remove",
            },
          ],
          [
            ":flipped-join(comments)",
            "push",
            {
              "row": {
                "id": "i2",
                "text": "second issue",
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
              "comments": [
                {
                  "relationships": {},
                  "row": {
                    "id": "c1",
                    "issueID": "i1",
                    "text": "i1 c1 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i1",
              "text": "first issue",
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
                    "id": "c1",
                    "issueID": "i2",
                    "text": "i2 c1 text",
                  },
                },
              ],
            },
            "row": {
              "id": "i2",
              "text": "second issue",
            },
          },
          "type": "add",
        },
      ]
    `);
  });
});
