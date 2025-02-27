import {expect, suite, test} from 'vitest';
import {Exists} from './exists.ts';
import type {Input, Storage} from './operator.ts';
import {Take} from './take.ts';
import {
  runJoinTest,
  type Joins,
  type SourceContents,
  type Sources,
} from './test/join-push-tests.ts';
import type {Format} from './view.ts';

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

const joins: Joins = {
  comments: {
    parentKey: ['id'],
    parentSource: 'issue',
    childKey: ['issueID'],
    childSource: 'comment',
    relationshipName: 'comments',
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

suite('EXISTS 1 to many', () => {
  const sources: Sources = {
    comment: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
    },
    issue: {
      columns: {
        id: {type: 'string'},
        title: {type: 'string'},
      },
      primaryKeys: ['id'],
      sorts: [['id', 'asc']],
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

  const joins: Joins = {
    children: {
      parentKey: ['issueID'],
      parentSource: 'comment',
      childKey: ['id'],
      childSource: 'issue',
      relationshipName: 'issue',
    },
  };

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
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: [
        (i: Input, storage: Storage) => ({
          name: 'exists',
          op: new Exists(i, storage, 'issue', ['issueID'], 'EXISTS'),
        }),
        (i: Input, storage: Storage) => ({
          name: 'take',
          op: new Take(i, storage, 2),
        }),
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
            },
          ],
          "issueID": "i2",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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
          "exists",
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
          "exists",
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
          "exists",
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
          "exists",
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
          "exists",
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
    expect(log.filter(msg => msg[0] === 'take')).toMatchInlineSnapshot(`
      [
        [
          "take",
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
          "take",
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
          "take",
          "push",
          {
            "row": {
              "id": "c2",
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
              "id": "c2",
              "issueID": "i1",
            },
          },
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row/["i1"]": 0,
        "row/["i1"]/["c1"]": 0,
        "row/["i1"]/["c2"]": 0,
        "row/["i1"]/["c3"]": 0,
        "row/["i2"]": 1,
        "row/["i2"]/["c4"]": 1,
      }
    `);

    expect(actualStorage['take']).toMatchInlineSnapshot(`
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
  const existsType = 'EXISTS';
  test('parent add that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
        "row//["i5"]": 0,
      }
    `);
  });

  test('parent add that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
        {
          "comments": [
            {
              "id": "c4",
              "issueID": "i5",
              "text": "i2 c54 text",
            },
          ],
          "id": "i5",
          "text": "fifth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
        "row//["i5"]": 1,
      }
    `);
  });

  test('parent remove that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    // i2 size is removed
    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent remove that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    // i1 size is removed
    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent edit that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent edit that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue v2",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child add resulting in one child causes push of parent add', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c4",
              "issueID": "i2",
              "text": "i2 c4 text",
            },
          ],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child add resulting in > 1 child is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
            {
              "id": "c4",
              "issueID": "i1",
              "text": "i1 c4 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 2,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child remove resulting in no children causes push of parent remove', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child remove resulting in > 0 children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 1,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child edit is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i1",
              "text": "i1 c1 text",
            },
          ],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text v2",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child edit changes correlation', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [
            {
              "id": "c1",
              "issueID": "i2",
              "text": "i2 c1 text",
            },
          ],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [
            {
              "id": "c2",
              "issueID": "i3",
              "text": "i3 c2 text",
            },
            {
              "id": "c3",
              "issueID": "i3",
              "text": "i3 c3 text",
            },
          ],
          "id": "i3",
          "text": "third issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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
          "exists",
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  // potential tests to add
  // 1. child child change is pushed
  // 2. child change to other relationship is pushed if parent has child
  // 3. child change to other relationship is not push if parent does not have children
});

suite('NOT EXISTS', () => {
  const existsType = 'NOT EXISTS';
  test('parent add that has no children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
        {
          "comments": [],
          "id": "i5",
          "text": "fifth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
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
              "comments": [],
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

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
        "row//["i5"]": 0,
      }
    `);
  });

  test('parent add that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
        "row//["i5"]": 1,
      }
    `);
  });

  test('parent remove that has no children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i2",
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
              "comments": [],
            },
            "row": {
              "id": "i2",
              "text": "first issue",
            },
          },
          "type": "remove",
        },
      ]
    `);

    // i2 size is removed
    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent remove that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    // i1 size is removed
    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent edit that has no children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue v2",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "oldRow": {
              "id": "i2",
              "text": "second issue",
            },
            "row": {
              "id": "i2",
              "text": "second issue v2",
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
            "id": "i2",
            "text": "second issue",
          },
          "row": {
            "id": "i2",
            "text": "second issue v2",
          },
          "type": "edit",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('parent edit that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child add resulting in one child causes push of parent remove', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i2",
              "text": "second issue",
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
              "comments": [],
            },
            "row": {
              "id": "i2",
              "text": "second issue",
            },
          },
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child add resulting in > 1 child is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 2,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child remove resulting in no children causes push of parent add', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i1",
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
              "comments": [],
            },
            "row": {
              "id": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child remove resulting in > 0 children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 1,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child edit is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i2",
          "text": "second issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`[]`);

    expect(pushes).toMatchInlineSnapshot(`[]`);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 1,
        "row//["i2"]": 0,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  test('child edit changes correlation', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      sourceContents,
      joins,
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
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', ['id'], existsType),
      }),
    });

    expect(data).toMatchInlineSnapshot(`
      [
        {
          "comments": [],
          "id": "i1",
          "text": "first issue",
        },
        {
          "comments": [],
          "id": "i4",
          "text": "fourth issue",
        },
      ]
    `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i1",
              "text": "first issue",
            },
            "type": "add",
          },
        ],
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i2",
              "text": "second issue",
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
              "comments": [],
            },
            "row": {
              "id": "i1",
              "text": "first issue",
            },
          },
          "type": "add",
        },
        {
          "node": {
            "relationships": {
              "comments": [],
            },
            "row": {
              "id": "i2",
              "text": "second issue",
            },
          },
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "row//["i1"]": 0,
        "row//["i2"]": 1,
        "row//["i3"]": 2,
        "row//["i4"]": 0,
      }
    `);
  });

  // potential tests to add
  // 1. child child change is not pushed
  // 2. child change to other relationship is not pushed if parent has child
  // 3. child change to other relationship is pushed if parent does not have children
});
