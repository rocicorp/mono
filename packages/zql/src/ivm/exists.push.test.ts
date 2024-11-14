import {expect, suite, test} from 'vitest';
import {runJoinTest, type Joins, type Sources} from './test/join-push-tests.js';
import type {Format} from './view.js';
import type {Storage, Input} from './operator.js';
import {Exists} from './exists.js';

const sources: Sources = {
  issue: {
    columns: {
      id: {type: 'string'},
      text: {type: 'string'},
    },
    primaryKeys: ['id'],
    sorts: [['id', 'asc']],
    rows: [
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
  },
  comment: {
    columns: {
      id: {type: 'string'},
      issueID: {type: 'string'},
      test: {type: 'string'},
    },
    primaryKeys: ['id'],
    sorts: [['id', 'asc']],
    rows: [
      {id: 'c1', issueID: 'i1', text: 'i1 c1 text'},
      {id: 'c2', issueID: 'i3', text: 'i3 c2 text'},
      {id: 'c3', issueID: 'i3', text: 'i3 c3 text'},
    ],
  },
};

const joins: Joins = {
  comments: {
    parentKey: 'id',
    parentSource: 'issue',
    childKey: 'issueID',
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

suite('EXISTS', () => {
  const existsType = 'EXISTS';
  test('parent add that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
        "["size",["i5"]]": 0,
      }
    `);
  });

  test('parent add that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
        "["size",["i5"]]": 1,
      }
    `);
  });

  test('parent remove that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent remove that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent edit that has no children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent edit that has children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child add resulting in one child causes push of parent add', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child add resulting in > 1 child is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 2,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child remove resulting in no children causes push of parent remove', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
              "comments": [],
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
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child remove resulting in > 0 children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 1,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child edit is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child edit changes correlation', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
        "["size",["i5"]]": 0,
      }
    `);
  });

  test('parent add that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
        "["size",["i5"]]": 1,
      }
    `);
  });

  test('parent remove that has no children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent remove that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent edit that has no children is pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('parent edit that has children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child add resulting in one child causes push of parent remove', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child add resulting in > 1 child is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 2,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child remove resulting in no children causes push of parent add', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child remove resulting in > 0 children is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 1,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child edit is not pushed', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
        "["size",["i1"]]": 1,
        "["size",["i2"]]": 0,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  test('child edit changes correlation', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
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
        op: new Exists(i, storage, 'comments', existsType),
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
          "type": "add",
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
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['exists']).toMatchInlineSnapshot(`
      {
        "["size",["i1"]]": 0,
        "["size",["i2"]]": 1,
        "["size",["i3"]]": 2,
        "["size",["i4"]]": 0,
      }
    `);
  });

  // potential tests to add
  // 1. child child change is not pushed
  // 2. child change to other relationship is not pushed if parent has child
  // 3. child change to other relationship is pushed if parent does not have children
});
