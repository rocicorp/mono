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
  test('child add resulting in one child causes push of parent add', () => {
    const {log, data, actualStorage, pushes} = runJoinTest({
      sources,
      joins,
      pushes: [
        [
          'comment',
          {
            type: 'add',
            row: {id: 'c3', issueID: 'i2', text: 'i2 c3 text'},
          },
        ],
      ],
      format,
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', 'EXISTS'),
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
            "id": "c3",
            "issueID": "i2",
            "text": "i2 c3 text",
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
                  "id": "c3",
                  "issueID": "i2",
                  "text": "i2 c3 text",
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

    expect(actualStorage['take']).toMatchInlineSnapshot(`undefined`);
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
            row: {id: 'c2', issueID: 'i3', text: 'i3 c2 text'},
          },
        ],
      ],
      format,
      addPostJoinsOperator: (i: Input, storage: Storage) => ({
        name: 'exists',
        op: new Exists(i, storage, 'comments', 'EXISTS'),
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
    ]
  `);

    expect(log.filter(msg => msg[0] === 'exists')).toMatchInlineSnapshot(`
      [
        [
          "exists",
          "push",
          {
            "row": {
              "id": "i3",
              "text": "third issue",
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
              "id": "i3",
              "text": "third issue",
            },
          },
          "type": "remove",
        },
      ]
    `);

    expect(actualStorage['take']).toMatchInlineSnapshot(`undefined`);
  });
});
