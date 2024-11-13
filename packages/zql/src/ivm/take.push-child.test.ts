import {expect, test} from 'vitest';
import {runJoinTest, type Joins, type Sources} from './test/join-push-tests.js';
import type {Format} from './view.js';

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
      {id: 'c2', issueID: 'i1', text: 'i1 c2 text'},
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

test('child change, parent is within bound', () => {
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
    limit: 2,
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
                "id": "c2",
                "issueID": "i1",
                "text": "i1 c2 text",
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
        ]
    `);

  expect(log.filter(msg => msg[0] === 'take')).toMatchInlineSnapshot(`
        [
          [
            "take",
            "push",
            {
              "child": {
                "row": {
                  "id": "c3",
                  "issueID": "i2",
                  "text": "i2 c3 text",
                },
                "type": "add",
              },
              "row": {
                "id": "i2",
                "text": "second issue",
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
                    "issueID": "i2",
                    "text": "i2 c3 text",
                  },
                },
                "type": "add",
              },
              "relationshipName": "comments",
            },
            "row": {
              "id": "i2",
              "text": "second issue",
            },
            "type": "child",
          },
        ]
    `);

  expect(actualStorage['take']).toMatchInlineSnapshot(`
        {
          "["take",null]": {
            "bound": {
              "id": "i2",
              "text": "second issue",
            },
            "size": 2,
          },
          "maxBound": {
            "id": "i2",
            "text": "second issue",
          },
        }
  `);
});

test('child change, parent is after bound', () => {
  const {log, data, actualStorage, pushes} = runJoinTest({
    sources,
    joins,
    pushes: [
      [
        'comment',
        {
          type: 'add',
          row: {id: 'c3', issueID: 'i3', text: 'i3 c3 text'},
        },
      ],
    ],
    format,
    limit: 2,
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
                "id": "c2",
                "issueID": "i1",
                "text": "i1 c2 text",
              },
            ],
            "id": "i1",
            "text": "first issue",
          },
          {
            "comments": [],
            "id": "i2",
            "text": "second issue",
          },
        ]
    `);

  expect(log.filter(msg => msg[0] === 'take')).toHaveLength(0);

  expect(pushes).toHaveLength(0);

  expect(actualStorage['take']).toMatchInlineSnapshot(`
        {
          "["take",null]": {
            "bound": {
              "id": "i2",
              "text": "second issue",
            },
            "size": 2,
          },
          "maxBound": {
            "id": "i2",
            "text": "second issue",
          },
        }
  `);
});
