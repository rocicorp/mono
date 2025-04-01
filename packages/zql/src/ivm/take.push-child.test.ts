import {expect, test} from 'vitest';
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
    {id: 'c2', issueID: 'i1', text: 'i1 c2 text'},
  ],
};

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
  limit: 2,
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

test('child change, parent is within bound', () => {
  const {log, data, actualStorage, pushes} = runPushTest({
    sources,
    sourceContents,
    ast,
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
            "id": "c2",
            "issueID": "i1",
            "text": "i1 c2 text",
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
            "id": "c3",
            "issueID": "i2",
            "text": "i2 c3 text",
            Symbol(rc): 1,
          },
        ],
        "id": "i2",
        "text": "second issue",
        Symbol(rc): 1,
      },
    ]
  `);

  expect(log.filter(msg => msg[0] === ':take')).toMatchInlineSnapshot(`
    [
      [
        ":take",
        "fetch",
        {
          "constraint": {
            "id": "i2",
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

  expect(actualStorage[':take']).toMatchInlineSnapshot(`
        {
          "["take"]": {
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
  const {log, data, actualStorage, pushes} = runPushTest({
    sources,
    sourceContents,
    ast,
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
            "id": "c2",
            "issueID": "i1",
            "text": "i1 c2 text",
            Symbol(rc): 1,
          },
        ],
        "id": "i1",
        "text": "first issue",
        Symbol(rc): 1,
      },
      {
        "comments": [],
        "id": "i2",
        "text": "second issue",
        Symbol(rc): 1,
      },
    ]
  `);

  expect(log.filter(msg => msg[0] === ':take')).toMatchInlineSnapshot(`
    [
      [
        ":take",
        "fetch",
        {
          "constraint": {
            "id": "i3",
          },
        },
      ],
    ]
  `);

  expect(pushes).toHaveLength(0);

  expect(actualStorage[':take']).toMatchInlineSnapshot(`
        {
          "["take"]": {
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
