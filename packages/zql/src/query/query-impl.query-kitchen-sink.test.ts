import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {consume} from '../ivm/stream.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';
import {schema} from './test/test-schemas.ts';

function addData(queryDelegate: QueryDelegate) {
  const userSource = must(queryDelegate.getSource('user'));
  const issueSource = must(queryDelegate.getSource('issue'));
  const commentSource = must(queryDelegate.getSource('comment'));
  const revisionSource = must(queryDelegate.getSource('revision'));
  const labelSource = must(queryDelegate.getSource('label'));
  const issueLabelSource = must(queryDelegate.getSource('issueLabel'));

  consume(
    userSource.push({
      type: 'add',
      row: {id: '001', name: 'Alice', metadata: null},
    }),
  );
  consume(
    userSource.push({
      type: 'add',
      row: {id: '002', name: 'Bob', metadata: null},
    }),
  );
  consume(
    userSource.push({
      type: 'add',
      row: {id: '003', name: 'Charlie', metadata: {foo: 1}},
    }),
  );
  consume(
    userSource.push({
      type: 'add',
      row: {id: '004', name: 'Daniel', metadata: null},
    }),
  );

  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '101',
        title: 'Issue 1',
        description: 'Description 1',
        closed: false,
        ownerId: '001',
        createdAt: 1,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '102',
        title: 'Issue 2',
        description: 'Description 2',
        closed: false,
        ownerId: '001',
        createdAt: 2,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '103',
        title: 'Issue 3',
        description: 'Description 3',
        closed: false,
        ownerId: '001',
        createdAt: 3,
      },
    }),
  );

  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '104',
        title: 'Issue 4',
        description: 'Description 4',
        closed: false,
        ownerId: '002',
        createdAt: 4,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '105',
        title: 'Issue 5',
        description: 'Description 5',
        closed: false,
        ownerId: '002',
        createdAt: 5,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '106',
        title: 'Issue 6',
        description: 'Description 6',
        closed: true,
        ownerId: '002',
        createdAt: 6,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '107',
        title: 'Issue 7',
        description: 'Description 7',
        closed: true,
        ownerId: '003',
        createdAt: 7,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '108',
        title: 'Issue 8',
        description: 'Description 8',
        closed: true,
        ownerId: '003',
        createdAt: 8,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '109',
        title: 'Issue 9',
        description: 'Description 9',
        closed: false,
        ownerId: '003',
        createdAt: 9,
      },
    }),
  );
  consume(
    issueSource.push({
      type: 'add',
      row: {
        id: '110',
        title: 'Issue 10',
        description: 'Description 10',
        closed: false,
        ownerId: '004',
        createdAt: 10,
      },
    }),
  );

  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '201',
        issueId: '101',
        text: 'Comment 1',
        authorId: '001',
        createdAt: 1,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '202',
        issueId: '101',
        text: 'Comment 2',
        authorId: '002',
        createdAt: 2,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '203',
        issueId: '101',
        text: 'Comment 3',
        authorId: '003',
        createdAt: 3,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '204',
        issueId: '102',
        text: 'Comment 4',
        authorId: '001',
        createdAt: 4,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '205',
        issueId: '102',
        text: 'Comment 5',
        authorId: '002',
        createdAt: 5,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '206',
        issueId: '102',
        text: 'Comment 6',
        authorId: '003',
        createdAt: 6,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '207',
        issueId: '103',
        text: 'Comment 7',
        authorId: '001',
        createdAt: 7,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '208',
        issueId: '103',
        text: 'Comment 8',
        authorId: '002',
        createdAt: 8,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '209',
        issueId: '103',
        text: 'Comment 9',
        authorId: '003',
        createdAt: 9,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '210',
        issueId: '105',
        text: 'Comment 10',
        authorId: '001',
        createdAt: 10,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '211',
        issueId: '105',
        text: 'Comment 11',
        authorId: '002',
        createdAt: 11,
      },
    }),
  );
  consume(
    commentSource.push({
      type: 'add',
      row: {
        id: '212',
        issueId: '105',
        text: 'Comment 12',
        authorId: '003',
        createdAt: 12,
      },
    }),
  );

  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '301', commentId: '209', text: 'Revision 1', authorId: '001'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '302', commentId: '209', text: 'Revision 2', authorId: '001'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '303', commentId: '209', text: 'Revision 3', authorId: '001'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '304', commentId: '208', text: 'Revision 1', authorId: '002'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '305', commentId: '208', text: 'Revision 2', authorId: '002'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '306', commentId: '208', text: 'Revision 3', authorId: '002'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '307', commentId: '211', text: 'Revision 1', authorId: '003'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '308', commentId: '211', text: 'Revision 2', authorId: '003'},
    }),
  );
  consume(
    revisionSource.push({
      type: 'add',
      row: {id: '309', commentId: '211', text: 'Revision 3', authorId: '003'},
    }),
  );

  consume(labelSource.push({type: 'add', row: {id: '401', name: 'bug'}}));
  consume(labelSource.push({type: 'add', row: {id: '402', name: 'feature'}}));

  consume(
    issueLabelSource.push({type: 'add', row: {issueId: '103', labelId: '401'}}),
  );
  consume(
    issueLabelSource.push({type: 'add', row: {issueId: '102', labelId: '401'}}),
  );
  consume(
    issueLabelSource.push({type: 'add', row: {issueId: '102', labelId: '402'}}),
  );
}

describe('kitchen sink query', () => {
  test('complex query with filters, limits, and multiple joins', () => {
    const queryDelegate = new QueryDelegateImpl();
    addData(queryDelegate);
    const issueQuery = newQuery(schema, 'issue')
      .where('ownerId', 'IN', ['001', '002', '003'])
      .where('closed', false)
      .related('owner')
      .related('comments', q =>
        q
          .orderBy('createdAt', 'desc')
          .related('revisions', q => q.orderBy('id', 'desc').limit(1))
          .limit(2),
      )
      .related('labels')
      .start({
        id: '101',
        title: 'Issue 1',
        description: 'Description 1',
        closed: false,
        ownerId: '001',
      })
      .orderBy('title', 'asc')
      .limit(6);

    const view = queryDelegate.materialize(issueQuery);

    expect(queryDelegate.addedServerQueries).toMatchInlineSnapshot(`
      [
        {
          "args": undefined,
          "ast": {
            "limit": 6,
            "orderBy": [
              [
                "title",
                "asc",
              ],
            ],
            "related": [
              {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "ownerId",
                  ],
                },
                "subquery": {
                  "alias": "owner",
                  "table": "user",
                },
                "system": "client",
              },
              {
                "correlation": {
                  "childField": [
                    "issueId",
                  ],
                  "parentField": [
                    "id",
                  ],
                },
                "subquery": {
                  "alias": "comments",
                  "limit": 2,
                  "orderBy": [
                    [
                      "createdAt",
                      "desc",
                    ],
                  ],
                  "related": [
                    {
                      "correlation": {
                        "childField": [
                          "commentId",
                        ],
                        "parentField": [
                          "id",
                        ],
                      },
                      "subquery": {
                        "alias": "revisions",
                        "limit": 1,
                        "orderBy": [
                          [
                            "id",
                            "desc",
                          ],
                        ],
                        "table": "revision",
                      },
                      "system": "client",
                    },
                  ],
                  "table": "comment",
                },
                "system": "client",
              },
              {
                "correlation": {
                  "childField": [
                    "issueId",
                  ],
                  "parentField": [
                    "id",
                  ],
                },
                "hidden": true,
                "subquery": {
                  "alias": "labels",
                  "related": [
                    {
                      "correlation": {
                        "childField": [
                          "id",
                        ],
                        "parentField": [
                          "labelId",
                        ],
                      },
                      "subquery": {
                        "alias": "labels",
                        "table": "label",
                      },
                      "system": "client",
                    },
                  ],
                  "table": "issueLabel",
                },
                "system": "client",
              },
            ],
            "start": {
              "exclusive": true,
              "row": {
                "closed": false,
                "description": "Description 1",
                "id": "101",
                "ownerId": "001",
                "title": "Issue 1",
              },
            },
            "table": "issue",
            "where": {
              "conditions": [
                {
                  "left": {
                    "name": "ownerId",
                    "type": "column",
                  },
                  "op": "IN",
                  "right": {
                    "type": "literal",
                    "value": [
                      "001",
                      "002",
                      "003",
                    ],
                  },
                  "type": "simple",
                },
                {
                  "left": {
                    "name": "closed",
                    "type": "column",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": false,
                  },
                  "type": "simple",
                },
              ],
              "type": "and",
            },
          },
          "name": undefined,
          "ttl": 300000,
        },
      ]
    `);

    let rows: unknown[] = [];
    view.addListener(data => {
      rows = [...data].map(row => ({
        ...row,
        owner: row.owner,
        comments: [...row.comments].map(comment => ({
          ...comment,
          revisions: [...comment.revisions],
        })),
        labels: [...row.labels].map(label => ({
          ...label,
        })),
      }));
    });
    expect(rows).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "comments": [
            {
              "authorId": "003",
              "createdAt": 6,
              "id": "206",
              "issueId": "102",
              "revisions": [],
              "text": "Comment 6",
              Symbol(rc): 1,
            },
            {
              "authorId": "002",
              "createdAt": 5,
              "id": "205",
              "issueId": "102",
              "revisions": [],
              "text": "Comment 5",
              Symbol(rc): 1,
            },
          ],
          "createdAt": 2,
          "description": "Description 2",
          "id": "102",
          "labels": [
            {
              "id": "401",
              "name": "bug",
              Symbol(rc): 1,
            },
            {
              "id": "402",
              "name": "feature",
              Symbol(rc): 1,
            },
          ],
          "owner": {
            "id": "001",
            "metadata": null,
            "name": "Alice",
            Symbol(rc): 1,
          },
          "ownerId": "001",
          "title": "Issue 2",
          Symbol(rc): 1,
        },
        {
          "closed": false,
          "comments": [
            {
              "authorId": "003",
              "createdAt": 9,
              "id": "209",
              "issueId": "103",
              "revisions": [
                {
                  "authorId": "001",
                  "commentId": "209",
                  "id": "303",
                  "text": "Revision 3",
                  Symbol(rc): 1,
                },
              ],
              "text": "Comment 9",
              Symbol(rc): 1,
            },
            {
              "authorId": "002",
              "createdAt": 8,
              "id": "208",
              "issueId": "103",
              "revisions": [
                {
                  "authorId": "002",
                  "commentId": "208",
                  "id": "306",
                  "text": "Revision 3",
                  Symbol(rc): 1,
                },
              ],
              "text": "Comment 8",
              Symbol(rc): 1,
            },
          ],
          "createdAt": 3,
          "description": "Description 3",
          "id": "103",
          "labels": [
            {
              "id": "401",
              "name": "bug",
              Symbol(rc): 1,
            },
          ],
          "owner": {
            "id": "001",
            "metadata": null,
            "name": "Alice",
            Symbol(rc): 1,
          },
          "ownerId": "001",
          "title": "Issue 3",
          Symbol(rc): 1,
        },
        {
          "closed": false,
          "comments": [],
          "createdAt": 4,
          "description": "Description 4",
          "id": "104",
          "labels": [],
          "owner": {
            "id": "002",
            "metadata": null,
            "name": "Bob",
            Symbol(rc): 1,
          },
          "ownerId": "002",
          "title": "Issue 4",
          Symbol(rc): 1,
        },
        {
          "closed": false,
          "comments": [
            {
              "authorId": "003",
              "createdAt": 12,
              "id": "212",
              "issueId": "105",
              "revisions": [],
              "text": "Comment 12",
              Symbol(rc): 1,
            },
            {
              "authorId": "002",
              "createdAt": 11,
              "id": "211",
              "issueId": "105",
              "revisions": [
                {
                  "authorId": "003",
                  "commentId": "211",
                  "id": "309",
                  "text": "Revision 3",
                  Symbol(rc): 1,
                },
              ],
              "text": "Comment 11",
              Symbol(rc): 1,
            },
          ],
          "createdAt": 5,
          "description": "Description 5",
          "id": "105",
          "labels": [],
          "owner": {
            "id": "002",
            "metadata": null,
            "name": "Bob",
            Symbol(rc): 1,
          },
          "ownerId": "002",
          "title": "Issue 5",
          Symbol(rc): 1,
        },
        {
          "closed": false,
          "comments": [],
          "createdAt": 9,
          "description": "Description 9",
          "id": "109",
          "labels": [],
          "owner": {
            "id": "003",
            "metadata": {
              "foo": 1,
            },
            "name": "Charlie",
            Symbol(rc): 1,
          },
          "ownerId": "003",
          "title": "Issue 9",
          Symbol(rc): 1,
        },
      ]
    `);
  });
});
