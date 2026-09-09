import {describe, expect, test} from 'vitest';
import type {TableSchema} from '../../../zero-types/src/schema.ts';
import {completeOrdering} from './complete-ordering.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import {type AnyQuery} from './query.ts';
import {schema} from './test/test-schemas.ts';

function ast(q: AnyQuery) {
  return asQueryInternals(q).ast;
}

const tables: Record<string, TableSchema> = schema.tables;

const getPrimaryKey = (tableName: string) => tables[tableName].primaryKey;

describe('completeOrdering', () => {
  test('basic', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": undefined,
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "asc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": undefined,
        }
      `);
  });

  test('basic, ordered on non primary key', () => {
    const issueQuery = newQuery(schema, 'issue').orderBy('title', 'asc');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": [
          [
            "title",
            "asc",
          ],
        ],
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": undefined,
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "title",
              "asc",
            ],
            [
              "id",
              "asc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": undefined,
        }
      `);
  });

  test('basic, partial order', () => {
    const q = newQuery(schema, 'issueLabel').orderBy('labelId', 'asc');
    expect(ast(q)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": [
          [
            "labelId",
            "asc",
          ],
        ],
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issueLabel",
        "where": undefined,
      }
    `);
    expect(completeOrdering(ast(q), getPrimaryKey)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": [
          [
            "labelId",
            "asc",
          ],
          [
            "issueId",
            "asc",
          ],
        ],
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issueLabel",
        "where": undefined,
      }
    `);

    const q2 = newQuery(schema, 'issueLabel').orderBy('issueId', 'asc');
    expect(ast(q2)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": [
          [
            "issueId",
            "asc",
          ],
        ],
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issueLabel",
        "where": undefined,
      }
    `);
    expect(completeOrdering(ast(q2), getPrimaryKey)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": [
          [
            "issueId",
            "asc",
          ],
          [
            "labelId",
            "asc",
          ],
        ],
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issueLabel",
        "where": undefined,
      }
    `);
  });

  test('related', () => {
    const issueQuery = newQuery(schema, 'issue').related('labels');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": [
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
              "limit": undefined,
              "orderBy": undefined,
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
                  "hidden": undefined,
                  "subquery": {
                    "alias": "labels",
                    "limit": undefined,
                    "orderBy": undefined,
                    "related": undefined,
                    "schema": undefined,
                    "start": undefined,
                    "table": "label",
                    "where": undefined,
                  },
                  "system": "client",
                },
              ],
              "schema": undefined,
              "start": undefined,
              "table": "issueLabel",
              "where": undefined,
            },
            "system": "client",
          },
        ],
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": undefined,
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "asc",
            ],
          ],
          "related": [
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
                "limit": undefined,
                "orderBy": [
                  [
                    "issueId",
                    "asc",
                  ],
                  [
                    "labelId",
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
                        "labelId",
                      ],
                    },
                    "hidden": undefined,
                    "subquery": {
                      "alias": "labels",
                      "limit": undefined,
                      "orderBy": [
                        [
                          "id",
                          "asc",
                        ],
                      ],
                      "related": undefined,
                      "schema": undefined,
                      "start": undefined,
                      "table": "label",
                      "where": undefined,
                    },
                    "system": "client",
                  },
                ],
                "schema": undefined,
                "start": undefined,
                "table": "issueLabel",
                "where": undefined,
              },
              "system": "client",
            },
          ],
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": undefined,
        }
      `);
  });

  test('exists', () => {
    const issueQuery = newQuery(schema, 'issue').whereExists('labels');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": {
          "flip": undefined,
          "op": "EXISTS",
          "related": {
            "correlation": {
              "childField": [
                "issueId",
              ],
              "parentField": [
                "id",
              ],
            },
            "hidden": undefined,
            "subquery": {
              "alias": "zsubq_labels",
              "limit": undefined,
              "orderBy": undefined,
              "related": undefined,
              "schema": undefined,
              "start": undefined,
              "table": "issueLabel",
              "where": {
                "flip": undefined,
                "op": "EXISTS",
                "related": {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "labelId",
                    ],
                  },
                  "hidden": undefined,
                  "subquery": {
                    "alias": "zsubq_zhidden_labels",
                    "limit": undefined,
                    "orderBy": undefined,
                    "related": undefined,
                    "schema": undefined,
                    "start": undefined,
                    "table": "label",
                    "where": undefined,
                  },
                  "system": "client",
                },
                "scalar": undefined,
                "type": "correlatedSubquery",
              },
            },
            "system": "client",
          },
          "scalar": undefined,
          "type": "correlatedSubquery",
        },
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "asc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": {
            "flip": undefined,
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "issueId",
                ],
                "parentField": [
                  "id",
                ],
              },
              "hidden": undefined,
              "subquery": {
                "alias": "zsubq_labels",
                "limit": undefined,
                "orderBy": [
                  [
                    "issueId",
                    "asc",
                  ],
                  [
                    "labelId",
                    "asc",
                  ],
                ],
                "related": undefined,
                "schema": undefined,
                "start": undefined,
                "table": "issueLabel",
                "where": {
                  "flip": undefined,
                  "op": "EXISTS",
                  "related": {
                    "correlation": {
                      "childField": [
                        "id",
                      ],
                      "parentField": [
                        "labelId",
                      ],
                    },
                    "hidden": undefined,
                    "subquery": {
                      "alias": "zsubq_zhidden_labels",
                      "limit": undefined,
                      "orderBy": [
                        [
                          "id",
                          "asc",
                        ],
                      ],
                      "related": undefined,
                      "schema": undefined,
                      "start": undefined,
                      "table": "label",
                      "where": undefined,
                    },
                    "system": "client",
                  },
                  "scalar": undefined,
                  "type": "correlatedSubquery",
                },
              },
              "system": "client",
            },
            "scalar": undefined,
            "type": "correlatedSubquery",
          },
        }
      `);
  });

  test('exists in compound condition', () => {
    const issueQuery = newQuery(schema, 'issue').where(
      ({and, or, cmp, exists}) =>
        and(
          cmp('id', '1'),
          exists('owner'),
          or(cmp('ownerId', '2'), exists('comments')),
        ),
    );

    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": {
          "conditions": [
            {
              "left": {
                "name": "id",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "1",
              },
              "type": "simple",
            },
            {
              "conditions": [
                {
                  "left": {
                    "name": "ownerId",
                    "type": "column",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": "2",
                  },
                  "type": "simple",
                },
                {
                  "flip": undefined,
                  "op": "EXISTS",
                  "related": {
                    "correlation": {
                      "childField": [
                        "issueId",
                      ],
                      "parentField": [
                        "id",
                      ],
                    },
                    "hidden": undefined,
                    "subquery": {
                      "alias": "zsubq_comments",
                      "limit": undefined,
                      "orderBy": undefined,
                      "related": undefined,
                      "schema": undefined,
                      "start": undefined,
                      "table": "comment",
                      "where": undefined,
                    },
                    "system": "client",
                  },
                  "scalar": undefined,
                  "type": "correlatedSubquery",
                },
              ],
              "type": "or",
            },
            {
              "flip": undefined,
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "ownerId",
                  ],
                },
                "hidden": undefined,
                "subquery": {
                  "alias": "zsubq_owner",
                  "limit": undefined,
                  "orderBy": undefined,
                  "related": undefined,
                  "schema": undefined,
                  "start": undefined,
                  "table": "user",
                  "where": undefined,
                },
                "system": "client",
              },
              "scalar": undefined,
              "type": "correlatedSubquery",
            },
          ],
          "type": "and",
        },
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": [
            [
              "id",
              "asc",
            ],
          ],
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": {
            "conditions": [
              {
                "left": {
                  "name": "id",
                  "type": "column",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "1",
                },
                "type": "simple",
              },
              {
                "conditions": [
                  {
                    "left": {
                      "name": "ownerId",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "2",
                    },
                    "type": "simple",
                  },
                  {
                    "flip": undefined,
                    "op": "EXISTS",
                    "related": {
                      "correlation": {
                        "childField": [
                          "issueId",
                        ],
                        "parentField": [
                          "id",
                        ],
                      },
                      "hidden": undefined,
                      "subquery": {
                        "alias": "zsubq_comments",
                        "limit": undefined,
                        "orderBy": [
                          [
                            "id",
                            "asc",
                          ],
                        ],
                        "related": undefined,
                        "schema": undefined,
                        "start": undefined,
                        "table": "comment",
                        "where": undefined,
                      },
                      "system": "client",
                    },
                    "scalar": undefined,
                    "type": "correlatedSubquery",
                  },
                ],
                "type": "or",
              },
              {
                "flip": undefined,
                "op": "EXISTS",
                "related": {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "ownerId",
                    ],
                  },
                  "hidden": undefined,
                  "subquery": {
                    "alias": "zsubq_owner",
                    "limit": undefined,
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "related": undefined,
                    "schema": undefined,
                    "start": undefined,
                    "table": "user",
                    "where": undefined,
                  },
                  "system": "client",
                },
                "scalar": undefined,
                "type": "correlatedSubquery",
              },
            ],
            "type": "and",
          },
        }
      `);
  });

  test('compound primary key preserves user-defined column order', () => {
    // This test verifies that completeOrdering preserves the order of
    // compound primary key columns as defined by the user, not alphabetically.
    // See: https://bugs.rocicorp.dev/p/zero/issue/246641
    // The bug was that lite-tables.ts called primaryKey.sort() which
    // alphabetized compound PK columns (e.g., ['callId', 'userId', 'connectionId']
    // became ['callId', 'connectionId', 'userId']), causing incorrect ORDER BY
    // and index column ordering.

    // Simulate the correct PK order as user defined it
    const userDefinedOrder = ['callId', 'userId', 'connectionId'] as const;

    const baseAST = {table: 'issueLabel'} as const;

    const resultWithCorrectOrder = completeOrdering(
      baseAST,
      () => userDefinedOrder,
    );

    // With correct user-defined order, PK columns should be in user-defined order
    expect(resultWithCorrectOrder.orderBy).toEqual([
      ['callId', 'asc'],
      ['userId', 'asc'],
      ['connectionId', 'asc'],
    ]);
  });
});
