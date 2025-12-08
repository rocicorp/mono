import {describe, expect, test} from 'vitest';
import type {TableSchema} from '../../../zero-types/src/schema.ts';
import {completeOrdering} from './complete-ordering.ts';
import {type AnyQuery} from './query-builder.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
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
        "table": "issue",
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "id",
            "asc",
          ],
        ],
        "table": "issue",
      }
    `);
  });

  test('basic, ordered on non primary key', () => {
    const issueQuery = newQuery(schema, 'issue').orderBy('title', 'asc');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "title",
            "asc",
          ],
        ],
        "table": "issue",
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
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
        "table": "issue",
      }
    `);
  });

  test('basic, partial order', () => {
    const q = newQuery(schema, 'issueLabel').orderBy('labelId', 'asc');
    expect(ast(q)).toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "labelId",
            "asc",
          ],
        ],
        "table": "issueLabel",
      }
    `);
    expect(completeOrdering(ast(q), getPrimaryKey)).toMatchInlineSnapshot(`
      {
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
        "table": "issueLabel",
      }
    `);

    const q2 = newQuery(schema, 'issueLabel').orderBy('issueId', 'asc');
    expect(ast(q2)).toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "issueId",
            "asc",
          ],
        ],
        "table": "issueLabel",
      }
    `);
    expect(completeOrdering(ast(q2), getPrimaryKey)).toMatchInlineSnapshot(`
      {
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
        "table": "issueLabel",
      }
    `);
  });

  test('related', () => {
    const issueQuery = newQuery(schema, 'issue').related('labels');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
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
        "table": "issue",
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
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
                  "subquery": {
                    "alias": "labels",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
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
        "table": "issue",
      }
    `);
  });

  test('exists', () => {
    const issueQuery = newQuery(schema, 'issue').whereExists('labels');
    expect(ast(issueQuery)).toMatchInlineSnapshot(`
      {
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
            "subquery": {
              "alias": "zsubq_labels",
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
                  "subquery": {
                    "alias": "zsubq_zhidden_labels",
                    "table": "label",
                  },
                  "system": "client",
                },
                "type": "correlatedSubquery",
              },
            },
            "system": "client",
          },
          "type": "correlatedSubquery",
        },
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "id",
            "asc",
          ],
        ],
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
            "subquery": {
              "alias": "zsubq_labels",
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
                  "subquery": {
                    "alias": "zsubq_zhidden_labels",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "label",
                  },
                  "system": "client",
                },
                "type": "correlatedSubquery",
              },
            },
            "system": "client",
          },
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
                "subquery": {
                  "alias": "zsubq_owner",
                  "table": "user",
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
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
                    "subquery": {
                      "alias": "zsubq_comments",
                      "table": "comment",
                    },
                    "system": "client",
                  },
                  "type": "correlatedSubquery",
                },
              ],
              "type": "or",
            },
          ],
          "type": "and",
        },
      }
    `);
    expect(completeOrdering(ast(issueQuery), getPrimaryKey))
      .toMatchInlineSnapshot(`
      {
        "orderBy": [
          [
            "id",
            "asc",
          ],
        ],
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
                "subquery": {
                  "alias": "zsubq_owner",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "user",
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
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
                    "subquery": {
                      "alias": "zsubq_comments",
                      "orderBy": [
                        [
                          "id",
                          "asc",
                        ],
                      ],
                      "table": "comment",
                    },
                    "system": "client",
                  },
                  "type": "correlatedSubquery",
                },
              ],
              "type": "or",
            },
          ],
          "type": "and",
        },
      }
    `);
  });
});
