import {describe, expect, test} from 'vitest';
import type {ExpressionFactory} from './expression.ts';
import {ast, newQuery, type QueryDelegate} from './query-impl.ts';
import {staticQuery} from './static-query.ts';
import {schema} from './test/test-schemas.ts';

const mockDelegate = {} as QueryDelegate;

describe('building the AST', () => {
  test('creates a new query', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    expect(ast(issueQuery)).toEqual({
      table: 'issue',
    });
  });

  test('exists over junction with extra conditions', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const notExists = issueQuery.where(({exists}) =>
      exists('labels', q => q.where('id', '=', '1').where('name', '=', 'foo')),
    );
    expect(ast(notExists)).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
                          "left": {
                            "name": "name",
                            "type": "column",
                          },
                          "op": "=",
                          "right": {
                            "type": "literal",
                            "value": "foo",
                          },
                          "type": "simple",
                        },
                      ],
                      "type": "and",
                    },
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

  test('where inserts a condition', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const where = issueQuery.where('id', '=', '1');
    expect(ast(where)).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
      }
    `);

    const where2 = where.where('title', '=', 'foo');
    expect(ast(where2)).toMatchInlineSnapshot(`
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
              "left": {
                "name": "title",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "foo",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);
  });

  test('multiple WHERE calls result in a single top level AND', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const where = issueQuery
      .where('id', '1')
      .where('title', 'foo')
      .where('closed', true)
      .where('ownerId', '2');
    expect(ast(where)).toMatchInlineSnapshot(`
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
              "left": {
                "name": "title",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "foo",
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
                "value": true,
              },
              "type": "simple",
            },
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
          ],
          "type": "and",
        },
      }
    `);
  });

  test('start adds a start field', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const start = issueQuery.start({id: '1'});
    expect(ast(start)).toMatchInlineSnapshot(`
      {
        "start": {
          "exclusive": true,
          "row": {
            "id": "1",
          },
        },
        "table": "issue",
      }
    `);
    const start2 = issueQuery.start({id: '2', closed: true}, {inclusive: true});
    expect(ast(start2)).toMatchInlineSnapshot(`
      {
        "start": {
          "exclusive": false,
          "row": {
            "closed": true,
            "id": "2",
          },
        },
        "table": "issue",
      }
    `);
  });

  test('related: field edges', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const related = issueQuery.related('owner', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
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
              "limit": 1,
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
        ],
        "table": "issue",
      }
    `);
  });

  test('related: junction edges', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const related = issueQuery.related('labels', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
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

  test('related: never stacked edges', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect(ast(related)).toMatchInlineSnapshot(`
      {
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
              "limit": 1,
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
                      "ownerId",
                    ],
                    "parentField": [
                      "id",
                    ],
                  },
                  "subquery": {
                    "alias": "issues",
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
                  },
                  "system": "client",
                },
              ],
              "table": "user",
            },
            "system": "client",
          },
        ],
        "table": "issue",
      }
    `);
  });

  test('related: never siblings', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
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
              "limit": 1,
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
});

test('where expressions', () => {
  const issueQuery = newQuery(mockDelegate, schema, 'issue');
  expect(ast(issueQuery.where('id', '=', '1')).where).toMatchInlineSnapshot(`
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
    }
  `);
  expect(ast(issueQuery.where('id', '=', '1').where('closed', true)).where)
    .toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);
  expect(
    ast(
      issueQuery.where(({cmp, or}) =>
        or(cmp('id', '=', '1'), cmp('closed', true)),
      ),
    ).where,
  ).toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);
  expect(
    ast(
      issueQuery.where(({and, cmp, or}) =>
        or(cmp('id', '1'), and(cmp('closed', true), cmp('id', '2'))),
      ),
    ).where,
  ).toMatchInlineSnapshot(`
    {
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
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "left": {
                "name": "id",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "2",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      ],
      "type": "or",
    }
  `);
  expect(
    ast(
      issueQuery.where(({and, cmp}) =>
        and(cmp('id', '=', '1'), cmp('closed', true)),
      ),
    ).where,
  ).toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);

  expect(
    ast(
      issueQuery.where(({and, cmp, not}) =>
        not(and(cmp('id', '=', '1'), cmp('closed', true))),
      ),
    ).where,
  ).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "id",
            "type": "column",
          },
          "op": "!=",
          "right": {
            "type": "literal",
            "value": "1",
          },
          "type": "simple",
        },
        {
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "!=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);

  expect(
    ast(
      issueQuery.where(({cmp, not, or}) =>
        not(or(cmp('id', '=', '1'), cmp('closed', true))),
      ),
    ).where,
  ).toMatchInlineSnapshot(`
    {
      "conditions": [
        {
          "left": {
            "name": "id",
            "type": "column",
          },
          "op": "!=",
          "right": {
            "type": "literal",
            "value": "1",
          },
          "type": "simple",
        },
        {
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "!=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);
});

// flatten is pretty extensively tested in `expression.test.ts`
// but we should double-check that `where` uses `expression` rather than trying to
// mutate the AST itself.
test('where to dnf', () => {
  const issueQuery = newQuery(mockDelegate, schema, 'issue');
  let flatten = issueQuery.where('id', '=', '1').where('closed', true);
  expect(ast(flatten).where).toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);

  flatten = issueQuery.where('id', '=', '1');
  expect(ast(flatten).where).toMatchInlineSnapshot(`
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
    }
  `);

  flatten = issueQuery.where(({cmp, or}) =>
    or(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(flatten).where).toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "or",
    }
  `);

  flatten = issueQuery.where(({and, cmp}) =>
    and(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(flatten).where).toMatchInlineSnapshot(`
    {
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
          "left": {
            "name": "closed",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      ],
      "type": "and",
    }
  `);

  flatten = issueQuery.where(({and, cmp, or}) =>
    and(cmp('id', '=', '1'), or(cmp('closed', true), cmp('id', '2'))),
  );
  expect(ast(flatten).where).toMatchInlineSnapshot(`
    {
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
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "left": {
                "name": "id",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "2",
              },
              "type": "simple",
            },
          ],
          "type": "or",
        },
      ],
      "type": "and",
    }
  `);
});

describe('expression builder', () => {
  const issueQuery = newQuery(mockDelegate, schema, 'issue');

  test('basics', () => {
    const expr = issueQuery.where(({cmp}) => cmp('id', '=', '1'));
    expect(ast(expr)).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
      }
    `);

    const f: ExpressionFactory<typeof schema, 'issue'> = eb =>
      eb.cmp('id', '2');
    const expr2 = issueQuery.where(f);
    expect(ast(expr2)).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "left": {
            "name": "id",
            "type": "column",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "2",
          },
          "type": "simple",
        },
      }
    `);

    expect(
      ast(
        issueQuery.where(({cmp, and}) =>
          and(
            cmp('id', '=', '1'),
            cmp('closed', true),
            cmp('title', '=', 'foo'),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
              "left": {
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "left": {
                "name": "title",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "foo",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);

    expect(
      ast(
        issueQuery.where(({cmp, or}) =>
          or(
            cmp('id', '=', '1'),
            cmp('closed', true),
            cmp('title', '=', 'foo'),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
              "left": {
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "left": {
                "name": "title",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "foo",
              },
              "type": "simple",
            },
          ],
          "type": "or",
        },
      }
    `);

    expect(ast(issueQuery.where(({cmp, not}) => not(cmp('id', '=', '1')))))
      .toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "left": {
            "name": "id",
            "type": "column",
          },
          "op": "!=",
          "right": {
            "type": "literal",
            "value": "1",
          },
          "type": "simple",
        },
      }
    `);

    expect(
      ast(
        issueQuery.where(({cmp, and, not, or}) =>
          // (id = 1 AND closed = true) OR (id = 2 AND NOT (closed = true))
          or(
            and(cmp('id', '=', '1'), cmp('closed', true)),
            and(cmp('id', '=', '2'), not(cmp('closed', true))),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "conditions": [
            {
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
                  "left": {
                    "name": "closed",
                    "type": "column",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": true,
                  },
                  "type": "simple",
                },
              ],
              "type": "and",
            },
            {
              "conditions": [
                {
                  "left": {
                    "name": "id",
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
                  "left": {
                    "name": "closed",
                    "type": "column",
                  },
                  "op": "!=",
                  "right": {
                    "type": "literal",
                    "value": true,
                  },
                  "type": "simple",
                },
              ],
              "type": "and",
            },
          ],
          "type": "or",
        },
      }
    `);
  });

  test('empty and', () => {
    expect(ast(issueQuery.where(({and}) => and()))).toEqual({
      table: 'issue',
      where: {
        type: 'and',
        conditions: [],
      },
    });
  });

  test('empty or', () => {
    expect(ast(issueQuery.where(({or}) => or()))).toEqual({
      table: 'issue',
      where: {
        type: 'or',
        conditions: [],
      },
    });
  });

  test('undefined terms in and', () => {
    expect(
      ast(
        issueQuery.where(({and, cmp}) =>
          and(cmp('id', '=', '1'), undefined, cmp('closed', true)),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
              "left": {
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);
  });

  test('single and turns into simple', () => {
    expect(ast(issueQuery.where(({and, cmp}) => and(cmp('id', '=', '1')))))
      .toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
      }
    `);
  });

  test('single or turns into simple', () => {
    expect(ast(issueQuery.where(({cmp, or}) => or(cmp('id', '=', '1')))))
      .toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
      }
    `);
  });

  test('undefined terms in or', () => {
    expect(
      ast(
        issueQuery.where(({cmp, or}) =>
          or(cmp('id', '=', '1'), undefined, cmp('closed', true)),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
              "left": {
                "name": "closed",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
          ],
          "type": "or",
        },
      }
    `);
  });

  test('undef', () => {
    expect(
      ast(
        issueQuery.where(({and, cmp, or}) =>
          // (undefined OR undefined) AND (id = '1' OR id = '2')
          and(
            or(undefined, undefined),
            or(cmp('id', '=', '1'), cmp('id', '2')),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "conditions": [],
          "type": "or",
        },
      }
    `);
  });

  test('undef', () => {
    expect(
      ast(
        issueQuery.where(({and, cmp, or}) =>
          // (id = '1' AND undefined) OR (id = '1' AND undefined)

          or(
            and(cmp('id', '=', '1'), undefined),
            and(cmp('id', '=', '2'), undefined),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
              "left": {
                "name": "id",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "2",
              },
              "type": "simple",
            },
          ],
          "type": "or",
        },
      }
    `);
  });
});

describe('exists', () => {
  test('field relationship', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    // full expression
    expect(ast(issueQuery.where(({exists}) => exists('owner'))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
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
        }
      `);

    // shorthand
    expect(ast(issueQuery.whereExists('owner'))).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
      }
    `);
  });

  test('field relationship with further conditions', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    expect(ast(issueQuery.whereExists('owner', q => q.where('id', '1'))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
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
                "where": {
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
              },
              "system": "client",
            },
            "type": "correlatedSubquery",
          },
        }
      `);

    expect(
      ast(
        issueQuery.whereExists('owner', q =>
          q.where(({or, cmp}) => or(cmp('id', '1'), cmp('name', 'foo'))),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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
                    "left": {
                      "name": "name",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "foo",
                    },
                    "type": "simple",
                  },
                ],
                "type": "or",
              },
            },
            "system": "client",
          },
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('junction edge', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    expect(ast(issueQuery.whereExists('labels'))).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
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

  test('existence within an or branch', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    expect(
      ast(
        issueQuery.where(({or, exists}) =>
          or(exists('owner'), exists('comments')),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "conditions": [
            {
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
      }
    `);
  });

  test('negated existence - permission', () => {
    const issueQuery = staticQuery(schema, 'issue');

    expect(ast(issueQuery.where(({not, exists}) => not(exists('comments')))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
            "op": "NOT EXISTS",
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
              "system": "permissions",
            },
            "type": "correlatedSubquery",
          },
        }
      `);
  });

  test('negated existence - client', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    expect(() =>
      issueQuery.where(({not, exists}) => not(exists('comments'))),
    ).toThrowError(
      new Error(
        'not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438',
      ),
    );
  });

  test('negated existence over junction edge - permission', () => {
    const issueQuery = staticQuery(schema, 'issue');

    expect(
      ast(issueQuery.where(({not, exists}) => not(exists('labels')))),
    ).toMatchInlineSnapshot(
      `
      {
        "table": "issue",
        "where": {
          "op": "NOT EXISTS",
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
                  "system": "permissions",
                },
                "type": "correlatedSubquery",
              },
            },
            "system": "permissions",
          },
          "type": "correlatedSubquery",
        },
      }
    `,
    );
  });

  test('negated existence over junction edge - client', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');

    expect(() =>
      issueQuery.where(({not, exists}) => not(exists('labels'))),
    ).toThrowError(
      new Error(
        'not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438',
      ),
    );
  });

  test('many exists on different relationships', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    expect(
      ast(
        issueQuery
          .whereExists('owner')
          .whereExists('comments')
          .whereExists('labels'),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "conditions": [
            {
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
            {
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
          ],
          "type": "and",
        },
      }
    `);
  });

  test('many exists on the same relationship', () => {
    const issueQuery = newQuery(mockDelegate, schema, 'issue');
    expect(
      ast(
        issueQuery.where(({and, exists}) =>
          and(
            exists('owner', o => o.where('name', 'foo')),
            exists('owner', o => o.where('name', 'bar')),
          ),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "conditions": [
            {
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
                  "where": {
                    "left": {
                      "name": "name",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "foo",
                    },
                    "type": "simple",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
            {
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
                  "where": {
                    "left": {
                      "name": "name",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "bar",
                    },
                    "type": "simple",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
          ],
          "type": "and",
        },
      }
    `);
  });
});

test('one in schema should imply limit 1 in the ast', () => {
  const issueQuery = newQuery(mockDelegate, schema, 'issue');
  const q1 = issueQuery.related('owner');
  const q2 = issueQuery.related('comments');

  expect(ast(q1)).toMatchObject({
    table: 'issue',
    related: [
      {
        subquery: {limit: 1, table: 'user'},
      },
    ],
  });
  expect(ast(q2)).toMatchObject({
    table: 'issue',
    related: [
      {
        subquery: expect.toSatisfy(sq => !('limit' in sq)),
      },
    ],
  });
});
