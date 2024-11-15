import {describe, expect, test} from 'vitest';
import type {TableSchema} from '../../../zero-schema/src/table-schema.js';
import type {ExpressionFactory} from './expression.js';
import {
  astForTestingSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from './query-impl.js';
import type {Query, QueryType} from './query.js';
import {issueSchema} from './test/testSchemas.js';

const mockDelegate = {} as QueryDelegate;

function ast(q: Query<TableSchema, QueryType>) {
  return (q as QueryImpl<TableSchema, QueryType>)[astForTestingSymbol];
}

describe('building the AST', () => {
  test('creates a new query', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    expect(ast(issueQuery)).toEqual({
      table: 'issue',
    });
  });

  test('where inserts a condition', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
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
    const issueQuery = newQuery(mockDelegate, issueSchema);
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
    const issueQuery = newQuery(mockDelegate, issueSchema);
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
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('owner', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "ownerId",
            },
            "subquery": {
              "alias": "owner",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "table": "user",
            },
          },
        ],
        "table": "issue",
      }
    `);
  });

  test('related: junction edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('labels', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "issueId",
              "op": "=",
              "parentField": "id",
            },
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
                    "childField": "id",
                    "op": "=",
                    "parentField": "labelId",
                  },
                  "hidden": true,
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
                },
              ],
              "table": "issueLabel",
            },
          },
        ],
        "table": "issue",
      }
    `);
  });

  test('related: never stacked edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "ownerId",
            },
            "subquery": {
              "alias": "owner",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": [
                {
                  "correlation": {
                    "childField": "ownerId",
                    "op": "=",
                    "parentField": "id",
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
                          "childField": "issueId",
                          "op": "=",
                          "parentField": "id",
                        },
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
                                "childField": "id",
                                "op": "=",
                                "parentField": "labelId",
                              },
                              "hidden": true,
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
                            },
                          ],
                          "table": "issueLabel",
                        },
                      },
                    ],
                    "table": "issue",
                  },
                },
              ],
              "table": "user",
            },
          },
        ],
        "table": "issue",
      }
    `);
  });

  test('related: never siblings', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "ownerId",
            },
            "subquery": {
              "alias": "owner",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "table": "user",
            },
          },
          {
            "correlation": {
              "childField": "issueId",
              "op": "=",
              "parentField": "id",
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
          },
          {
            "correlation": {
              "childField": "issueId",
              "op": "=",
              "parentField": "id",
            },
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
                    "childField": "id",
                    "op": "=",
                    "parentField": "labelId",
                  },
                  "hidden": true,
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
                },
              ],
              "table": "issueLabel",
            },
          },
        ],
        "table": "issue",
      }
    `);
  });
});

test('where expressions', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);
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

// DNF conversion is pretty extensively tested in `expression.test.ts`
// but we should double-check that `where` uses `expression` rather than trying to
// mutate the AST itself.
test('where to dnf', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);
  let dnf = issueQuery.where('id', '=', '1').where('closed', true);
  expect(ast(dnf).where).toMatchInlineSnapshot(`
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

  dnf = issueQuery.where('id', '=', '1');
  expect(ast(dnf).where).toMatchInlineSnapshot(`
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

  dnf = issueQuery.where(({cmp, or}) =>
    or(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(dnf).where).toMatchInlineSnapshot(`
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

  dnf = issueQuery.where(({and, cmp}) =>
    and(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(dnf).where).toMatchInlineSnapshot(`
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

  dnf = issueQuery.where(({and, cmp, or}) =>
    and(cmp('id', '=', '1'), or(cmp('closed', true), cmp('id', '2'))),
  );
  expect(ast(dnf).where).toMatchInlineSnapshot(`
    {
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
          "type": "and",
        },
      ],
      "type": "or",
    }
  `);
});

describe('expression builder', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);

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

    type IssueSchema = typeof issueSchema;
    const f: ExpressionFactory<IssueSchema> = eb => eb.cmp('id', '2');
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
    const issueQuery = newQuery(mockDelegate, issueSchema);

    // full expression
    expect(ast(issueQuery.where(({exists}) => exists('owner'))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": "id",
                "op": "=",
                "parentField": "ownerId",
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
              "childField": "id",
              "op": "=",
              "parentField": "ownerId",
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
          },
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('field relationship with further conditions', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);

    expect(ast(issueQuery.whereExists('owner', q => q.where('id', '1'))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": "id",
                "op": "=",
                "parentField": "ownerId",
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
              "childField": "id",
              "op": "=",
              "parentField": "ownerId",
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
          },
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('junction edge', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);

    expect(ast(issueQuery.whereExists('labels'))).toMatchInlineSnapshot(`
      {
        "table": "issue",
        "where": {
          "op": "EXISTS",
          "related": {
            "correlation": {
              "childField": "issueId",
              "op": "=",
              "parentField": "id",
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
                    "childField": "id",
                    "op": "=",
                    "parentField": "labelId",
                  },
                  "subquery": {
                    "alias": "zsubq_labels",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "table": "label",
                  },
                },
                "type": "correlatedSubquery",
              },
            },
          },
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('existence within an or branch', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);

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
                  "childField": "id",
                  "op": "=",
                  "parentField": "ownerId",
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
              },
              "type": "correlatedSubquery",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": "issueId",
                  "op": "=",
                  "parentField": "id",
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
              },
              "type": "correlatedSubquery",
            },
          ],
          "type": "or",
        },
      }
    `);
  });

  test('negated existence', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);

    expect(ast(issueQuery.where(({not, exists}) => not(exists('comments')))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
            "op": "NOT EXISTS",
            "related": {
              "correlation": {
                "childField": "issueId",
                "op": "=",
                "parentField": "id",
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
            },
            "type": "correlatedSubquery",
          },
        }
      `);
  });

  test('negated existence over junction edge', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);

    expect(ast(issueQuery.where(({not, exists}) => not(exists('labels')))))
      .toMatchInlineSnapshot(`
        {
          "table": "issue",
          "where": {
            "op": "NOT EXISTS",
            "related": {
              "correlation": {
                "childField": "issueId",
                "op": "=",
                "parentField": "id",
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
                      "childField": "id",
                      "op": "=",
                      "parentField": "labelId",
                    },
                    "subquery": {
                      "alias": "zsubq_labels",
                      "orderBy": [
                        [
                          "id",
                          "asc",
                        ],
                      ],
                      "table": "label",
                    },
                  },
                  "type": "correlatedSubquery",
                },
              },
            },
            "type": "correlatedSubquery",
          },
        }
      `);
  });

  test('many exists on different relationships', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
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
                  "childField": "id",
                  "op": "=",
                  "parentField": "ownerId",
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
              },
              "type": "correlatedSubquery",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": "issueId",
                  "op": "=",
                  "parentField": "id",
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
              },
              "type": "correlatedSubquery",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": "issueId",
                  "op": "=",
                  "parentField": "id",
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
                        "childField": "id",
                        "op": "=",
                        "parentField": "labelId",
                      },
                      "subquery": {
                        "alias": "zsubq_labels",
                        "orderBy": [
                          [
                            "id",
                            "asc",
                          ],
                        ],
                        "table": "label",
                      },
                    },
                    "type": "correlatedSubquery",
                  },
                },
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
    const issueQuery = newQuery(mockDelegate, issueSchema);
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
                  "childField": "id",
                  "op": "=",
                  "parentField": "ownerId",
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
              },
              "type": "correlatedSubquery",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": "id",
                  "op": "=",
                  "parentField": "ownerId",
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
