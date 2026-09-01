import {describe, expect, test} from 'vitest';
import {staticParam} from '../../../zero-permissions/src/permissions.ts';
import {
  normalizeAST,
  toStaticParam,
  type AST,
  type Condition,
} from '../../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import type {ExpressionFactory} from './expression.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import {type AnyQuery} from './query.ts';
import {newStaticQuery} from './static-query.ts';
import {schema} from './test/test-schemas.ts';

function ast(q: AnyQuery) {
  return asQueryInternals(q).ast;
}

describe('building the AST', () => {
  test('creates a new query', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(ast(issueQuery)).toEqual({
      table: 'issue',
    });
  });

  test('exists over junction with extra conditions', () => {
    const issueQuery = newQuery(schema, 'issue');
    const notExists = issueQuery.where(({exists}) =>
      exists('labels', q => q.where('id', '=', '1').where('name', '=', 'foo')),
    );
    expect(ast(notExists)).toMatchInlineSnapshot(`
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

  test('where inserts a condition', () => {
    const issueQuery = newQuery(schema, 'issue');
    const where = issueQuery.where('id', '=', '1');
    expect(ast(where)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
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
    const issueQuery = newQuery(schema, 'issue');
    const where = issueQuery
      .where('id', '1')
      .where('title', 'foo')
      .where('closed', true)
      .where('ownerId', '2');
    expect(ast(where)).toMatchInlineSnapshot(`
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
                "value": "1",
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

  test('start adds a start field', () => {
    const issueQuery = newQuery(schema, 'issue');
    const start = issueQuery.start({id: '1'});
    expect(ast(start)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": {
          "exclusive": true,
          "row": {
            "id": "1",
          },
        },
        "table": "issue",
        "where": undefined,
      }
    `);
    const start2 = issueQuery.start({id: '2', closed: true}, {inclusive: true});
    expect(ast(start2)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": {
          "exclusive": false,
          "row": {
            "closed": true,
            "id": "2",
          },
        },
        "table": "issue",
        "where": undefined,
      }
    `);
  });

  test('related: field edges', () => {
    const issueQuery = newQuery(schema, 'issue');
    const related = issueQuery.related('owner', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
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
            "hidden": undefined,
            "subquery": {
              "alias": "owner",
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
        ],
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": undefined,
      }
    `);
  });

  test('related: junction edges', () => {
    const issueQuery = newQuery(schema, 'issue');
    const related = issueQuery.related('labels', q => q);
    expect(ast(related)).toMatchInlineSnapshot(`
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
  });

  test('related: never stacked edges', () => {
    const issueQuery = newQuery(schema, 'issue');
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect(ast(related)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
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
            "hidden": undefined,
            "subquery": {
              "alias": "owner",
              "limit": undefined,
              "orderBy": undefined,
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
                  "hidden": undefined,
                  "subquery": {
                    "alias": "issues",
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
                  },
                  "system": "client",
                },
              ],
              "schema": undefined,
              "start": undefined,
              "table": "user",
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

  test('related: never siblings', () => {
    const issueQuery = newQuery(schema, 'issue');
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect(ast(related)).toMatchInlineSnapshot(`
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
            "hidden": undefined,
            "subquery": {
              "alias": "comments",
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
          {
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
              "alias": "owner",
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
        ],
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": undefined,
      }
    `);
  });
});

test('where expressions', () => {
  const issueQuery = newQuery(schema, 'issue');
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
              "value": "1",
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
            "value": "1",
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
            "value": "1",
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
      ],
      "type": "and",
    }
  `);
});

// flatten is pretty extensively tested in `expression.test.ts`
// but we should double-check that `where` uses `expression` rather than trying to
// mutate the AST itself.
test('where to dnf', () => {
  const issueQuery = newQuery(schema, 'issue');
  let flatten: AnyQuery = issueQuery
    .where('id', '=', '1')
    .where('closed', true);
  expect(ast(flatten).where).toMatchInlineSnapshot(`
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
            "value": "1",
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
            "value": "1",
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
            "value": "1",
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
  const issueQuery = newQuery(schema, 'issue');

  test('basics', () => {
    const expr = issueQuery.where(({cmp}) => cmp('id', '=', '1'));
    expect(ast(expr)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
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

    const f: ExpressionFactory<'issue', typeof schema> = eb =>
      eb.cmp('id', '2');
    const expr2 = issueQuery.where(f);
    expect(ast(expr2)).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
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
          "type": "or",
        },
      }
    `);

    expect(ast(issueQuery.where(({cmp, not}) => not(cmp('id', '=', '1')))))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
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
              "conditions": [
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
                    "value": "1",
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
                "value": "1",
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
          "alias": undefined,
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
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
          "alias": undefined,
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
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
                "value": "1",
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
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
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
    const issueQuery = newQuery(schema, 'issue');

    // full expression
    expect(ast(issueQuery.where(({exists}) => exists('owner'))))
      .toMatchInlineSnapshot(`
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
        }
      `);

    // shorthand
    expect(ast(issueQuery.whereExists('owner'))).toMatchInlineSnapshot(`
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
      }
    `);
  });

  test('field relationship with further conditions', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(ast(issueQuery.whereExists('owner', q => q.where('id', '1'))))
      .toMatchInlineSnapshot(`
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
            "scalar": undefined,
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
          "scalar": undefined,
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('junction edge', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(ast(issueQuery.whereExists('labels'))).toMatchInlineSnapshot(`
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
  });

  test('existence within an or branch', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery.where(({or, exists}) =>
          or(exists('owner'), exists('comments')),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
          "type": "or",
        },
      }
    `);
  });

  test('negated existence - permission', () => {
    const issueQuery = newStaticQuery(schema, 'issue');

    expect(ast(issueQuery.where(({not, exists}) => not(exists('comments')))))
      .toMatchInlineSnapshot(`
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
              "system": "permissions",
            },
            "scalar": undefined,
            "type": "correlatedSubquery",
          },
        }
      `);
  });

  test('negated existence over junction edge - permission', () => {
    const issueQuery = newStaticQuery(schema, 'issue');

    expect(
      ast(issueQuery.where(({not, exists}) => not(exists('labels')))),
    ).toMatchInlineSnapshot(
      `
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
                  "system": "permissions",
                },
                "scalar": undefined,
                "type": "correlatedSubquery",
              },
            },
            "system": "permissions",
          },
          "scalar": undefined,
          "type": "correlatedSubquery",
        },
      }
    `,
    );
  });

  test('many exists on different relationships', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(
      ast(
        issueQuery
          .whereExists('owner')
          .whereExists('comments')
          .whereExists('labels'),
      ),
    ).toMatchInlineSnapshot(`
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
  });

  test('exists with flip option - field relationship', () => {
    const issueQuery = newQuery(schema, 'issue');

    // Using whereExists with flip option
    expect(ast(issueQuery.whereExists('owner', {flip: true})))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": {
            "flip": true,
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
        }
      `);

    // Using exists in expression builder with flip option
    expect(
      ast(
        issueQuery.where(({exists}) =>
          exists('owner', undefined, {flip: true}),
        ),
      ),
    ).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": {
          "flip": true,
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
      }
    `);
  });

  test('exists with flip option - junction relationship', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(ast(issueQuery.whereExists('labels', {flip: true})))
      .toMatchInlineSnapshot(`
        {
          "alias": undefined,
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "issue",
          "where": {
            "flip": true,
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
                  "flip": true,
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
  });

  test('exists with flip option and callback', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery.whereExists('owner', q => q.where('id', '1'), {flip: true}),
      ),
    ).toMatchInlineSnapshot(`
      {
        "alias": undefined,
        "limit": undefined,
        "orderBy": undefined,
        "related": undefined,
        "schema": undefined,
        "start": undefined,
        "table": "issue",
        "where": {
          "flip": true,
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
          "scalar": undefined,
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('many exists on the same relationship', () => {
    const issueQuery = newQuery(schema, 'issue');
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
              "scalar": undefined,
              "type": "correlatedSubquery",
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
              "scalar": undefined,
              "type": "correlatedSubquery",
            },
          ],
          "type": "and",
        },
      }
    `);
  });
});

test('one in schema should not imply limit 1 in the ast -- the user needs to get this right so we do not degrade perf tracking extra data in take', () => {
  const issueQuery = newQuery(schema, 'issue');
  const q1 = issueQuery.related('owner');
  const q2 = issueQuery.related('comments');

  expect(ast(q1)).toMatchObject({
    table: 'issue',
    related: [
      {
        subquery: {table: 'user'},
      },
    ],
  });
  expect(ast(q2)).toMatchObject({
    table: 'issue',
    related: [
      {
        subquery: expect.toSatisfy(sq => sq.limit === undefined),
      },
    ],
  });
});

test('scalar option on two-hop relationship applies to inner condition', () => {
  const issueQuery = newQuery(schema, 'issue');

  expect(
    ast(
      issueQuery.whereExists('labels', q => q.where('id', '=', 'l1'), {
        scalar: true,
        flip: true,
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "alias": undefined,
      "limit": undefined,
      "orderBy": undefined,
      "related": undefined,
      "schema": undefined,
      "start": undefined,
      "table": "issue",
      "where": {
        "flip": true,
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
              "flip": true,
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
                  "where": {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "l1",
                    },
                    "type": "simple",
                  },
                },
                "system": "client",
              },
              "scalar": true,
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

describe('whereExists with scalar option', () => {
  test('basic scalar exists', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery.whereExists('owner', q => q.where('id', '=', 'u1'), {
          scalar: true,
        }),
      ),
    ).toMatchInlineSnapshot(`
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
              "where": {
                "left": {
                  "name": "id",
                  "type": "column",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "u1",
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          "scalar": true,
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('scalar with where condition on subquery', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery.whereExists('owner', q => q.where('id', '=', 'u1'), {
          scalar: true,
        }),
      ),
    ).toMatchInlineSnapshot(`
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
              "where": {
                "left": {
                  "name": "id",
                  "type": "column",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "u1",
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          "scalar": true,
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('scalar in and combinator', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery
          .whereExists('owner', q => q.where('id', '=', 'u1'), {scalar: true})
          .where('title', 'LIKE', '%bug%'),
      ).where,
    ).toMatchObject({
      type: 'and',
      // Normalization sorts simple conditions before correlated subqueries.
      conditions: [
        {
          type: 'simple',
          op: 'LIKE',
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
        },
      ],
    });
  });

  test('not(exists(..., {scalar: true}))', () => {
    const issueQuery = newQuery(schema, 'issue');

    expect(
      ast(
        issueQuery.where(({not, exists}) =>
          not(exists('owner', q => q.where('name', 'Alice'), {scalar: true})),
        ),
      ),
    ).toMatchInlineSnapshot(`
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
          "op": "NOT EXISTS",
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
              "where": {
                "left": {
                  "name": "name",
                  "type": "column",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "Alice",
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          "scalar": true,
          "type": "correlatedSubquery",
        },
      }
    `);
  });

  test('chained with where', () => {
    const issueQuery = newQuery(schema, 'issue');

    const q = issueQuery
      .whereExists('owner', q => q.where('id', '=', 'u1'), {scalar: true})
      .where('closed', false);

    expect(ast(q).where).toMatchObject({
      type: 'and',
      // Normalization sorts simple conditions before correlated subqueries.
      conditions: [
        {
          type: 'simple',
          op: '=',
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
        },
      ],
    });
  });
});

describe('normalized by construction', () => {
  // `normalizeAST` caches its result on the AST it is given, so round trip
  // through JSON to get an equivalent AST that it has not seen before and
  // therefore actually normalizes.
  function normalizedCopyOf(q: AnyQuery) {
    return normalizeAST(JSON.parse(JSON.stringify(ast(q))));
  }

  function expectNormalized(q: AnyQuery) {
    expect(JSON.stringify(ast(q))).toBe(JSON.stringify(normalizedCopyOf(q)));
    expect(asQueryInternals(q).hash()).toBe(hashOfAST(normalizedCopyOf(q)));
    // The JSON encoding says nothing about the fields that are undefined, so
    // check the shape (which V8 cares about) as well.
    expectNormalizedShape(ast(q));
  }

  const AST_FIELDS = [
    'schema',
    'table',
    'alias',
    'where',
    'related',
    'start',
    'limit',
    'orderBy',
  ];
  const RELATED_FIELDS = ['correlation', 'hidden', 'subquery', 'system'];

  function expectNormalizedShape(ast: AST) {
    expect(Object.keys(ast)).toEqual(AST_FIELDS);
    for (const related of ast.related ?? []) {
      expect(Object.keys(related)).toEqual(RELATED_FIELDS);
      expectNormalizedShape(related.subquery);
    }
    if (ast.where) {
      expectNormalizedSubqueryShapes(ast.where);
    }
  }

  function expectNormalizedSubqueryShapes(cond: Condition) {
    switch (cond.type) {
      case 'simple':
        break;
      case 'correlatedSubquery':
        expectNormalizedShape(cond.related.subquery);
        break;
      default:
        cond.conditions.forEach(expectNormalizedSubqueryShapes);
    }
  }

  const issueQuery = newQuery(schema, 'issue');

  test('empty query', () => {
    expectNormalized(issueQuery);
  });

  test('where conditions are sorted', () => {
    expectNormalized(
      issueQuery.where('title', 'foo').where('id', '1').where('closed', true),
    );
    expectNormalized(
      issueQuery.where(({and, cmp, or}) =>
        and(
          or(cmp('title', 'b'), cmp('title', 'a')),
          cmp('id', '2'),
          cmp('id', '1'),
        ),
      ),
    );
  });

  test('empty conjunctions are kept', () => {
    expectNormalized(issueQuery.where(({and}) => and()));
    expectNormalized(issueQuery.where(({or}) => or()));
  });

  test('related subqueries are sorted', () => {
    expectNormalized(
      issueQuery
        .related('owner')
        .related('comments', q => q.related('revisions').limit(10))
        .related('labels'),
    );
  });

  test('exists', () => {
    expectNormalized(
      issueQuery
        .whereExists('labels', q => q.where('name', 'bug'))
        .whereExists('comments')
        .where('closed', false),
    );
  });

  test('one, start, limit and orderBy', () => {
    expectNormalized(
      issueQuery
        .orderBy('createdAt', 'desc')
        .start({id: '1', createdAt: 1})
        .limit(10)
        .one(),
    );
  });

  test('static query with parameters', () => {
    const authParam = (field: string) => ({
      [toStaticParam]: () => staticParam('authData', field),
    });
    const staticQuery = newStaticQuery(schema, 'issue') as AnyQuery;
    expectNormalized(
      staticQuery.where(({cmpLit, and}) =>
        and(
          cmpLit(authParam('role'), '=', 'admin'),
          cmpLit(authParam('org'), '=', 'rocicorp'),
        ),
      ),
    );
  });
});
