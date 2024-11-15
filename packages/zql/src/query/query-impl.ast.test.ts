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
    expect(ast(where)).toEqual({
      table: 'issue',
      where: {type: 'simple', field: 'id', op: '=', value: '1'},
    });

    const where2 = where.where('title', '=', 'foo');
    expect(ast(where2)).toEqual({
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          {type: 'simple', field: 'id', op: '=', value: '1'},
          {type: 'simple', field: 'title', op: '=', value: 'foo'},
        ],
      },
    });
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
              "field": "id",
              "op": "=",
              "type": "simple",
              "value": "1",
            },
            {
              "field": "title",
              "op": "=",
              "type": "simple",
              "value": "foo",
            },
            {
              "field": "closed",
              "op": "=",
              "type": "simple",
              "value": true,
            },
            {
              "field": "ownerId",
              "op": "=",
              "type": "simple",
              "value": "2",
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
    expect(ast(start)).toEqual({
      table: 'issue',
      start: {
        row: {id: '1'},
        exclusive: true,
      },
    });
    const start2 = issueQuery.start({id: '2', closed: true}, {inclusive: true});
    expect(ast(start2)).toEqual({
      table: 'issue',
      start: {
        row: {id: '2', closed: true},
        exclusive: false,
      },
    });
  });

  test('related: field edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('owner', q => q);
    expect(ast(related)).toEqual({
      related: [
        {
          correlation: {
            childField: 'id',
            op: '=',
            parentField: 'ownerId',
          },
          subquery: {
            table: 'user',
            alias: 'owner',
            orderBy: [['id', 'asc']],
          },
        },
      ],
      table: 'issue',
    });
  });

  test('related: junction edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('labels', q => q);
    expect(ast(related)).toEqual({
      related: [
        {
          correlation: {
            childField: 'issueId',
            op: '=',
            parentField: 'id',
          },
          subquery: {
            related: [
              {
                correlation: {
                  childField: 'id',
                  op: '=',
                  parentField: 'labelId',
                },
                hidden: true,
                subquery: {
                  table: 'label',
                  alias: 'labels',
                  orderBy: [['id', 'asc']],
                },
              },
            ],
            table: 'issueLabel',
            alias: 'labels',
            orderBy: [
              ['issueId', 'asc'],
              ['labelId', 'asc'],
            ],
          },
        },
      ],
      table: 'issue',
    });
  });

  test('related: never stacked edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect(ast(related)).toEqual({
      related: [
        {
          correlation: {
            childField: 'id',
            op: '=',
            parentField: 'ownerId',
          },
          subquery: {
            related: [
              {
                correlation: {
                  childField: 'ownerId',
                  op: '=',
                  parentField: 'id',
                },
                subquery: {
                  related: [
                    {
                      correlation: {
                        childField: 'issueId',
                        op: '=',
                        parentField: 'id',
                      },
                      subquery: {
                        related: [
                          {
                            correlation: {
                              childField: 'id',
                              op: '=',
                              parentField: 'labelId',
                            },
                            hidden: true,
                            subquery: {
                              table: 'label',
                              alias: 'labels',
                              orderBy: [['id', 'asc']],
                            },
                          },
                        ],
                        table: 'issueLabel',
                        alias: 'labels',
                        orderBy: [
                          ['issueId', 'asc'],
                          ['labelId', 'asc'],
                        ],
                      },
                    },
                  ],
                  table: 'issue',
                  alias: 'issues',
                  orderBy: [['id', 'asc']],
                },
              },
            ],
            table: 'user',
            alias: 'owner',
            orderBy: [['id', 'asc']],
          },
        },
      ],
      table: 'issue',
    });
  });

  test('related: never siblings', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect(ast(related)).toEqual({
      related: [
        {
          correlation: {
            childField: 'id',
            op: '=',
            parentField: 'ownerId',
          },
          subquery: {
            table: 'user',
            alias: 'owner',
            orderBy: [['id', 'asc']],
          },
        },
        {
          correlation: {
            childField: 'issueId',
            op: '=',
            parentField: 'id',
          },
          subquery: {
            table: 'comment',
            alias: 'comments',
            orderBy: [['id', 'asc']],
          },
        },
        {
          correlation: {
            childField: 'issueId',
            op: '=',
            parentField: 'id',
          },
          subquery: {
            related: [
              {
                correlation: {
                  childField: 'id',
                  op: '=',
                  parentField: 'labelId',
                },
                hidden: true,
                subquery: {
                  table: 'label',
                  alias: 'labels',
                  orderBy: [['id', 'asc']],
                },
              },
            ],
            table: 'issueLabel',
            alias: 'labels',
            orderBy: [
              ['issueId', 'asc'],
              ['labelId', 'asc'],
            ],
          },
        },
      ],
      table: 'issue',
    });
  });
});

test('where expressions', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);
  expect(ast(issueQuery.where('id', '=', '1')).where).toEqual({
    type: 'simple',
    field: 'id',
    op: '=',
    value: '1',
  });
  expect(
    ast(issueQuery.where('id', '=', '1').where('closed', true)).where,
  ).toEqual({
    type: 'and',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });
  expect(
    ast(
      issueQuery.where(({cmp, or}) =>
        or(cmp('id', '=', '1'), cmp('closed', true)),
      ),
    ).where,
  ).toEqual({
    type: 'or',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });
  expect(
    ast(
      issueQuery.where(({and, cmp, or}) =>
        or(cmp('id', '1'), and(cmp('closed', true), cmp('id', '2'))),
      ),
    ).where,
  ).toEqual({
    type: 'or',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {
        type: 'and',
        conditions: [
          {type: 'simple', field: 'closed', op: '=', value: true},
          {type: 'simple', field: 'id', op: '=', value: '2'},
        ],
      },
    ],
  });
  expect(
    ast(
      issueQuery.where(({and, cmp}) =>
        and(cmp('id', '=', '1'), cmp('closed', true)),
      ),
    ).where,
  ).toEqual({
    type: 'and',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });

  expect(
    ast(
      issueQuery.where(({and, cmp, not}) =>
        not(and(cmp('id', '=', '1'), cmp('closed', true))),
      ),
    ).where,
  ).toEqual({
    type: 'or',
    conditions: [
      {type: 'simple', field: 'id', op: '!=', value: '1'},
      {type: 'simple', field: 'closed', op: '!=', value: true},
    ],
  });

  expect(
    ast(
      issueQuery.where(({cmp, not, or}) =>
        not(or(cmp('id', '=', '1'), cmp('closed', true))),
      ),
    ).where,
  ).toEqual({
    type: 'and',
    conditions: [
      {type: 'simple', field: 'id', op: '!=', value: '1'},
      {type: 'simple', field: 'closed', op: '!=', value: true},
    ],
  });
});

// DNF conversion is pretty extensively tested in `expression.test.ts`
// but we should double-check that `where` uses `expression` rather than trying to
// mutate the AST itself.
test('where to dnf', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);
  let dnf = issueQuery.where('id', '=', '1').where('closed', true);
  expect(ast(dnf).where).toEqual({
    type: 'and',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });

  dnf = issueQuery.where('id', '=', '1');
  expect(ast(dnf).where).toEqual({
    type: 'simple',
    field: 'id',
    op: '=',
    value: '1',
  });

  dnf = issueQuery.where(({cmp, or}) =>
    or(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(dnf).where).toEqual({
    type: 'or',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });

  dnf = issueQuery.where(({and, cmp}) =>
    and(cmp('id', '=', '1'), cmp('closed', true)),
  );
  expect(ast(dnf).where).toEqual({
    type: 'and',
    conditions: [
      {type: 'simple', field: 'id', op: '=', value: '1'},
      {type: 'simple', field: 'closed', op: '=', value: true},
    ],
  });

  dnf = issueQuery.where(({and, cmp, or}) =>
    and(cmp('id', '=', '1'), or(cmp('closed', true), cmp('id', '2'))),
  );
  expect(ast(dnf).where).toEqual({
    type: 'or',
    conditions: [
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            field: 'id',
            op: '=',
            value: '1',
          },
          {
            type: 'simple',
            field: 'closed',
            op: '=',
            value: true,
          },
        ],
      },
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            field: 'id',
            op: '=',
            value: '1',
          },
          {
            type: 'simple',
            field: 'id',
            op: '=',
            value: '2',
          },
        ],
      },
    ],
  });
});

describe('expression builder', () => {
  const issueQuery = newQuery(mockDelegate, issueSchema);

  test('basics', () => {
    const expr = issueQuery.where(({cmp}) => cmp('id', '=', '1'));
    expect(ast(expr)).toEqual({
      table: 'issue',
      where: {
        type: 'simple',
        field: 'id',
        op: '=',
        value: '1',
      },
    });

    type IssueSchema = typeof issueSchema;
    const f: ExpressionFactory<IssueSchema> = eb => eb.cmp('id', '2');
    const expr2 = issueQuery.where(f);
    expect(ast(expr2)).toEqual({
      table: 'issue',
      where: {
        type: 'simple',
        field: 'id',
        op: '=',
        value: '2',
      },
    });

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
    ).toEqual({
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          {
            field: 'id',
            op: '=',
            type: 'simple',
            value: '1',
          },
          {
            field: 'closed',
            op: '=',
            type: 'simple',
            value: true,
          },
          {
            field: 'title',
            op: '=',
            type: 'simple',
            value: 'foo',
          },
        ],
      },
    });

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
    ).toEqual({
      table: 'issue',
      where: {
        type: 'or',
        conditions: [
          {
            field: 'id',
            op: '=',
            type: 'simple',
            value: '1',
          },
          {
            field: 'closed',
            op: '=',
            type: 'simple',
            value: true,
          },
          {
            field: 'title',
            op: '=',
            type: 'simple',
            value: 'foo',
          },
        ],
      },
    });

    expect(
      ast(issueQuery.where(({cmp, not}) => not(cmp('id', '=', '1')))),
    ).toEqual({
      table: 'issue',
      where: {
        field: 'id',
        op: '!=',
        type: 'simple',
        value: '1',
      },
    });

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
    ).toEqual({
      table: 'issue',
      where: {
        type: 'or',
        conditions: [
          {
            type: 'and',
            conditions: [
              {
                field: 'id',
                op: '=',
                type: 'simple',
                value: '1',
              },
              {
                field: 'closed',
                op: '=',
                type: 'simple',
                value: true,
              },
            ],
          },
          {
            type: 'and',
            conditions: [
              {
                field: 'id',
                op: '=',
                type: 'simple',
                value: '2',
              },
              {
                field: 'closed',
                op: '!=',
                type: 'simple',
                value: true,
              },
            ],
          },
        ],
      },
    });
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
    ).toEqual({
      table: 'issue',
      where: {
        type: 'and',
        conditions: [
          {
            field: 'id',
            op: '=',
            type: 'simple',
            value: '1',
          },
          {
            field: 'closed',
            op: '=',
            type: 'simple',
            value: true,
          },
        ],
      },
    });
  });

  test('single and turns into simple', () => {
    expect(
      ast(issueQuery.where(({and, cmp}) => and(cmp('id', '=', '1')))),
    ).toEqual({
      table: 'issue',
      where: {
        field: 'id',
        op: '=',
        type: 'simple',
        value: '1',
      },
    });
  });

  test('single or turns into simple', () => {
    expect(
      ast(issueQuery.where(({cmp, or}) => or(cmp('id', '=', '1')))),
    ).toEqual({
      table: 'issue',
      where: {
        field: 'id',
        op: '=',
        type: 'simple',
        value: '1',
      },
    });
  });

  test('undefined terms in or', () => {
    expect(
      ast(
        issueQuery.where(({cmp, or}) =>
          or(cmp('id', '=', '1'), undefined, cmp('closed', true)),
        ),
      ),
    ).toEqual({
      table: 'issue',
      where: {
        type: 'or',
        conditions: [
          {
            field: 'id',
            op: '=',
            type: 'simple',
            value: '1',
          },
          {
            field: 'closed',
            op: '=',
            type: 'simple',
            value: true,
          },
        ],
      },
    });
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
              "field": "id",
              "op": "=",
              "type": "simple",
              "value": "1",
            },
            {
              "field": "id",
              "op": "=",
              "type": "simple",
              "value": "2",
            },
          ],
          "type": "or",
        },
      }
    `);
  });
});
