import {describe, expect, test} from 'vitest';
import {newQuery, type QueryDelegate, QueryImpl} from './query-impl.js';
import {issueSchema} from './test/testSchemas.js';

const mockDelegate = {} as QueryDelegate;

describe('building the AST', () => {
  test('creates a new query', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    expect((issueQuery as unknown as QueryImpl<never, never>).ast).toEqual({
      table: 'issue',
    });
  });

  test('selecting fields does nothing to the ast', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const selected = issueQuery.select('id', 'title');
    expect((selected as unknown as QueryImpl<never, never>).ast).toEqual({
      table: 'issue',
    });
  });

  test('where inserts a condition', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const where = issueQuery.where('id', '=', '1');
    expect((where as unknown as QueryImpl<never, never>).ast).toEqual({
      table: 'issue',
      where: [{type: 'simple', field: 'id', op: '=', value: '1'}],
    });

    const where2 = where.where('title', '=', 'foo');
    expect((where2 as unknown as QueryImpl<never, never>).ast).toEqual({
      table: 'issue',
      where: [
        {type: 'simple', field: 'id', op: '=', value: '1'},
        {type: 'simple', field: 'title', op: '=', value: 'foo'},
      ],
    });
  });

  test('start adds a start field', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const start = issueQuery.start({id: '1'});
    expect((start as unknown as QueryImpl<never, never>).ast).toEqual({
      table: 'issue',
      start: {
        row: {id: '1'},
        exclusive: true,
      },
    });
    const start2 = issueQuery.start({id: '2', closed: true}, {inclusive: true});
    expect((start2 as unknown as QueryImpl<never, never>).ast).toEqual({
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
    expect((related as unknown as QueryImpl<never, never>).ast).toEqual({
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
    expect((related as unknown as QueryImpl<never, never>).ast).toEqual({
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

  test('related: mnever stacked edges', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect((related as unknown as QueryImpl<never, never>).ast).toEqual({
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

  test('related: mnever siblings', () => {
    const issueQuery = newQuery(mockDelegate, issueSchema);
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect((related as unknown as QueryImpl<never, never>).ast).toEqual({
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
