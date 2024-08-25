import {describe, expect, test} from 'vitest';
import {newQuery} from './query-impl.js';
import {Host} from '../builder/builder.js';

const mockHost = {} as Host;

const issueSchema = {
  table: 'issue',
  fields: {
    id: {type: 'string'},
    title: {type: 'string'},
    description: {type: 'string'},
    closed: {type: 'boolean'},
    ownerId: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    owner: {
      source: 'ownerId',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    comments: {
      source: 'id',
      dest: {
        field: 'issueId',
        schema: () => commentSchema,
      },
    },
    labels: {
      source: 'id',
      junction: {
        sourceField: 'issueId',
        destField: 'labelId',
        schema: () => issueLabelSchema,
      },
      dest: {
        field: 'id',
        schema: () => labelSchema,
      },
    },
  },
} as const;

const issueLabelSchema = {
  table: 'issueLabel',
  fields: {
    issueId: {type: 'string'},
    labelId: {type: 'string'},
  },
  primaryKey: ['issueId', 'labelId'],
} as const;

const labelSchema = {
  table: 'label',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      source: 'id',
      junction: {
        sourceField: 'labelId',
        destField: 'issueId',
      },
      dest: {
        field: 'id',
        schema: issueSchema,
      },
    },
  },
} as const;

const commentSchema = {
  table: 'comment',
  fields: {
    id: {type: 'string'},
    issueId: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issue: {
      source: 'issueId',
      dest: {
        field: 'id',
        schema: issueSchema,
      },
    },
    revisions: {
      source: 'id',
      dest: {
        field: 'commentId',
        schema: () => revisionSchema,
      },
    },
  },
} as const;

const revisionSchema = {
  table: 'revision',
  fields: {
    id: {type: 'string'},
    commentId: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    comment: {
      source: 'commentId',
      dest: {
        field: 'id',
        schema: commentSchema,
      },
    },
  },
} as const;

const userSchema = {
  table: 'user',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      source: 'id',
      dest: {
        field: 'ownerId',
        schema: issueSchema,
      },
    },
  },
} as const;

describe('building the AST', () => {
  test('creates a new query', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    expect(issueQuery.ast).toEqual({
      table: 'issue',
    });
  });

  test('selecting fields does nothing to the ast', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const selected = issueQuery.select('id', 'title');
    expect(selected.ast).toEqual({
      table: 'issue',
    });
  });

  test('as sets an alias', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const aliased = issueQuery.as('i');
    expect(aliased.ast).toEqual({
      table: 'issue',
      alias: 'i',
    });
  });

  test('where inserts a condition', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const where = issueQuery.where('id', '=', '1');
    expect(where.ast).toEqual({
      table: 'issue',
      where: [{type: 'simple', field: 'id', op: '=', value: '1'}],
    });

    const where2 = where.where('title', '=', 'foo');
    expect(where2.ast).toEqual({
      table: 'issue',
      where: [
        {type: 'simple', field: 'id', op: '=', value: '1'},
        {type: 'simple', field: 'title', op: '=', value: 'foo'},
      ],
    });
  });

  test('related: field edges', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const related = issueQuery.related('owner', q => q);
    expect(related.ast).toEqual({
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
    const issueQuery = newQuery(mockHost, issueSchema);
    const related = issueQuery.related('labels', q => q);
    expect(related.ast).toEqual({
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

  test('related: many stacked edges', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const related = issueQuery.related('owner', oq =>
      oq.related('issues', iq => iq.related('labels', lq => lq)),
    );
    expect(related.ast).toEqual({
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

  test('related: many siblings', () => {
    const issueQuery = newQuery(mockHost, issueSchema);
    const related = issueQuery
      .related('owner', oq => oq)
      .related('comments', cq => cq)
      .related('labels', lq => lq);
    expect(related.ast).toEqual({
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
