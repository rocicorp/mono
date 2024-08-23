import {describe, expect, test} from 'vitest';
import {newEntityQuery} from './entity-query-impl.js';
import {Context} from '../context/context.js';

const mockContext = {} as Context;

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
  test('creates a new entity query', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    expect(issueQuery.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
    });
  });

  test('selecting fields does nothing to the ast', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const selected = issueQuery.select('id', 'title');
    expect(selected.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
    });
  });

  test('as sets an alias', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const aliased = issueQuery.as('i');
    expect(aliased.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      alias: 'i',
    });
  });

  test('where inserts a condition', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const where = issueQuery.where('id', '=', '1');
    expect(where.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      where: [{type: 'simple', field: 'id', op: '=', value: '1'}],
    });

    const where2 = where.where('title', '=', 'foo');
    expect(where2.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      where: [
        {type: 'simple', field: 'id', op: '=', value: '1'},
        {type: 'simple', field: 'title', op: '=', value: 'foo'},
      ],
    });
  });

  test('related: field edges', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const related = issueQuery.related('owner');
    expect(related.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      related: [
        {
          sourceField: 'ownerId',
          destField: 'id',
          destTable: 'user',
        },
      ],
    });
  });
  test('related: junction edges', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const related = issueQuery.related('labels');
    expect(related.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      related: [
        {
          sourceField: 'id',
          junctionTable: 'issueLabel',
          junctionSourceField: 'issueId',
          junctionDestField: 'labelId',
          destField: 'id',
          destTable: 'label',
        },
      ],
    });
  });
  test('related: many stacked edges', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const related = issueQuery
      .related('owner')
      .related('issues')
      .related('labels');
    expect(related.ast).toEqual({
      type: 'unmoored',
      table: 'issue',
      related: [
        {
          sourceField: 'ownerId',
          destField: 'id',
          destTable: 'user',
        },
        {
          sourceField: 'id',
          destField: 'ownerId',
          destTable: 'issue',
        },
        {
          sourceField: 'id',
          junctionTable: 'issueLabel',
          junctionSourceField: 'issueId',
          junctionDestField: 'labelId',
          destField: 'id',
          destTable: 'label',
        },
      ],
    });
  });

  test('subquery with stacked relationships', () => {
    const issueQuery = newEntityQuery(mockContext, issueSchema);
    const subquery = issueQuery
      .sub(iq => iq.related('comments').sub(cq => cq.related('revisions')))
      .sub(iq => iq.related('owner'))
      .sub(iq => iq.related('labels'));
    expect(subquery.ast).toEqual({
      subqueries: [
        {
          related: [
            {
              destField: 'issueId',
              destTable: 'comment',
              sourceField: 'id',
            },
          ],
          subqueries: [
            {
              related: [
                {
                  destField: 'commentId',
                  destTable: 'revision',
                  sourceField: 'id',
                },
              ],
              type: 'anchored',
            },
          ],
          type: 'anchored',
        },
        {
          related: [
            {
              destField: 'id',
              destTable: 'user',
              sourceField: 'ownerId',
            },
          ],
          type: 'anchored',
        },
        {
          related: [
            {
              destField: 'id',
              destTable: 'label',
              junctionDestField: 'labelId',
              junctionSourceField: 'issueId',
              junctionTable: 'issueLabel',
              sourceField: 'id',
            },
          ],
          type: 'anchored',
        },
      ],
      table: 'issue',
      type: 'unmoored',
    });
  });
});
