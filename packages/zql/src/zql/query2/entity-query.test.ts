import {describe, expectTypeOf, test} from 'vitest';
import {EntityQuery, EntitySchema} from './entity-query.js';

const mockQuery = {
  select() {
    return this;
  },
  run() {
    return this;
  },
  sub() {
    return this;
  },
};

type TestSchema = {
  fields: {
    s: {type: 'string'};
    b: {type: 'boolean'};
    n: {type: 'number'};
  };
};

describe('types', () => {
  test('simple select', () => {
    const query = mockQuery as unknown as EntityQuery<TestSchema>;

    // @ts-expect-error - cannot select a field that does not exist
    query.select('foo');

    // Nothing selected? Return type is empty array.
    expectTypeOf(query.run()).toMatchTypeOf<readonly []>();

    const query2 = query.select('s');
    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        readonly entity: {readonly s: string};
        subselects: never;
      }[]
    >();

    const query3 = query2.select('s', 'b', 'n');
    expectTypeOf(query3.run()).toMatchTypeOf<
      readonly {
        readonly entity: {
          readonly s: string;
          readonly b: boolean;
          readonly n: number;
        };
        subselects: never;
      }[]
    >();
  });

  test('subquery', () => {
    const query = mockQuery as unknown as EntityQuery<TestSchema>;

    const query2 = query
      .select('s')
      .sub(query => query.select('s', 'b').as('first'));
    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        entity: {s: string};
        subselects: {
          first: readonly {
            entity: {readonly s: string; readonly b: boolean};
            readonly subselects: never;
          }[];
        };
      }[]
    >();

    // @ts-expect-error - cannot select a field that does not exist even in subqueries
    query.sub(query => query.select('x'));

    // many subqueries
    const query3 = query2.sub(query => query.select('s', 'b').as('second'));
    expectTypeOf(query3.run()).toMatchTypeOf<
      readonly {
        entity: {s: string};
        subselects: {
          first: readonly {
            entity: {readonly s: string; readonly b: boolean};
            readonly subselects: never;
          }[];
          second: readonly {
            entity: {readonly s: string; readonly b: boolean};
            readonly subselects: never;
          }[];
        };
      }[]
    >();
  });
});

describe('schema structure', () => {
  test('dag', () => {
    const commentSchema = {
      fields: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        text: {type: 'string'},
      },
    } as const;

    const issueSchema = {
      fields: {
        id: {type: 'string'},
        title: {type: 'string'},
      },
      relationships: {
        comments: {
          source: 'id',
          dest: {
            field: 'issueId',
            schema: commentSchema,
          },
        },
      },
    } as const;

    takeSchema(issueSchema);
  });

  test('cycle', () => {
    const commentSchema = {
      fields: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        text: {type: 'string'},
      },
      relationships: {
        issue: {
          source: 'issueId',
          dest: {
            field: 'id',
            schema: () => issueSchema,
          },
        },
      },
    } as const;

    const issueSchema = {
      fields: {
        id: {type: 'string'},
        title: {type: 'string'},
        parentId: {type: 'string', optional: true},
      },
      relationships: {
        comments: {
          source: 'id',
          dest: {
            field: 'issueId',
            schema: commentSchema,
          },
        },
        parent: {
          source: 'parentId',
          dest: {
            field: 'id',
            schema: () => issueSchema,
          },
        },
      },
    } as const;

    takeSchema(issueSchema);
  });
});

function takeSchema(x: EntitySchema) {
  return x;
}
