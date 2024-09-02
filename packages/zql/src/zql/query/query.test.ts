/* eslint-disable @typescript-eslint/ban-types */
import {describe, expectTypeOf, test} from 'vitest';
import {Query} from './query.js';
import {Schema} from './schema.js';

const mockQuery = {
  select() {
    return this;
  },
  materialize() {
    return {
      get() {
        return this;
      },
    };
  },
  sub() {
    return this;
  },
  related() {
    return this;
  },
  where() {
    return this;
  },
  start() {
    return this;
  },
};

type TestSchema = {
  tableName: 'test';
  columns: {
    s: {type: 'string'};
    b: {type: 'boolean'};
    n: {type: 'number'};
  };
  primaryKey: ['s'];
};

type TestSchemaWithRelationships = {
  tableName: 'testWithRelationships';
  columns: {
    s: {type: 'string'};
    a: {type: 'string'};
    b: {type: 'boolean'};
  };
  relationships: {
    test: {
      source: 's';
      dest: {
        field: 's';
        schema: TestSchema;
      };
    };
  };
  primaryKey: ['s'];
};

type TestSchemaWithMoreRelationships = {
  tableName: 'testWithMoreRelationships';
  columns: {
    s: {type: 'string'};
    a: {type: 'string'};
    b: {type: 'boolean'};
  };
  relationships: {
    testWithRelationships: {
      source: 'a';
      dest: {
        field: 'a';
        schema: TestSchemaWithRelationships;
      };
    };
    test: {
      source: 's';
      dest: {
        field: 's';
        schema: TestSchema;
      };
    };
    self: {
      source: 's';
      dest: {
        field: 's';
        schema: TestSchemaWithMoreRelationships;
      };
    };
  };
  primaryKey: ['s'];
};

describe('types', () => {
  test('simple select', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    // @ts-expect-error - cannot select a field that does not exist
    query.select('foo');

    const query2 = query.select('s');
    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      Array<{
        readonly s: string;
      }>
    >();

    const query3 = query2.select('s', 'b', 'n');
    expectTypeOf(query3.materialize().data).toMatchTypeOf<
      Array<{
        readonly s: string;
        readonly b: boolean;
        readonly n: number;
      }>
    >();

    // no select? All fields are returned.
    expectTypeOf(query.materialize().data).toMatchTypeOf<
      Array<{
        s: string;
        b: boolean;
        n: number;
      }>
    >();
  });

  test('related', () => {
    const query = mockQuery as unknown as Query<TestSchemaWithRelationships>;

    // @ts-expect-error - cannot select a field that does not exist
    query.related('test', q => q.select('a'));

    // @ts-expect-error - cannot traverse a relationship that does not exist
    query.related('doesNotExist', q => q);

    const query2 = query.related('test', q => q.select('b')).select('s');
    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      Array<{
        readonly s: string;
        readonly test: Array<{
          readonly b: boolean;
        }>;
      }>
    >();

    // Many calls to related builds up the related object.
    const query3 =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;
    const t = query3
      .related('self', q => q.select('s'))
      .related('testWithRelationships', q => q.select('b'))
      .related('test', q => q.select('n'))
      .select('a')
      .materialize().data;
    expectTypeOf(t).toMatchTypeOf<
      Array<{
        a: string;
        self: Array<{
          s: string;
        }>;
        testWithRelationships: Array<{
          b: boolean;
        }>;
        test: Array<{
          n: number;
        }>;
      }>
    >();
  });

  test('related in subquery position', () => {
    const query =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;

    const query2 = query
      .select('s')
      .related('self', query =>
        query.related('test', q => q.select('b')).select('s'),
      );

    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      Array<{
        s: string;
        self: Array<{
          s: string;
          test: Array<{
            b: boolean;
          }>;
        }>;
      }>
    >();
  });

  test('where', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    const query2 = query.where('s', '=', 'foo');
    expectTypeOf(query2.materialize().data).toMatchTypeOf<Array<{}>>();

    // @ts-expect-error - cannot use a field that does not exist
    query.where('doesNotExist', '=', 'foo');
    // @ts-expect-error - value and field types must match
    query.where('b', '=', 'false');

    expectTypeOf(
      query.select('b').where('b', '=', true).materialize().data,
    ).toMatchTypeOf<
      Array<{
        b: boolean;
      }>
    >();
  });

  test('start', () => {
    const query = mockQuery as unknown as Query<TestSchema>;
    const query2 = query.start({b: true, s: 'foo'});
    expectTypeOf(query2.materialize().data).toMatchTypeOf<Array<{}>>();
    const query3 = query.start({b: true, s: 'foo'}, {inclusive: true});
    expectTypeOf(query3.materialize().data).toMatchTypeOf<Array<{}>>();
  });
});

describe('schema structure', () => {
  test('dag', () => {
    const commentSchema = {
      tableName: 'comment',
      columns: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
    } as const;

    const issueSchema = {
      tableName: 'issue',
      columns: {
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
      primaryKey: ['id'],
    } as const;

    takeSchema(issueSchema);
  });

  test('cycle', () => {
    const commentSchema = {
      tableName: 'comment',
      primaryKey: ['id'],
      columns: {
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
      tableName: 'issue',
      primaryKey: ['id'],
      columns: {
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

function takeSchema(x: Schema) {
  return x;
}
