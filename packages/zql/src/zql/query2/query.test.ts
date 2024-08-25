import {describe, expectTypeOf, test} from 'vitest';
import {Query} from './query.js';
import {Schema} from './schema.js';

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
  related() {
    return this;
  },
  where() {
    return this;
  },
};

type TestSchema = {
  table: 'test';
  fields: {
    s: {type: 'string'};
    b: {type: 'boolean'};
    n: {type: 'number'};
  };
  primaryKey: ['s'];
};

type TestSchemaWithRelationships = {
  table: 'testWithRelationships';
  fields: {
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
  table: 'testWithMoreRelationships';
  fields: {
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

    // Nothing selected? Return type is empty array.
    expectTypeOf(query.run()).toMatchTypeOf<readonly never[]>();

    const query2 = query.select('s');
    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        readonly s: string;
      }[]
    >();

    const query3 = query2.select('s', 'b', 'n');
    expectTypeOf(query3.run()).toMatchTypeOf<
      readonly {
        readonly s: string;
        readonly b: boolean;
        readonly n: number;
      }[]
    >();
  });

  test('related', () => {
    const query = mockQuery as unknown as Query<TestSchemaWithRelationships>;

    // @ts-expect-error - cannot select a field that does not exist
    query.related('test', q => q.select('a'));

    // @ts-expect-error - cannot traverse a relationship that does not exist
    query.related('doesNotExist', q => q);

    const query2 = query.related('test', q => q.select('b')).select('s');

    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        readonly s: string;
        readonly test: readonly {
          readonly b: boolean;
        }[];
      }[]
    >();

    // Many calls to related builds up the related object.
    const query3 =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;
    const t = query3
      .related('self', q => q.select('s'))
      .related('testWithRelationships', q => q.select('b'))
      .related('test', q => q.select('n'))
      .select('a')
      .run();
    expectTypeOf(t).toMatchTypeOf<
      readonly {
        a: string;
        self: readonly {
          s: string;
        }[];
        testWithRelationships: readonly {
          b: boolean;
        }[];
        test: readonly {
          n: number;
        }[];
      }[]
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
    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        s: string;
        self: readonly {
          s: string;
          test: readonly {
            b: boolean;
          }[];
        }[];
      }[]
    >();
  });

  test('where', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    const query2 = query.where('s', '=', 'foo');
    expectTypeOf(query2.run()).toMatchTypeOf<readonly never[]>();

    // @ts-expect-error - cannot use a field that does not exist
    query.where('doesNotExist', '=', 'foo');
    // @ts-expect-error - value and field types must match
    query.where('b', '=', 'false');

    expectTypeOf(query.select('b').where('b', '=', true).run()).toMatchTypeOf<
      readonly {
        b: boolean;
      }[]
    >();
  });
});

describe('schema structure', () => {
  test('dag', () => {
    const commentSchema = {
      table: 'comment',
      fields: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
    } as const;

    const issueSchema = {
      table: 'issue',
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
      primaryKey: ['id'],
    } as const;

    takeSchema(issueSchema);
  });

  test('cycle', () => {
    const commentSchema = {
      table: 'comment',
      primaryKey: ['id'],
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
      table: 'issue',
      primaryKey: ['id'],
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

function takeSchema(x: Schema) {
  return x;
}
