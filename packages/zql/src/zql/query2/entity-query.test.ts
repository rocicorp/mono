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
  related() {
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

type TestSchemaWithRelationships = {
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
};

type TestSchemaWithMoreRelationships = {
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

  test('related', () => {
    const query =
      mockQuery as unknown as EntityQuery<TestSchemaWithRelationships>;

    // @ts-expect-error - cannot select a field that does not exist. We moved to `related` and `a` does not exist there
    query.related('test').select('a');

    // @ts-expect-error - cannot traverse a relationship that does not exist
    query.related('doesNotExist');

    const query2 = query.related('test').select('s');

    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        entity: {s: string};
        subselects: never;
      }[]
    >();

    // The semantics of `related` currently implemented is that we move
    // to the related entity without adding levels of nesting.
    const query3 =
      mockQuery as unknown as EntityQuery<TestSchemaWithMoreRelationships>;
    const t = query3
      .related('self')
      .related('testWithRelationships')
      .related('test')
      .select('n')
      .run();
    expectTypeOf(t).toMatchTypeOf<
      readonly {entity: {n: number}; subselects: never}[]
    >();
  });

  test('related in subquery position', () => {
    const query =
      mockQuery as unknown as EntityQuery<TestSchemaWithRelationships>;

    const query2 = query
      .select('s')
      .sub(query => query.related('test').select('s'));

    expectTypeOf(query2.run()).toMatchTypeOf<
      readonly {
        entity: {s: string};
        subselects: {
          test: readonly {
            entity: {s: string};
            subselects: never;
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
