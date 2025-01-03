/* eslint-disable @typescript-eslint/ban-types */
import {describe, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.js';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
} from '../../../zero-schema/src/column.js';
import {
  type Supertype,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import type {ExpressionFactory} from './expression.js';
import {staticParam} from './query-impl.js';
import type {AdvancedQuery} from './query-internal.js';
import {type Query, type QueryType, type Row} from './query.js';

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
  one() {
    return this;
  },
  run() {
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
  relationships: {};
};

type SchemaWithEnums = {
  tableName: 'testWithEnums';
  columns: {
    s: {type: 'string'};
    e: {kind: 'enum'; type: 'string'; customType: 'open' | 'closed'};
  };
  primaryKey: ['s'];
  relationships: {
    self: {
      sourceField: ['s'];
      destField: ['s'];
      destSchema: SchemaWithEnums;
    };
  };
};

type Opaque<BaseType, BrandType = unknown> = BaseType & {
  readonly [base]: BaseType;
  readonly [brand]: BrandType;
};

declare const base: unique symbol;
declare const brand: unique symbol;

type Timestamp = Opaque<number>;
type IdOf<T> = Opaque<string, T>;

function timestamp(n: number): Timestamp {
  return n as Timestamp;
}

const schemaWithAdvancedTypes = {
  tableName: 'schemaWithAdvancedTypes',
  columns: {
    s: string(),
    n: number<Timestamp>(),
    b: boolean(),
    j: json<{foo: string; bar: boolean}>(),
    e: enumeration<'open' | 'closed'>(),
    otherId: string<IdOf<SchemaWithEnums>>(),
  },
  primaryKey: ['s'],
  relationships: {
    self: {
      sourceField: ['s'],
      destField: ['s'],
      destSchema: () => schemaWithAdvancedTypes,
    },
  },
} as const;

type SchemaWithJson = {
  tableName: 'testWithJson';
  columns: {
    a: {type: 'string'};
    j: {type: 'json'};
    maybeJ: {type: 'json'; optional: true};
  };
  primaryKey: ['a'];
  relationships: {};
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
      sourceField: ['s'];
      destField: ['s'];
      destSchema: TestSchema;
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
      sourceField: ['a'];
      destField: ['a'];
      destSchema: TestSchemaWithRelationships;
    };
    test: {
      sourceField: ['s'];
      destField: ['s'];
      destSchema: TestSchema;
    };
    self: {
      sourceField: ['s'];
      destField: ['s'];
      destSchema: TestSchemaWithMoreRelationships;
    };
  };
  primaryKey: ['s'];
};

describe('types', () => {
  test('simple select', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    // no select? All fields are returned.
    expectTypeOf(query.materialize().data).toMatchTypeOf<
      ReadonlyArray<Row<TestSchema>>
    >();
  });

  test('simple select with enums', () => {
    const query = mockQuery as unknown as Query<SchemaWithEnums>;
    expectTypeOf(query.run()).toMatchTypeOf<
      ReadonlyArray<{
        s: string;
        e: 'open' | 'closed';
      }>
    >();

    const q2 = mockQuery as unknown as Query<typeof schemaWithAdvancedTypes>;
    q2.where('e', '=', 'open');
    // @ts-expect-error - invalid enum value
    q2.where('e', 'bogus');
    expectTypeOf(q2.run()).toMatchTypeOf<
      ReadonlyArray<{
        s: string;
        n: Timestamp;
        b: boolean;
        j: {foo: string; bar: boolean};
        e: 'open' | 'closed';
        otherId: IdOf<SchemaWithEnums>;
      }>
    >();

    // @ts-expect-error - 'foo' is not an id of `SchemaWithEnums`
    q2.where('otherId', '=', 'foo');

    // @ts-expect-error - 42 is not a timestamp
    q2.where('n', '>', 42);

    q2.where('n', '>', timestamp(42));
  });

  test('related with advanced types', () => {
    const query = mockQuery as unknown as Query<typeof schemaWithAdvancedTypes>;

    const query2 = query.related('self');
    expectTypeOf(query2.run()).toMatchTypeOf<
      ReadonlyArray<{
        s: string;
        n: Timestamp;
        b: boolean;
        j: {foo: string; bar: boolean};
        e: 'open' | 'closed';
        otherId: IdOf<SchemaWithEnums>;
        self: ReadonlyArray<{
          s: string;
          n: Timestamp;
          b: boolean;
          j: {foo: string; bar: boolean};
          e: 'open' | 'closed';
          otherId: IdOf<SchemaWithEnums>;
        }>;
      }>
    >();

    // @ts-expect-error - missing enum value
    query2.related('self', sq => sq.where('e', 'bogus'));
    query2.related('self', sq => sq.where('e', 'open'));
    query2.related('self', sq =>
      sq.related('self', sq => sq.where('e', 'open')),
    );
  });

  test('related', () => {
    const query = mockQuery as unknown as Query<TestSchemaWithRelationships>;

    // @ts-expect-error - cannot traverse a relationship that does not exist
    query.related('doesNotExist', q => q);

    const query2 = query.related('test');

    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      ReadonlyArray<
        Row<TestSchemaWithMoreRelationships> & {
          test: ReadonlyArray<Row<TestSchema>>;
        }
      >
    >();

    // Many calls to related builds up the related object.
    const query3 =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;
    const t = query3
      .related('self')
      .related('testWithRelationships')
      .related('test')
      .materialize().data;
    expectTypeOf(t).toMatchTypeOf<
      ReadonlyArray<{
        a: string;
        self: ReadonlyArray<{
          s: string;
        }>;
        testWithRelationships: ReadonlyArray<{
          b: boolean;
        }>;
        test: ReadonlyArray<{
          n: number;
        }>;
      }>
    >();
  });

  test('related with enums', () => {
    const query = mockQuery as unknown as Query<SchemaWithEnums>;

    const query2 = query.related('self');
    expectTypeOf(query2.run()).toMatchTypeOf<
      ReadonlyArray<
        Row<SchemaWithEnums> & {
          self: ReadonlyArray<Row<SchemaWithEnums>>;
        }
      >
    >();
  });

  test('where against enum field', () => {
    const query = mockQuery as unknown as Query<SchemaWithEnums>;

    query.where('e', '=', 'open');
    query.where('e', '=', 'closed');
    // @ts-expect-error - invalid enum value
    query.where('e', '=', 'bogus');
  });

  test('one', () => {
    const q1 = mockQuery as unknown as Query<TestSchema>;

    expectTypeOf(q1.one().materialize().data).toMatchTypeOf<
      | {
          readonly s: string;
          readonly b: boolean;
          readonly n: number;
        }
      | undefined
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q1_1 = mockQuery as unknown as Query<TestSchema>;
    expectTypeOf(q1_1.one().one().materialize().data).toMatchTypeOf<
      | {
          readonly s: string;
          readonly b: boolean;
          readonly n: number;
        }
      | undefined
    >();

    const q2 = mockQuery as unknown as Query<TestSchemaWithRelationships>;
    expectTypeOf(q2.related('test').one().materialize().data).toMatchTypeOf<
      | {
          readonly s: string;
          readonly a: string;
          readonly b: boolean;
          readonly test: ReadonlyArray<{
            readonly s: string;
            readonly b: boolean;
            readonly n: number;
          }>;
        }
      | undefined
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q2_1 = mockQuery as unknown as Query<TestSchemaWithRelationships>;
    expectTypeOf(q2_1.one().related('test').materialize().data).toMatchTypeOf<
      | {
          readonly s: string;
          readonly a: string;
          readonly b: boolean;
          readonly test: ReadonlyArray<{
            readonly s: string;
            readonly b: boolean;
            readonly n: number;
          }>;
        }
      | undefined
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q2_2 = mockQuery as unknown as Query<TestSchemaWithRelationships>;
    expectTypeOf(
      q2_2.related('test', t => t.one()).materialize().data,
    ).toMatchTypeOf<
      ReadonlyArray<{
        readonly s: string;
        readonly a: string;
        readonly b: boolean;
        readonly test:
          | {
              readonly s: string;
              readonly b: boolean;
              readonly n: number;
            }
          | undefined;
      }>
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q2_3 = mockQuery as unknown as Query<TestSchemaWithRelationships>;
    expectTypeOf(
      q2_3.related('test', t => t.one().where('b', true)).materialize().data,
    ).toMatchTypeOf<
      ReadonlyArray<{
        readonly s: string;
        readonly a: string;
        readonly b: boolean;
        readonly test:
          | {
              readonly s: string;
              readonly b: boolean;
              readonly n: number;
            }
          | undefined;
      }>
    >();

    const q3 = mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;
    expectTypeOf(
      q3.related('test').related('self').one().materialize().data,
    ).toMatchTypeOf<
      | {
          readonly s: string;
          readonly a: string;
          readonly b: boolean;
          readonly test: ReadonlyArray<{
            readonly s: string;
            readonly b: boolean;
            readonly n: number;
          }>;
          readonly self: ReadonlyArray<{
            readonly s: string;
            readonly a: string;
            readonly b: boolean;
          }>;
        }
      | undefined
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q3_1 = mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;
    expectTypeOf(
      q3_1
        .related('test', t => t.one())
        .related('self', s => s.one())
        .one()
        .materialize().data,
    ).toMatchTypeOf<
      | {
          readonly s: string;
          readonly a: string;
          readonly b: boolean;
          readonly test:
            | {
                readonly s: string;
                readonly b: boolean;
                readonly n: number;
              }
            | undefined;
          readonly self:
            | {
                readonly s: string;
                readonly a: string;
                readonly b: boolean;
              }
            | undefined;
        }
      | undefined
    >();
  });

  test('related in subquery position', () => {
    const query =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;

    const query2 = query.related('self', query => query.related('test'));

    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      ReadonlyArray<
        Row<TestSchemaWithMoreRelationships> & {
          self: ReadonlyArray<
            Row<TestSchemaWithMoreRelationships> & {
              test: ReadonlyArray<Row<TestSchema>>;
            }
          >;
        }
      >
    >;
  });

  test('where', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    const query2 = query.where('s', '=', 'foo');
    expectTypeOf(query2.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();

    // @ts-expect-error - cannot use a field that does not exist
    query.where('doesNotExist', '=', 'foo');
    // @ts-expect-error - value and field types must match
    query.where('b', '=', 'false');

    expectTypeOf(query.where('b', '=', true).materialize().data).toMatchTypeOf<
      ReadonlyArray<Row<TestSchema>>
    >();
  });

  test('where-parameters', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    query.where('s', '=', staticParam('authData', 'aud'));

    const p = staticParam('authData', 'aud');
    query.where('b', '=', p);
  });

  test('where-optional-op', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    const query2 = query.where('s', 'foo');
    expectTypeOf(query2.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();

    // @ts-expect-error - cannot use a field that does not exist
    query.where('doesNotExist', 'foo');
    // @ts-expect-error - value and field types must match
    query.where('b', 'false');

    expectTypeOf(query.where('b', true).materialize().data).toMatchTypeOf<
      ReadonlyArray<Row<TestSchema>>
    >();
  });

  test('where-in', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    // @ts-expect-error - `IN` must take an array!
    query.where('s', 'IN', 'foo');

    query.where('s', 'IN', ['foo', 'bar']);
    // @ts-expect-error - cannot compare with null
    query.where('s', '=', null);
    // @ts-expect-error - cannot compare with undefined
    query.where('s', '=', undefined);

    // IS can compare to null
    query.where('s', 'IS', null);
  });

  test('start', () => {
    const query = mockQuery as unknown as Query<TestSchema>;
    const query2 = query.start({b: true, s: 'foo'});
    expectTypeOf(query2.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();
    const query3 = query.start({b: true, s: 'foo'}, {inclusive: true});
    expectTypeOf(query3.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();
  });
});

describe('schema structure', () => {
  test('dag', () => {
    const commentSchema: TableSchema = {
      tableName: 'comment',
      columns: {
        id: {type: 'string'},
        issueId: {type: 'string'},
        text: {type: 'string'},
      },
      primaryKey: ['id'],
      relationships: {},
    };

    const issueSchema: TableSchema = {
      tableName: 'issue',
      columns: {
        id: {type: 'string'},
        title: {type: 'string'},
      },
      relationships: {
        comments: {
          sourceField: ['id'],
          destField: ['issueId'],
          destSchema: commentSchema,
        },
      },
      primaryKey: ['id'],
    };

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
          sourceField: ['issueId'],
          destField: ['id'],
          destSchema: () => issueSchema,
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
          sourceField: ['id'],
          destField: ['issueId'],
          destSchema: commentSchema,
        },
        parent: {
          sourceField: ['parentId'],
          destField: ['id'],
          destSchema: () => issueSchema,
        },
      },
    } as const;

    takeSchema(issueSchema);
  });
});

test('supertype query', () => {
  const commentSchema = {
    tableName: 'comment',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
      creatorID: {type: 'string'},
      body: {type: 'string'},
    },
    relationships: {},
  } as const;
  const issueSchema = {
    tableName: 'issue',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
      creatorID: {type: 'string'},
      title: {type: 'string'},
    },
    relationships: {},
  } as const;
  const draftSchema = {
    tableName: 'draft',
    primaryKey: ['id'],
    columns: {
      id: {type: 'string'},
      creatorID: {type: 'string'},
      title: {type: 'string'},
    },
    relationships: {},
  } as const;

  const commentQuery = mockQuery as unknown as Query<typeof commentSchema>;
  const issueQuery = mockQuery as unknown as Query<typeof issueSchema>;
  const draftQuery = mockQuery as unknown as Query<typeof draftSchema>;

  function checkCreator(
    q: Query<
      Supertype<[typeof commentSchema, typeof issueSchema, typeof draftSchema]>
    >,
  ) {
    return q.where('creatorID', '=', 'foo');
  }

  function checkCreatorExpectError(
    q: Query<Supertype<[typeof commentSchema, typeof issueSchema]>>,
  ) {
    // @ts-expect-error - title is not shared by both types
    return q.where('title', 'title is not shared by both types');
  }

  checkCreator(commentQuery);
  checkCreator(issueQuery);
  checkCreator(draftQuery);
  checkCreatorExpectError(commentQuery);
  checkCreatorExpectError(issueQuery);
});

test('complex expressions', () => {
  const query = mockQuery as unknown as Query<TestSchema>;

  query.where(({cmp, or}) =>
    or(cmp('b', '!=', true), cmp('s', 'IN', ['foo', 'bar'])),
  );
  query.where(({cmp}) => cmp('b', '!=', true));

  // @ts-expect-error - boolean compared to string
  query.where(({cmp}) => cmp('b', '!=', 's'));
  // @ts-expect-error - field does not exist
  query.where(({cmp}) => cmp('x', '!=', true));
  // @ts-expect-error - boolean compared to string
  query.where(({cmp, or}) => or(cmp('b', '!=', 's')));
  // @ts-expect-error - field does not exist
  query.where(({cmp, or}) => or(cmp('x', '!=', true)));
  // @ts-expect-error - boolean compared to string
  query.where(({and, cmp}) => and(cmp('b', '!=', 's')));
  // @ts-expect-error - field does not exist
  query.where(({and, cmp}) => and(cmp('x', '!=', true)));
});

test('json type', () => {
  const query = mockQuery as unknown as Query<SchemaWithJson>;
  const datum = query.one().materialize().data;
  const {data} = query.materialize();

  expectTypeOf(datum).toMatchTypeOf<
    {a: string; j: ReadonlyJSONValue} | undefined
  >();

  expectTypeOf(data).toMatchTypeOf<
    ReadonlyArray<{a: string; j: ReadonlyJSONValue}>
  >();

  // @ts-expect-error - json fields cannot be used in `where` yet
  query.where('j', '=', {foo: 'bar'});
  // @ts-expect-error - json fields cannot be used in cmp yet
  query.where(({cmp}) => cmp('j', '=', {foo: 'bar'}));
});

function takeSchema(x: TableSchema) {
  return x;
}

test('custom materialize factory', () => {
  const query = mockQuery as unknown as AdvancedQuery<TestSchema>;
  const x = query.materialize();
  expectTypeOf(x.data).toMatchTypeOf<
    ReadonlyArray<{s: string; b: boolean; n: number}>
  >();

  // This is a pretend factory that unlike ArrayView, which has a `data` property that is an array,
  // has a `dataAsSet` property that is a Set.
  function factory<TSchema extends TableSchema, TReturn extends QueryType>(
    _query: Query<TSchema, TReturn>,
  ): {
    dataAsSet: Set<TReturn['row']>;
  } {
    return {dataAsSet: new Set()};
  }

  const y = query.materialize(factory);
  expectTypeOf(y.dataAsSet).toMatchTypeOf<
    Set<{s: string; b: boolean; n: number}>
  >();
});

test('Make sure that QueryInternal does not expose the ast', () => {
  const query = mockQuery as unknown as Query<TestSchema>;
  // @ts-expect-error - ast is not part of the public API
  query.ast;

  const internalQuery = mockQuery as unknown as AdvancedQuery<TestSchema>;
  // @ts-expect-error - ast is not part of the public API
  internalQuery.ast;
});

describe('Where expression factory and builder', () => {
  test('does not change the type', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    const query2 = query.where('n', '>', 42);
    expectTypeOf(query2).toMatchTypeOf(query);

    const query3 = query.where(eb => {
      eb.cmp('b', '=', true);
      eb.cmp('n', '>', 42);
      eb.cmp('s', '=', 'foo');

      // @ts-expect-error - field does not exist
      eb.cmp('no-b', '=', true);

      // @ts-expect-error - boolean compared to string
      eb.cmp('b', '=', 'foo');

      // skipping '='
      eb.cmp('b', true);
      eb.cmp('n', 42);
      return eb.cmp('s', 'foo');
    });

    // Where does not change the type of the query.
    expectTypeOf(query3).toMatchTypeOf(query);
  });

  test('and, or, not, cmp, eb', () => {
    const query = mockQuery as unknown as Query<TestSchema>;

    query.where(({and, cmp, or}) =>
      and(cmp('n', '>', 42), or(cmp('b', true), cmp('s', 'foo'))),
    );
    query.where(({not, cmp}) => not(cmp('n', '>', 42)));

    query.where(({eb}) => eb.cmp('n', '>', 42));

    query.where(({not, cmp}) =>
      not(
        // @ts-expect-error - field does not exist
        cmp('n2', '>', 42),
      ),
    );
  });

  test('exists', () => {
    const query =
      mockQuery as unknown as Query<TestSchemaWithMoreRelationships>;

    // can check relationships
    query.where(({exists}) => exists('self'));

    // can check relationships with a subquery
    query.where(({exists}) =>
      exists('testWithRelationships', q => q.where('b', true)),
    );

    // relationships that do not exist are type errors
    query.where(({exists}) =>
      // @ts-expect-error - relationship does not exist
      exists('doesNotExist'),
    );

    // nested existence is not an error
    query.where(({exists}) =>
      exists('self', q =>
        q.where(({exists}) =>
          exists('testWithRelationships', q =>
            q.where(({exists}) => exists('test')),
          ),
        ),
      ),
    );

    query.where(({exists}) =>
      exists('self', q =>
        q.where(({exists}) =>
          exists('testWithRelationships', q =>
            // @ts-expect-error - relationship does not exist
            q.where(({exists}) => exists('bogus')),
          ),
        ),
      ),
    );

    // not exists
    query.where(({not, exists}) => not(exists('self')));
  });

  describe('allow undefined terms', () => {
    test('and', () => {
      const query = mockQuery as unknown as Query<TestSchema>;

      query.where(({and}) => and());
      query.where(({and}) => and(undefined));
      query.where(({and}) => and(undefined, undefined));
      query.where(({and}) => and(undefined, undefined, undefined));
      query.where(({and, cmp}) => and(cmp('n', 1), undefined, cmp('n', 2)));
    });

    test('or', () => {
      const query = mockQuery as unknown as Query<TestSchema>;

      query.where(({or}) => or());
      query.where(({or}) => or(undefined));
      query.where(({or}) => or(undefined, undefined));
      query.where(({or}) => or(undefined, undefined, undefined));
      query.where(({or, cmp}) => or(cmp('n', 1), undefined, cmp('n', 2)));
    });
  });

  test('expression builder append from array', () => {
    const q = mockQuery as unknown as Query<TestSchema>;
    const numbers = [1, 23, 456];
    const f: ExpressionFactory<TestSchema> = b => {
      const exprs = [];
      for (const n of numbers) {
        exprs.push(b.cmp('n', '>', n));
      }
      return b.or(...exprs);
    };
    const q2 = q.where(f);
    expectTypeOf(q2).toMatchTypeOf(q);
  });

  test('expression builder append from object', () => {
    type Entries<T> = {
      [K in keyof T]: [K, T[K]];
    }[keyof T][];

    const q = mockQuery as unknown as Query<TestSchema>;
    const o = {n: 1, s: 'hi', b: true};
    const entries = Object.entries(o) as Entries<typeof o>;
    const f: ExpressionFactory<TestSchema> = b => {
      const exprs = [];
      for (const [n, v] of entries) {
        exprs.push(b.cmp(n, v));
      }
      return b.or(...exprs);
    };
    const q2 = q.where(f);
    expectTypeOf(q2).toMatchTypeOf(q);
  });
});
