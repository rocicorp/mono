/* eslint-disable @typescript-eslint/no-floating-promises */
/* eslint-disable @typescript-eslint/ban-types */
import {describe, expectTypeOf, test} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {toStaticParam} from '../../../zero-protocol/src/ast.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  type Opaque,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.ts';
import type {ExpressionFactory} from './expression.ts';
import {staticParam} from './query-impl.ts';
import {type Query, type Row} from './query.ts';

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

type Timestamp = Opaque<number>;
type IdOf<T> = Opaque<string, T>;

function timestamp(n: number): Timestamp {
  return n as Timestamp;
}

const testSchema = table('test')
  .columns({
    s: string(),
    b: boolean(),
    n: number(),
  })
  .primaryKey('s');

const testSchemaWithNulls = table('testWithNulls')
  .columns({
    n: number(),
    s: string().optional(),
  })
  .primaryKey('n');

const schemaWithEnums = table('testWithEnums')
  .columns({
    s: string(),
    e: enumeration<'open' | 'closed'>(),
  })
  .primaryKey('s');

const schemaWithEnumsRelationships = relationships(
  schemaWithEnums,
  ({many}) => ({
    self: many({
      sourceField: ['s'],
      destField: ['s'],
      destSchema: schemaWithEnums,
    }),
  }),
);

const schemaWithAdvancedTypes = table('schemaWithAdvancedTypes')
  .columns({
    s: string(),
    n: number<Timestamp>(),
    b: boolean(),
    j: json<{foo: string; bar: boolean}>(),
    e: enumeration<'open' | 'closed'>(),
    otherId: string<IdOf<(typeof schemaWithEnums)['schema']>>(),
  })
  .primaryKey('s');

const withAdvancedTypesRelationships = relationships(
  schemaWithAdvancedTypes,
  connect => ({
    self: connect.many({
      sourceField: ['s'],
      destField: ['s'],
      destSchema: schemaWithAdvancedTypes,
    }),
  }),
);

const schemaWithJson = table('testWithJson')
  .columns({
    a: string(),
    j: json(),
    maybeJ: json().optional(),
  })
  .primaryKey('a');

const schemaWithArray = table('testWithArray')
  .columns({
    id: string(),

    arrayOfNumber: json<number[]>(),
    arrayOfString: json<string[]>(),
    arrayOfBoolean: json<boolean[]>(),

    optionalArrayOfNumber: json<number[]>().optional(),
    optionalArrayOfString: json<string[]>().optional(),
    optionalArrayOfBoolean: json<boolean[]>().optional(),
  })
  .primaryKey('id');

const testWithRelationships = table('testWithRelationships')
  .columns({
    s: string(),
    a: string(),
    b: boolean(),
  })
  .primaryKey('s');

const testWithRelationshipsRelationships = relationships(
  testWithRelationships,
  connect => ({
    test: connect.many({
      sourceField: ['s'],
      destField: ['s'],
      destSchema: testSchema,
    }),
  }),
);

const testWithMoreRelationships = table('testWithMoreRelationships')
  .columns({
    s: string(),
    a: string(),
    b: boolean(),
  })
  .primaryKey('s');

const testWithMoreRelationshipsRelationships = relationships(
  testWithMoreRelationships,
  connect => ({
    testWithRelationships: connect.many({
      sourceField: ['a'],
      destField: ['a'],
      destSchema: testWithRelationships,
    }),
    test: connect.many({
      sourceField: ['s'],
      destField: ['s'],
      destSchema: testSchema,
    }),
    self: connect.many({
      sourceField: ['s'],
      destField: ['s'],
      destSchema: testWithMoreRelationships,
    }),
  }),
);

const testWithOneRelationships = table('testWithOneRelationships')
  .columns({
    s: string(),
    a: string(),
    b: boolean(),
  })
  .primaryKey('s');

const testWithOneRelationshipsRelationships = relationships(
  testWithOneRelationships,
  connect => ({
    testWithRelationships: connect.one({
      sourceField: ['a'],
      destField: ['a'],
      destSchema: testWithRelationships,
    }),
  }),
);

const schema = createSchema({
  tables: [
    testSchema,
    testSchemaWithNulls,
    schemaWithEnums,
    schemaWithJson,
    schemaWithArray,
    schemaWithAdvancedTypes,
    testWithRelationships,
    testWithMoreRelationships,
    testWithOneRelationships,
  ],
  relationships: [
    testWithRelationshipsRelationships,
    testWithMoreRelationshipsRelationships,
    withAdvancedTypesRelationships,
    schemaWithEnumsRelationships,
    testWithOneRelationshipsRelationships,
  ],
});

type Schema = typeof schema;
type SchemaWithEnums = Schema['tables']['testWithEnums'];
type TestSchemaWithMoreRelationships =
  Schema['tables']['testWithMoreRelationships'];
type TestSchema = Schema['tables']['test'];

describe('types', () => {
  test('simple select', () => {
    const query = mockQuery as unknown as Query<Schema, 'test'>;

    // no select? All fields are returned.
    expectTypeOf(query.materialize().data).toMatchTypeOf<
      ReadonlyArray<Row<typeof schema.tables.test>>
    >();
  });

  test('simple select with enums', () => {
    const query = mockQuery as unknown as Query<Schema, 'testWithEnums'>;
    expectTypeOf(query.run()).toExtend<
      Promise<
        ReadonlyArray<{
          s: string;
          e: 'open' | 'closed';
        }>
      >
    >();

    const q2 = mockQuery as unknown as Query<Schema, 'schemaWithAdvancedTypes'>;
    q2.where('e', '=', 'open');
    // @ts-expect-error - invalid enum value
    q2.where('e', 'bogus');
    expectTypeOf(q2.run()).toMatchTypeOf<
      Promise<
        ReadonlyArray<{
          s: string;
          n: Timestamp;
          b: boolean;
          j: {foo: string; bar: boolean};
          e: 'open' | 'closed';
          otherId: IdOf<Schema['tables']['testWithEnums']>;
        }>
      >
    >();

    // @ts-expect-error - 'foo' is not an id of `SchemaWithEnums`
    q2.where('otherId', '=', 'foo');

    // @ts-expect-error - 42 is not a timestamp
    q2.where('n', '>', 42);

    q2.where('n', '>', timestamp(42));
  });

  test('related with advanced types', () => {
    const query = mockQuery as unknown as Query<
      Schema,
      'schemaWithAdvancedTypes'
    >;

    const query2 = query.related('self');
    expectTypeOf(query2.run()).toMatchTypeOf<
      Promise<
        ReadonlyArray<{
          s: string;
          n: Timestamp;
          b: boolean;
          j: {foo: string; bar: boolean};
          e: 'open' | 'closed';
          otherId: IdOf<Schema['tables']['testWithEnums']>;
          self: ReadonlyArray<{
            s: string;
            n: Timestamp;
            b: boolean;
            j: {foo: string; bar: boolean};
            e: 'open' | 'closed';
            otherId: IdOf<Schema['tables']['testWithEnums']>;
          }>;
        }>
      >
    >();

    // @ts-expect-error - missing enum value
    query2.related('self', sq => sq.where('e', 'bogus'));
    query2.related('self', sq => sq.where('e', 'open'));
    query2.related('self', sq =>
      sq.related('self', sq => sq.where('e', 'open')),
    );
  });

  test('related', () => {
    const query = mockQuery as unknown as Query<
      Schema,
      'testWithRelationships'
    >;

    // @ts-expect-error - cannot traverse a relationship that does not exist
    query.related('doesNotExist', q => q);

    const query2 = query.related('test');

    expectTypeOf(query2.materialize().data).toMatchTypeOf<
      ReadonlyArray<
        Row<Schema['tables']['testWithMoreRelationships']> & {
          test: ReadonlyArray<Row<Schema['tables']['test']>>;
        }
      >
    >();

    // Many calls to related builds up the related object.
    const query3 = mockQuery as unknown as Query<
      Schema,
      'testWithMoreRelationships'
    >;
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
    const query = mockQuery as unknown as Query<Schema, 'testWithEnums'>;

    const query2 = query.related('self');
    expectTypeOf(query2.run()).toMatchTypeOf<
      Promise<
        ReadonlyArray<
          Row<SchemaWithEnums> & {
            self: ReadonlyArray<Row<SchemaWithEnums>>;
          }
        >
      >
    >();
  });

  test('where against enum field', () => {
    const query = mockQuery as unknown as Query<Schema, 'testWithEnums'>;

    query.where('e', '=', 'open');
    query.where('e', '=', 'closed');
    // @ts-expect-error - invalid enum value
    query.where('e', '=', 'bogus');
  });

  test('one', () => {
    const q1 = mockQuery as unknown as Query<Schema, 'test'>;
    expectTypeOf(q1.one().run()).toMatchTypeOf<
      Promise<
        | {
            readonly s: string;
            readonly b: boolean;
            readonly n: number;
          }
        | undefined
      >
    >();

    // eslint-disable-next-line @typescript-eslint/naming-convention
    const q1_1 = mockQuery as unknown as Query<Schema, 'test'>;
    expectTypeOf(q1_1.one().one().materialize().data).toMatchTypeOf<
      | {
          readonly s: string;
          readonly b: boolean;
          readonly n: number;
        }
      | undefined
    >();

    const q2 = mockQuery as unknown as Query<Schema, 'testWithRelationships'>;
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
    const q2_1 = mockQuery as unknown as Query<Schema, 'testWithRelationships'>;
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
    const q2_2 = mockQuery as unknown as Query<Schema, 'testWithRelationships'>;
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
    const q2_3 = mockQuery as unknown as Query<Schema, 'testWithRelationships'>;
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

    const q3 = mockQuery as unknown as Query<
      Schema,
      'testWithMoreRelationships'
    >;
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
    const q3_1 = mockQuery as unknown as Query<
      Schema,
      'testWithMoreRelationships'
    >;
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
    const query = mockQuery as unknown as Query<
      Schema,
      'testWithMoreRelationships'
    >;

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
    const query = mockQuery as unknown as Query<Schema, 'test'>;

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
    const query = mockQuery as unknown as Query<Schema, 'test'>;

    query.where('s', '=', {
      [toStaticParam]: () => staticParam('authData', 'aud'),
    });

    const p = {
      [toStaticParam]: () => staticParam('authData', 'aud'),
    };
    query.where('b', '=', p);
  });

  test('where-optional-op', () => {
    const query = mockQuery as unknown as Query<Schema, 'test'>;

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
    const query = mockQuery as unknown as Query<Schema, 'test'>;

    // @ts-expect-error - `IN` must take an array!
    query.where('s', 'IN', 'foo');

    query.where('s', 'IN', ['foo', 'bar']);
  });

  test('where-null', () => {
    const q1 = mockQuery as unknown as Query<Schema, 'test'>;

    // @ts-expect-error - cannot compare with null
    q1.where('s', '=', null);
    // @ts-expect-error - cannot compare with null
    q1.where('s', null);

    // @ts-expect-error - cannot compare with undefined
    q1.where('s', '=', undefined);
    // @ts-expect-error - cannot compare with undefined
    q1.where('s', undefined);

    // @ts-expect-error - IN cannot compare with null.
    q1.where('s', 'IN', [null]);
    // @ts-expect-error - IN cannot compare with undefined.
    q1.where('s', 'IN', [undefined]);

    // IS and IS NOT can always compare with null
    q1.where('s', 'IS', null);
    q1.where('s', 'IS NOT', null);

    // @ts-expect-error - IS cannot compare with undefined
    q1.where('s', 'IS', undefined);
    // @ts-expect-error - same with IS NOT
    q1.where('s', 'IS NOT', undefined);

    const q2 = mockQuery as unknown as Query<Schema, 'testWithNulls'>;

    // @ts-expect-error - = cannot be used with null, must use IS
    q2.where('s', null);
    // @ts-expect-error - = cannot be used with null, must use IS
    q2.where('s', '=', null);
    // @ts-expect-error - = cannot be used with undefined, must use IS
    q2.where('s', undefined);
    // @ts-expect-error - = cannot be used with undefined, must use IS
    q2.where('s', '=', undefined);

    q2.where('s', 'IS', null);
    q2.where('s', 'IS NOT', null);

    // @ts-expect-error - IS cannot compare with undefined, even when field is
    // optional.
    q2.where('s', 'IS', undefined);
    // @ts-expect-error - Same with IS NOT
    q2.where('s', 'IS NOT', undefined);
  });

  test('start', () => {
    const query = mockQuery as unknown as Query<Schema, 'test'>;
    const query2 = query.start({b: true, s: 'foo'});
    expectTypeOf(query2.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();
    const query3 = query.start({b: true, s: 'foo'}, {inclusive: true});
    expectTypeOf(query3.materialize().data).toMatchTypeOf<ReadonlyArray<{}>>();
  });
});

describe('schema structure', () => {
  test('dag', () => {
    const comment = table('comment')
      .columns({
        id: string(),
        issueId: string(),
        text: string(),
      })
      .primaryKey('id');

    const issue = table('issue')
      .columns({
        id: string(),
        title: string(),
      })
      .primaryKey('id');

    const issueRelationships = relationships(issue, connect => ({
      comments: connect.many({
        sourceField: ['id'],
        destField: ['issueId'],
        destSchema: comment,
      }),
    }));

    const schema = createSchema({
      tables: [comment, issue],
      relationships: [issueRelationships],
    });

    takeSchema(schema.tables.issue);
  });

  test('cycle', () => {
    const comment = table('comment')
      .columns({
        id: string(),
        issueId: string(),
        text: string(),
      })
      .primaryKey('id');

    const issue = table('issue')
      .columns({
        id: string(),
        title: string(),
        parentId: string(),
      })
      .primaryKey('id');

    const commentRelationships = relationships(comment, connect => ({
      issue: connect.many({
        sourceField: ['issueId'],
        destField: ['id'],
        destSchema: issue,
      }),
    }));

    const issueRelationships = relationships(issue, connect => ({
      comments: connect.many({
        sourceField: ['id'],
        destField: ['issueId'],
        destSchema: comment,
      }),
      parent: connect.many({
        sourceField: ['parentId'],
        destField: ['id'],
        destSchema: issue,
      }),
    }));

    const schema = createSchema({
      tables: [issue, comment],
      relationships: [issueRelationships, commentRelationships],
    });
    takeSchema(schema.tables.issue);
  });
});

test('complex expressions', () => {
  const query = mockQuery as unknown as Query<Schema, 'test'>;

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
  const query = mockQuery as unknown as Query<Schema, 'testWithJson'>;
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

test('array type', () => {
  const query = mockQuery as unknown as Query<Schema, 'testWithArray'>;
  const datum = query.one().materialize().data;
  const {data} = query.materialize();

  expectTypeOf(datum).toMatchTypeOf<
    | {
        readonly id: string;

        readonly arrayOfNumber: number[];
        readonly arrayOfString: string[];
        readonly arrayOfBoolean: boolean[];

        readonly optionalArrayOfNumber: number[] | null;
        readonly optionalArrayOfString: string[] | null;
        readonly optionalArrayOfBoolean: boolean[] | null;
      }
    | undefined
  >();

  expectTypeOf(data).toMatchTypeOf<
    {
      readonly id: string;

      readonly arrayOfNumber: number[];
      readonly arrayOfString: string[];
      readonly arrayOfBoolean: boolean[];

      readonly optionalArrayOfNumber: number[] | null;
      readonly optionalArrayOfString: string[] | null;
      readonly optionalArrayOfBoolean: boolean[] | null;
    }[]
  >();

  //  @ts-expect-error - Cannot compare json/arrays. Should we allow this... Maybe in a follow up PR?
  query.where('arrayOfNumber', '=', [1, 2]);

  //  @ts-expect-error - Cannot compare json/arrays. Should we allow this... Maybe in a follow up PR?
  query.where(({cmp}) => cmp('arrayOfString', '=', ['a', 'b']));
});

function takeSchema(x: TableSchema) {
  return x;
}

test('custom materialize factory', () => {
  const query = mockQuery as unknown as Query<Schema, 'test'>;
  const x = query.materialize();
  expectTypeOf(x.data).toMatchTypeOf<
    ReadonlyArray<{s: string; b: boolean; n: number}>
  >();

  // This is a pretend factory that unlike ArrayView, which has a `data` property that is an array,
  // has a `dataAsSet` property that is a Set.
  function factory<TSchema extends ZeroSchema, TTable extends string, TReturn>(
    _query: Query<TSchema, TTable, TReturn>,
  ): {
    dataAsSet: Set<TReturn>;
  } {
    return {dataAsSet: new Set()};
  }

  const y = query.materialize(factory);
  expectTypeOf(y.dataAsSet).toMatchTypeOf<
    Set<{s: string; b: boolean; n: number}>
  >();
});

test('Make sure that QueryInternal does not expose the ast', () => {
  const query = mockQuery as unknown as Query<Schema, 'test'>;
  // @ts-expect-error - ast is not part of the public API
  query.ast;

  const internalQuery = mockQuery as unknown as Query<Schema, 'test'>;
  // @ts-expect-error - ast is not part of the public API
  internalQuery.ast;
});

describe('Where expression factory and builder', () => {
  test('does not change the type', () => {
    const query = mockQuery as unknown as Query<Schema, 'test'>;

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
    const query = mockQuery as unknown as Query<Schema, 'test'>;

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
    const query = mockQuery as unknown as Query<
      Schema,
      'testWithMoreRelationships'
    >;

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
      const query = mockQuery as unknown as Query<Schema, 'test'>;

      query.where(({and}) => and());
      query.where(({and}) => and(undefined));
      query.where(({and}) => and(undefined, undefined));
      query.where(({and}) => and(undefined, undefined, undefined));
      query.where(({and, cmp}) => and(cmp('n', 1), undefined, cmp('n', 2)));
    });

    test('or', () => {
      const query = mockQuery as unknown as Query<Schema, 'test'>;

      query.where(({or}) => or());
      query.where(({or}) => or(undefined));
      query.where(({or}) => or(undefined, undefined));
      query.where(({or}) => or(undefined, undefined, undefined));
      query.where(({or, cmp}) => or(cmp('n', 1), undefined, cmp('n', 2)));
    });
  });

  test('expression builder append from array', () => {
    const q = mockQuery as unknown as Query<Schema, 'test'>;
    const numbers = [1, 23, 456];
    const f: ExpressionFactory<Schema, 'test'> = b => {
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

    const q = mockQuery as unknown as Query<Schema, 'test'>;
    const o = {n: 1, s: 'hi', b: true};
    const entries = Object.entries(o) as Entries<typeof o>;
    const f: ExpressionFactory<Schema, 'test'> = b => {
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

test('one', async () => {
  const q = mockQuery as unknown as Query<Schema, 'test'>;
  const q1 = q;
  const q2 = q.one();
  expectTypeOf(q2).not.toEqualTypeOf(q1);

  const r1 = await q1.run();
  const r2 = await q2.run();

  expectTypeOf(r1).not.toEqualTypeOf(r2);

  expectTypeOf(r1).toEqualTypeOf<
    {
      readonly s: string;
      readonly b: boolean;
      readonly n: number;
    }[]
  >();

  expectTypeOf(r2).toEqualTypeOf<
    | {
        readonly s: string;
        readonly b: boolean;
        readonly n: number;
      }
    | undefined
  >();
});

test('one in related subquery', async () => {
  const q = mockQuery as unknown as Query<Schema, 'testWithOneRelationships'>;
  const q1 = q.related('testWithRelationships');
  const r1 = await q1.run();

  expectTypeOf(r1).toEqualTypeOf<
    {
      readonly s: string;
      readonly a: string;
      readonly b: boolean;
      readonly testWithRelationships:
        | {
            readonly s: string;
            readonly b: boolean;
            readonly a: string;
          }
        | undefined;
    }[]
  >();
});
