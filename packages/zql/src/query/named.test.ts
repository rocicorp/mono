/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test} from 'vitest';
import {
  createBuilder,
  syncedQuery,
  syncedQueryWithContext,
  withContext,
  withValidation,
} from './named.ts';
import {schema} from './test/test-schemas.ts';
import {assert} from '../../../shared/src/asserts.ts';
import {ast} from './query-impl.ts';
const builder = createBuilder(schema);

test('defining a synced query', () => {
  const def = syncedQuery('myQuery', (id: string) =>
    builder.issue.where('id', id),
  );
  let q = def('123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
      }[]
    >
  >();

  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  const defWithFakeContext = withContext(def);
  q = defWithFakeContext('1', '321');
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['321'],
  });

  // no validator was defined
  expect(() => withValidation(def)).toThrowErrorMatchingInlineSnapshot(
    `[Error: ret does not have a validator defined]`,
  );
});

test('defining a synced query with context', () => {
  const def = syncedQueryWithContext('myQuery', (_c: unknown, id: string) =>
    builder.issue.where('id', id),
  );
  const q = def(1, '123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
      }[]
    >
  >();

  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  // no validator was defined
  expect(() => withValidation(def)).toThrowErrorMatchingInlineSnapshot(
    `[Error: ret does not have a validator defined]`,
  );
});

test('defining a synced query with validation', () => {
  const def = syncedQuery(
    'myQuery',
    (id: unknown) => {
      assert(typeof id === 'string', 'id must be a string');
      return [id] as const;
    },
    id => builder.issue.where('id', id),
  );

  let q = def('123');
  expectTypeOf<ReturnType<typeof q.run>>().toEqualTypeOf<
    Promise<
      {
        readonly id: string;
        readonly title: string;
        readonly description: string;
        readonly closed: boolean;
        readonly ownerId: string | null;
        readonly createdAt: number;
      }[]
    >
  >();

  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  const {validator} = def;
  expectTypeOf<ReturnType<typeof validator>>().toEqualTypeOf<
    readonly [string] | [string]
  >();

  const validated = withValidation(def);
  q = validated('321');
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['321'],
  });

  expect(() => validated(1)).toThrowErrorMatchingInlineSnapshot(
    `[Error: id must be a string]`,
  );
});

test('defining a synced query with validation and context', () => {
  const def = syncedQueryWithContext(
    'myQuery',
    (id: unknown, createdAt: unknown) => {
      assert(typeof id === 'string', 'id must be a string');
      assert(typeof createdAt === 'number', 'createdAt must be a number');
      return [id, createdAt] as const;
    },
    (ctx: object, ownerId, createdAt) => {
      expect(ctx).toEqual({});
      return builder.issue
        .where('ownerId', ownerId)
        .where('createdAt', '>', createdAt);
    },
  );

  let q = def({}, '123', 123);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123', 123],
  });

  const validated = withValidation(def);
  q = validated({}, '321', 321);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['321', 321],
  });

  // calling context on a thing with context is a no-op
  const defWithCtx = withContext(def);
  q = defWithCtx({}, '123', 123);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123', 123],
  });

  expectTypeOf<Parameters<typeof defWithCtx>>().toEqualTypeOf<
    [object, string, number]
  >();
});

// test no args
test('no args provided to a syncedQuery', () => {
  const expectedAst = {
    table: 'issue',
    where: {
      left: {
        name: 'id',
        type: 'column',
      },
      op: '=',
      right: {
        type: 'literal',
        value: '123',
      },
      type: 'simple',
    },
  };
  const myQuery = syncedQueryWithContext('myQuery', (_ctx: object) =>
    builder.issue.where('id', '123'),
  );
  let q = myQuery({});
  expect(ast(q)).toEqual(expectedAst);

  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: [],
  });

  const myQuery2 = syncedQuery('myQuery', () =>
    builder.issue.where('id', '123'),
  );

  q = myQuery2();
  expect(ast(q)).toEqual(expectedAst);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: [],
  });

  const myQuery3 = syncedQuery(
    'myQuery',
    () => [],
    () => builder.issue.where('id', '123'),
  );

  q = myQuery3();
  expect(ast(q)).toEqual(expectedAst);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: [],
  });

  const myQuery4 = syncedQueryWithContext(
    'myQuery',
    () => [],
    (_ctx: object) => builder.issue.where('id', '123'),
  );
  q = myQuery4({});
  expect(ast(q)).toEqual(expectedAst);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: [],
  });
});

test('makeSchemaQuery', () => {
  const builders = createBuilder(schema);
  const q1 = builders.issue.where('id', '123').nameAndArgs('myName', ['123']);
  expect(ast(q1)).toMatchInlineSnapshot(`
    {
      "table": "issue",
      "where": {
        "left": {
          "name": "id",
          "type": "column",
        },
        "op": "=",
        "right": {
          "type": "literal",
          "value": "123",
        },
        "type": "simple",
      },
    }
  `);
});
