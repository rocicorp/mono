import {expect, expectTypeOf, test} from 'vitest';
import * as v from '../../../shared/src/valita.ts';
import {
  createBuilder,
  syncedQuery,
  syncedQueryWithContext,
  withValidation,
  type QueryFnReturn,
} from './named.ts';
import {asQueryInternals} from './query-internals.ts';
import type {QueryReturn} from './query.ts';
import {schema} from './test/test-schemas.ts';
const builder = createBuilder(schema);

test('syncedQuery', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQuery('myQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(false);

  expectTypeOf<QueryFnReturn<typeof def>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const query = def('123');
  expectTypeOf<QueryReturn<typeof query>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const q = asQueryInternals(query);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(q.ast).toEqual({
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
    orderBy: [['id', 'asc']],
  });

  const wv = withValidation(def);
  expect(wv.queryName).toEqual('myQuery');
  expect(wv.parse).toBeDefined();
  expect(wv.takesContext).toEqual(true);
  // @ts-expect-error 123 is not a string
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  expectTypeOf<QueryFnReturn<typeof wv>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const vquery = wv('ignored', '123');
  expectTypeOf<QueryReturn<typeof vquery>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const vq = asQueryInternals(vquery);
  expect(vq.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(vq.ast).toEqual({
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
    orderBy: [['id', 'asc']],
  });
});

test('syncedQueryWithContext', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQueryWithContext(
    'myQuery',
    idArgs,
    (context: string, id: string) =>
      builder.issue.where('id', id).where('ownerId', context),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(true);

  expectTypeOf<QueryFnReturn<typeof def>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const query2 = def('user1', '123');
  expectTypeOf<QueryReturn<typeof query2>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const q = asQueryInternals(query2);
  expect(q.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(q.ast).toEqual({
    table: 'issue',
    where: {
      conditions: [
        {
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
        {
          left: {
            name: 'ownerId',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'user1',
          },
          type: 'simple',
        },
      ],
      type: 'and',
    },
    orderBy: [['id', 'asc']],
  });

  const wv = withValidation(def);
  expect(wv.queryName).toEqual('myQuery');
  expect(wv.parse).toBeDefined();
  expect(wv.takesContext).toEqual(true);
  // @ts-expect-error 123 is not a string
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  expectTypeOf<QueryFnReturn<typeof wv>>().toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly description: string;
      readonly closed: boolean;
      readonly ownerId: string | null;
      readonly createdAt: number;
    }[]
  >();

  const vquery2 = wv('user1', '123');
  expectTypeOf<QueryReturn<typeof vquery2>>().toEqualTypeOf<{
    readonly id: string;
    readonly title: string;
    readonly description: string;
    readonly closed: boolean;
    readonly ownerId: string | null;
    readonly createdAt: number;
  }>();

  const vq = asQueryInternals(vquery2);
  expect(vq.customQueryID).toEqual({
    name: 'myQuery',
    args: ['123'],
  });

  expect(vq.ast).toEqual({
    table: 'issue',
    where: {
      conditions: [
        {
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
        {
          left: {
            name: 'ownerId',
            type: 'column',
          },
          op: '=',
          right: {
            type: 'literal',
            value: 'user1',
          },
          type: 'simple',
        },
      ],
      type: 'and',
    },
    orderBy: [['id', 'asc']],
  });
});

// TODO: test unions

test('makeSchemaQuery', () => {
  const builders = createBuilder(schema);
  const q1 = asQueryInternals(
    asQueryInternals(builders.issue.where('id', '123')).nameAndArgs('myName', [
      '123',
    ]),
  );
  expect(q1.ast).toMatchInlineSnapshot(`
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
