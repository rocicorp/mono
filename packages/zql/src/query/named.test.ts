/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test} from 'vitest';
import {
  createBuilder,
  syncedQuery,
  syncedQueryWithContext,
  withValidation,
} from './named.ts';
import {schema} from './test/test-schemas.ts';
const builder = createBuilder(schema);
import * as v from '../../../shared/src/valita.ts';
import {ast} from './query-impl.ts';

test('syncedQuery', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQuery('myQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(false);

  const q = def('123');
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
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  const vq = wv('ignored', '123');
  expectTypeOf<ReturnType<typeof vq.run>>().toEqualTypeOf<
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

  const q = def('user1', '123');
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
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  const vq = wv('user1', '123');
  expectTypeOf<ReturnType<typeof vq.run>>().toEqualTypeOf<
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
