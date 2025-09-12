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
import type {Query} from './query.ts';

test('syncedQuery', () => {
  const idArgs = v.tuple([v.string()]);
  const def = syncedQuery('myQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  expect(def.queryName).toEqual('myQuery');
  expect(def.parse).toBeDefined();
  expect(def.takesContext).toEqual(false);

  const q = def('123');
  expectTypeOf(q.run()).toEqualTypeOf<
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
  expectTypeOf(vq.run()).toEqualTypeOf<
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
  expectTypeOf(q.run()).toEqualTypeOf<
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
  expectTypeOf(vq.run()).toEqualTypeOf<
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

test('syncedQuery with async function', () => {
  const idArgs = v.tuple([v.string()]);
  const asyncDef = syncedQuery('myAsyncQuery', idArgs, async (id: string) => {
    // Simulate async operation
    await Promise.resolve();
    return builder.issue.where('id', id);
  });

  expect(asyncDef.queryName).toEqual('myAsyncQuery');
  expect(asyncDef.parse).toBeDefined();
  expect(asyncDef.takesContext).toEqual(false);

  const qPromise = asyncDef('456');

  // Type check: qPromise should be a Promise
  expectTypeOf(qPromise).toMatchTypeOf<Promise<any>>();

  // Verify it's a Promise
  expect(qPromise).toBeInstanceOf(Promise);

  // Test the sync version to verify the expected structure
  const syncDef = syncedQuery('myAsyncQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  const q = syncDef('456');

  expect(q.customQueryID).toEqual({
    name: 'myAsyncQuery',
    args: ['456'],
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
        value: '456',
      },
      type: 'simple',
    },
    orderBy: [['id', 'asc']],
  });
});

test('syncedQuery with sync function type inference', () => {
  const idArgs = v.tuple([v.string()]);
  const syncDef = syncedQuery('mySyncQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );

  const q = syncDef('789');

  // Type check: q should NOT be a Promise
  expectTypeOf(q).not.toEqualTypeOf<Promise<any>>();

  expect(q.customQueryID).toEqual({
    name: 'mySyncQuery',
    args: ['789'],
  });
});

test('syncedQueryWithContext with async function', () => {
  const idArgs = v.tuple([v.string()]);
  const asyncDef = syncedQueryWithContext(
    'myAsyncContextQuery',
    idArgs,
    async (context: string, id: string) => {
      // Simulate async operation
      await Promise.resolve();
      return builder.issue.where('id', id).where('ownerId', context);
    },
  );

  expect(asyncDef.queryName).toEqual('myAsyncContextQuery');
  expect(asyncDef.parse).toBeDefined();
  expect(asyncDef.takesContext).toEqual(true);

  const qPromise = asyncDef('user2', '999');

  // Type check: qPromise should be a Promise
  expectTypeOf(qPromise).toMatchTypeOf<Promise<any>>();

  // Verify it's a Promise
  expect(qPromise).toBeInstanceOf(Promise);

  // Test the sync version to verify the expected structure
  const syncDef = syncedQueryWithContext(
    'myAsyncContextQuery',
    idArgs,
    (context: string, id: string) =>
      builder.issue.where('id', id).where('ownerId', context),
  );
  const q = syncDef('user2', '999');

  expect(q.customQueryID).toEqual({
    name: 'myAsyncContextQuery',
    args: ['999'],
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
            value: '999',
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
            value: 'user2',
          },
          type: 'simple',
        },
      ],
      type: 'and',
    },
    orderBy: [['id', 'asc']],
  });
});

test('withValidation with async syncedQuery', () => {
  const idArgs = v.tuple([v.string()]);
  const asyncDef = syncedQuery(
    'myAsyncValidatedQuery',
    idArgs,
    async (id: string) => {
      await Promise.resolve();
      return builder.issue.where('id', id);
    },
  );

  const wv = withValidation(asyncDef);
  expect(wv.queryName).toEqual('myAsyncValidatedQuery');
  expect(wv.parse).toBeDefined();
  expect(wv.takesContext).toEqual(true);

  // Should validate arguments
  expect(() => wv('ignored', 123)).toThrow(
    'invalid_type at .0 (expected string)',
  );

  const vqPromise = wv('ignored', '111');

  // Type check: should be a Promise of Query after withValidation
  expectTypeOf(vqPromise).toMatchTypeOf<Promise<Query<any, any, any>>>();

  // Verify it's a Promise
  expect(vqPromise).toBeInstanceOf(Promise);

  // Test that sync withValidation returns query directly
  const syncDef = syncedQuery('mySyncValidatedQuery', idArgs, (id: string) =>
    builder.issue.where('id', id),
  );
  const wvSync = withValidation(syncDef);
  const vq = wvSync('ignored', '111');

  expect(vq.customQueryID).toEqual({
    name: 'mySyncValidatedQuery',
    args: ['111'],
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
