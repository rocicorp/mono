/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect, expectTypeOf, test, describe} from 'vitest';
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
import * as valibot from 'valibot';
import {type} from 'arktype';
import * as z from 'zod';

test('syncedQuery', async () => {
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
  await expect(() => wv('ignored', 123)).rejects.toThrow(
    'invalid_type at .0 (expected string)',
  );

  const vq = (await wv('ignored', '123')).query;
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

test('syncedQueryWithContext', async () => {
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
  await expect(() => wv('ignored', 123)).rejects.toThrow(
    'invalid_type at .0 (expected string)',
  );

  const vq = (await wv('user1', '123')).query;
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

describe('Schema compatibility (StandardSchemaV1 and HasParseFn)', () => {
  const testCases = [
    {
      name: 'Valibot (StandardSchemaV1) with syncedQuery',
      parser: valibot.tuple([valibot.string(), valibot.number()]),
      validArgs: ['test', 123],
      invalidArgs: [123, 'test'],
      expectedError: 'Invalid type: Expected string but received 123',
      queryFn: (arg1: string, arg2: number) =>
        builder.issue.where('id', arg1).where('createdAt', '>', arg2),
    },
    {
      name: 'Arktype (StandardSchemaV1) with syncedQuery',
      parser: type(['string', 'number']),
      validArgs: ['test', 456],
      invalidArgs: ['test', 'not-a-number'],
      expectedError: 'must be a number \\(was a string\\)',
      queryFn: (arg1: string, arg2: number) =>
        builder.issue.where('id', arg1).where('createdAt', '>', arg2),
    },
    {
      name: 'Zod 3 (HasParseFn) with syncedQuery',
      parser: z.tuple([z.string(), z.number()]),
      validArgs: ['test', 789],
      invalidArgs: [999, 'test'],
      expectedError: 'Expected string, received number',
      queryFn: (arg1: string, arg2: number) =>
        builder.issue.where('id', arg1).where('createdAt', '>', arg2),
    },
  ];

  testCases.forEach(
    ({name, parser, validArgs, invalidArgs, expectedError, queryFn}) => {
      test(name, async () => {
        const def = syncedQuery('testQuery', parser as any, queryFn as any);

        expect(def.queryName).toEqual('testQuery');
        expect(def.parse).toBeDefined();
        expect(def.takesContext).toEqual(false);

        const q = def(...validArgs);
        expect(q.customQueryID).toEqual({
          name: 'testQuery',
          args: validArgs,
        });

        const wv = withValidation(def);
        expect(wv.queryName).toEqual('testQuery');

        await expect(() => wv('ignored', ...invalidArgs)).rejects.toThrow(
          new RegExp(expectedError),
        );

        const vq = (await wv('ignored', ...validArgs)).query;
        expect(vq.customQueryID).toEqual({
          name: 'testQuery',
          args: validArgs,
        });
      });
    },
  );
});

describe('Schema compatibility (StandardSchemaV1 and HasParseFn) with context', () => {
  const testCases = [
    {
      name: 'Valibot (StandardSchemaV1) with syncedQueryWithContext',
      parser: valibot.tuple([valibot.string(), valibot.number()]),
      validArgs: ['test', 789],
      invalidArgs: [true, 789],
      expectedError: 'Invalid type: Expected string but received true',
      queryFn: (context: string, arg1: string, arg2: number) =>
        builder.issue
          .where('ownerId', context)
          .where('id', arg1)
          .where('createdAt', '>', arg2),
    },
    {
      name: 'Arktype (StandardSchemaV1) with syncedQueryWithContext',
      parser: type(['string', 'number']),
      validArgs: ['test', 321],
      invalidArgs: [null, 321],
      expectedError: 'must be a string',
      queryFn: (context: string, arg1: string, arg2: number) =>
        builder.issue
          .where('ownerId', context)
          .where('id', arg1)
          .where('createdAt', '>', arg2),
    },
    {
      name: 'Zod 3 (HasParseFn) with syncedQueryWithContext',
      parser: z.tuple([z.string(), z.number()]),
      validArgs: ['test', 654],
      invalidArgs: [false, 654],
      expectedError: 'Expected string, received boolean',
      queryFn: (context: string, arg1: string, arg2: number) =>
        builder.issue
          .where('ownerId', context)
          .where('id', arg1)
          .where('createdAt', '>', arg2),
    },
  ];

  testCases.forEach(
    ({name, parser, validArgs, invalidArgs, expectedError, queryFn}) => {
      test(name, async () => {
        const def = syncedQueryWithContext(
          'testQueryWithContext',
          parser as any,
          queryFn as any,
        );

        expect(def.queryName).toEqual('testQueryWithContext');
        expect(def.parse).toBeDefined();
        expect(def.takesContext).toEqual(true);

        const q = def('context1', ...validArgs);
        expect(q.customQueryID).toEqual({
          name: 'testQueryWithContext',
          args: validArgs,
        });

        const wv = withValidation(def);
        expect(wv.queryName).toEqual('testQueryWithContext');

        await expect(() => wv('context1', ...invalidArgs)).rejects.toThrow(
          new RegExp(expectedError),
        );

        const vq = (await wv('context1', ...validArgs)).query;
        expect(vq.customQueryID).toEqual({
          name: 'testQueryWithContext',
          args: validArgs,
        });
      });
    },
  );
});
