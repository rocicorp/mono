// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import * as z from 'zod';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Transaction} from './custom.ts';
import {
  defineMutator,
  defineMutatorWithType,
  isMutatorDefinition,
  type MutatorDefinition,
} from './mutator.ts';

describe('defineMutator', () => {
  test('creates a mutator definition without validator', () => {
    const mutator = async ({
      args,
      ctx,
      tx,
    }: {
      args: {id: string} | undefined;
      ctx: unknown;
      tx: Transaction<Schema>;
    }) => {
      void args;
      void ctx;
      void tx;
    };

    const def = defineMutator(mutator);

    expect(def).toBe(mutator);
    expect(def.validator).toBeUndefined();
    expect(isMutatorDefinition(def)).toBe(true);
  });

  test('creates a mutator definition with validator', () => {
    const validator: StandardSchemaV1<{a: number}, {b: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: vi.fn(() => ({value: {b: 'test'}})),
      },
    };

    const mutator = async ({
      args,
      ctx,
      tx,
    }: {
      args: {b: string};
      ctx: unknown;
      tx: Transaction<Schema>;
    }) => {
      void args;
      void ctx;
      void tx;
    };

    const def = defineMutator(validator, mutator);

    expect(def).toBe(mutator);
    expect(def.validator).toBe(validator);
    expect(isMutatorDefinition(def)).toBe(true);
  });
});

describe('isMutatorDefinition', () => {
  test.for([
    {
      input: defineMutator(async () => {}),
      expected: true,
      desc: 'valid mutator definition',
    },
    {
      input: async () => {},
      expected: false,
      desc: 'plain function without tag',
    },
    {input: {validator: undefined}, expected: false, desc: 'plain object'},
    {input: null, expected: false, desc: 'null'},
    {input: undefined, expected: false, desc: 'undefined'},
  ])('returns $expected for $desc', ({input, expected}) => {
    expect(isMutatorDefinition(input)).toBe(expected);
  });
});

describe('defineMutatorWithType', () => {
  test('creates mutator with bound schema and context type', () => {
    type Context = {userId: string};
    const define = defineMutatorWithType<Schema, Context>();

    const mutator = async ({
      ctx,
      args,
      tx,
    }: {
      ctx: Context;
      args: {id: string} | undefined;
      tx: Transaction<Schema>;
    }) => {
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      void args;
      void tx;
    };

    const def = define(mutator);

    expect(isMutatorDefinition(def)).toBe(true);
    expect(def.validator).toBeUndefined();
  });

  test('creates mutator with validator and bound schema and context type', () => {
    type Context = {userId: string};
    const validator: StandardSchemaV1<{a: number}, {b: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({value: {b: 'test'}}),
      },
    };

    const define = defineMutatorWithType<Schema, Context>();

    const mutator = async ({
      ctx,
      args,
      tx,
    }: {
      ctx: Context;
      args: {b: string};
      tx: Transaction<Schema>;
    }) => {
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      void args;
      void tx;
    };

    const def = define(validator, mutator);

    expect(isMutatorDefinition(def)).toBe(true);
    expect(def.validator).toBe(validator);
  });

  test('infers args from validator and ctx from bound context type', () => {
    type Context = {userId: string};
    const validator = z.object({name: z.string()});

    const define = defineMutatorWithType<Schema, Context>();

    // Key test: args and ctx types should be inferred without explicit annotations
    const def = define(validator, async ({args, ctx}) => {
      // These should compile - types inferred from validator and Context
      args.name;
      ctx.userId;
    });

    expect(isMutatorDefinition(def)).toBe(true);
  });
});

describe('Type Tests', () => {
  test('MutatorDefinition type structure', () => {
    type TestDef = MutatorDefinition<
      Schema,
      {userId: string},
      {input: number},
      {output: string},
      unknown
    >;

    // Should be a function
    expectTypeOf<TestDef>().toBeFunction();

    // Should have validator property
    expectTypeOf<TestDef>().toHaveProperty('validator');
  });

  test('defineMutator without validator returns correct type', () => {
    const def = defineMutator(
      async ({
        args,
        ctx,
        tx,
      }: {
        args: {id: string};
        ctx: {userId: string};
        tx: Transaction<Schema>;
      }) => {
        expectTypeOf(args).toEqualTypeOf<{id: string}>();
        expectTypeOf(ctx).toEqualTypeOf<{userId: string}>();
        expectTypeOf(tx).toEqualTypeOf<Transaction<Schema>>();
      },
    );

    // Without validator, TInput === TOutput === TArgs
    expectTypeOf(def).toEqualTypeOf<
      MutatorDefinition<
        Schema,
        {userId: string},
        {id: string},
        {id: string},
        unknown
      >
    >();
  });

  test('defineMutator with validator returns correct type', () => {
    const validator: StandardSchemaV1<{a: number}, {b: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: () => ({value: {b: 'test'}}),
      },
    };

    const def = defineMutator(validator, async ({args, ctx, tx}) => {
      // args is TOutput (the validated type)
      expectTypeOf(args).toEqualTypeOf<{b: string}>();
      void ctx;
      void tx;
    });

    // MutatorDefinition still has TInput/TOutput for validator typing
    expectTypeOf(def).toEqualTypeOf<
      MutatorDefinition<Schema, unknown, {a: number}, {b: string}, unknown>
    >();
  });

  test('defineMutatorWithType returns correct type', () => {
    type Context = {userId: string; role: string};
    const define = defineMutatorWithType<Schema, Context>();

    const mutator = async ({
      ctx,
      args,
      tx,
    }: {
      ctx: Context;
      args: undefined;
      tx: Transaction<Schema>;
    }) => {
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      void args;
      void tx;
    };

    const def = define(mutator);

    expectTypeOf(def).toEqualTypeOf<
      MutatorDefinition<Schema, Context, undefined, undefined, unknown>
    >();
  });
});
