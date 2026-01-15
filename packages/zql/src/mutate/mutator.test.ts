// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Transaction} from './custom.ts';
import {defineMutators} from './mutator-registry.ts';
import {
  defineMutator,
  defineMutatorWithType,
  isMutator,
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

    expect(def.fn).toBe(mutator);
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

    expect(def.fn).toBe(mutator);
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

describe('Type Tests', () => {
  test('MutatorDefinition type structure', () => {
    type TestDef = MutatorDefinition<
      {input: number},
      {output: string},
      {userId: string},
      unknown
    >;

    // Should have a function
    expectTypeOf<TestDef['fn']>().toBeFunction();

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
      MutatorDefinition<{id: string}, {id: string}, {userId: string}, unknown>
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
      MutatorDefinition<{a: number}, {b: string}, unknown, unknown>
    >();
  });
});

describe('Mutator callable type tests', () => {
  test('Mutator without args - callable with 0 arguments', () => {
    const mutators = defineMutators({
      noArgs: defineMutator(({tx}) => {
        void tx;
        return Promise.resolve();
      }),
    } as const);

    // Type test: noArgs() should be callable with no arguments
    expectTypeOf(mutators.noArgs).toBeCallableWith();

    // The result should be a MutateRequest with ReadonlyJSONValue | undefined args
    const mr = mutators.noArgs();
    expectTypeOf(mr.args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
  });

  test('Mutator with required args - requires argument', () => {
    const mutators = defineMutators({
      withArgs: defineMutator(
        ({tx, args}: {args: {id: string; title: string}; tx: unknown}) => {
          void tx;
          void args;
          return Promise.resolve();
        },
      ),
    } as const);

    // Type test: withArgs should require an argument
    expectTypeOf(mutators.withArgs).toBeCallableWith({
      id: 'test',
      title: 'test',
    });

    // @ts-expect-error - should not be callable without args
    mutators.withArgs();

    const mr = mutators.withArgs({id: '1', title: 'test'});
    expectTypeOf(mr.args).toEqualTypeOf<{id: string; title: string}>();
  });

  test('Mutator with optional args - callable with or without argument', () => {
    const mutators = defineMutators({
      optionalArgs: defineMutator(
        ({tx, args}: {args: {id: string} | undefined; tx: unknown}) => {
          void tx;
          void args;
          return Promise.resolve();
        },
      ),
    } as const);

    // Type test: optionalArgs should be callable with no arguments
    expectTypeOf(mutators.optionalArgs).toBeCallableWith();

    // Type test: optionalArgs should also be callable with an argument
    expectTypeOf(mutators.optionalArgs).toBeCallableWith({id: 'test'});

    // Both should work
    const mr1 = mutators.optionalArgs();
    const mr2 = mutators.optionalArgs({id: 'test'});

    expectTypeOf(mr1.args).toEqualTypeOf<{id: string} | undefined>();
    expectTypeOf(mr2.args).toEqualTypeOf<{id: string} | undefined>();
  });
});

describe('isMutator', () => {
  test('returns true for a Mutator from defineMutators', () => {
    const mutators = defineMutators({
      test: defineMutator(async () => {}),
    });

    expect(isMutator(mutators.test)).toBe(true);
  });

  test('returns false for a MutatorDefinition', () => {
    const def = defineMutator(async () => {});
    expect(isMutator(def)).toBe(false);
  });

  test('returns false for non-mutator values', () => {
    expect(isMutator(null)).toBe(false);
    expect(isMutator(undefined)).toBe(false);
    expect(isMutator({})).toBe(false);
    expect(isMutator({mutatorName: 'test'})).toBe(false);
    expect(isMutator({fn: () => {}})).toBe(false);
    expect(isMutator(() => {})).toBe(false);
  });
});

describe('Mutator phantom type (~)', () => {
  test('Mutator has phantom type with correct structure', () => {
    type TestContext = {userId: string};

    const mutators = defineMutators({
      testMutator: defineMutator(
        ({
          args,
          ctx,
          tx,
        }: {
          args: {id: string; title: string};
          ctx: TestContext;
          tx: unknown;
        }) => {
          void args;
          void ctx;
          void tx;
          return Promise.resolve();
        },
      ),
    } as const);

    const mutator = mutators.testMutator;

    // Verify the phantom type structure has all expected properties
    expectTypeOf(mutator['~']['$input']).toEqualTypeOf<{
      id: string;
      title: string;
    }>();
    expectTypeOf(mutator['~']['$schema']).toEqualTypeOf<Schema>();
    expectTypeOf(mutator['~']['$context']).toEqualTypeOf<TestContext>();
    expectTypeOf(mutator['~']['$wrappedTransaction']).toEqualTypeOf<unknown>();
  });

  test('Mutator phantom type reflects undefined args', () => {
    const mutators = defineMutators({
      noArgs: defineMutator(({tx}: {tx: unknown}) => {
        void tx;
        return Promise.resolve();
      }),
    } as const);

    const mutator = mutators.noArgs;

    // When args is not specified, $input should be ReadonlyJSONValue | undefined
    expectTypeOf(mutator['~']['$input']).toEqualTypeOf<
      ReadonlyJSONValue | undefined
    >();
  });

  test('Mutator phantom type reflects optional args', () => {
    const mutators = defineMutators({
      optionalArgs: defineMutator(
        ({args, tx}: {args: {id: string} | undefined; tx: unknown}) => {
          void args;
          void tx;
          return Promise.resolve();
        },
      ),
    } as const);

    const mutator = mutators.optionalArgs;

    expectTypeOf(mutator['~']['$input']).toEqualTypeOf<
      {id: string} | undefined
    >();
  });

  test('MutatorDefinition phantom carries input/output/context/wrapped types', () => {
    type Ctx = {user: string};
    type Wrapped = {tx: true};
    const validator: StandardSchemaV1<{in: number}, {out: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: data => ({value: {out: String((data as {in: number}).in)}}),
      },
    };

    const def = defineMutatorWithType<Schema, Ctx, Wrapped>()(
      validator,
      async ({args, ctx, tx}) => {
        void args;
        void ctx;
        void tx;
      },
    );

    expectTypeOf(def['~']['$input']).toEqualTypeOf<{in: number}>();
    expectTypeOf(def['~']['$output']).toEqualTypeOf<{out: string}>();
    expectTypeOf(def['~']['$context']).toEqualTypeOf<Ctx>();
    expectTypeOf(def['~']['$wrappedTransaction']).toEqualTypeOf<Wrapped>();
  });
});
