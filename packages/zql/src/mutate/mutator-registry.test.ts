// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {assert, describe, expect, expectTypeOf, test, vi} from 'vitest';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {AnyTransaction, Transaction} from './custom.ts';
import {
  defineMutators,
  defineMutatorsWithType,
  getMutator,
  isMutatorRegistry,
  iterateMutators,
  mustGetMutator,
} from './mutator-registry.ts';
import {defineMutator, defineMutatorWithType, type Mutator} from './mutator.ts';

const schema = createSchema({
  tables: [
    table('foo').columns({id: string(), count: number()}).primaryKey('id'),
  ],
});

type Context = {
  userId: string;
};
type DbTransaction = {
  db: true;
};

const createUser = defineMutatorWithType<
  typeof schema,
  Context,
  DbTransaction
>()<{name: string}>(({args, ctx, tx}) => {
  void args;
  void ctx;
  void tx;
  return Promise.resolve();
});

const deleteUser = defineMutatorWithType<
  typeof schema,
  Context,
  DbTransaction
>()<{id: string}>(({args, ctx, tx}) => {
  void args;
  void ctx;
  void tx;
  return Promise.resolve();
});

const publishPost = defineMutatorWithType<
  typeof schema,
  Context,
  DbTransaction
>()<{postId: string}>(({args, ctx, tx}) => {
  void args;
  void ctx;
  void tx;
  return Promise.resolve();
});

test('defineMutators creates a registry with nested mutators', () => {
  const mutators = defineMutatorsWithType<typeof schema>()({
    user: {
      create: createUser,
      delete: deleteUser,
    },
    post: {
      publish: publishPost,
    },
  });

  expect(isMutatorRegistry(mutators)).toBe(true);
  expect(mutators.user.create.mutatorName).toBe('user.create');
  expect(mutators.user.delete.mutatorName).toBe('user.delete');
  expect(mutators.post.publish.mutatorName).toBe('post.publish');
});

test('calling a mutator returns a MutateRequest', () => {
  const mutators = defineMutators({
    user: {
      create: createUser,
    },
  });

  const mr = mutators.user.create({name: 'Alice'});

  expect(mr.mutator).toBe(mutators.user.create);
  expect(mr.args).toEqual({name: 'Alice'});
});

test('MutateRequest phantom tags carry schema, context, and wrapped transaction', () => {
  const mutators = defineMutatorsWithType<typeof schema>()({
    user: {
      create: createUser,
    },
  });

  const mr = mutators.user.create({name: 'Alice'});

  expectTypeOf(mutators['~']['$schema']).toEqualTypeOf<typeof schema>();
  expectTypeOf(mr['~']['$input']).toEqualTypeOf<{name: string}>();
  expectTypeOf(mr['~']['$schema']).toEqualTypeOf<typeof schema>();
  expectTypeOf(mr['~']['$context']).toEqualTypeOf<Context>();
  expectTypeOf(mr['~']['$wrappedTransaction']).toEqualTypeOf<DbTransaction>();
});

test('mutator.fn executes the definition with args, ctx, and tx', async () => {
  const capturedArgs: unknown[] = [];

  const testMutator = defineMutatorWithType<typeof schema>()(({
    args,
    ctx,
    tx,
  }: {
    args: {id: string};
    ctx: unknown;
    tx: unknown;
  }) => {
    capturedArgs.push({args, ctx, tx});
    return Promise.resolve();
  });

  const mutators = defineMutatorsWithType<typeof schema>()({
    item: {
      test: testMutator,
    },
  });

  const mockTx = {
    location: 'client',
    clientID: 'test-client',
    mutationID: 1,
    reason: 'optimistic',
    mutate: {},
  } as Transaction<typeof schema, unknown>;
  const mockCtx = {user: 'testuser'};

  await mutators.item.test.fn({
    args: {id: '123'},
    ctx: mockCtx,
    tx: mockTx,
  });

  expect(capturedArgs).toHaveLength(1);
  expect(capturedArgs[0]).toEqual({
    args: {id: '123'},
    ctx: mockCtx,
    tx: mockTx,
  });
});

test('mutator.fn validates args when validator is provided', async () => {
  const capturedArgs: unknown[] = [];
  const validator: StandardSchemaV1<{id: string}, {id: string}> = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: vi.fn(input => ({value: input})),
    },
  };

  const testMutator = defineMutatorWithType<typeof schema>()(
    validator,
    ({args, ctx, tx}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
      capturedArgs.push({args, ctx, tx});
      return Promise.resolve();
    },
  );

  const mutators = defineMutatorsWithType<typeof schema>()({
    item: {
      test: testMutator,
    },
  });

  const mockTx = {
    location: 'client',
    clientID: 'test-client',
    mutationID: 1,
    reason: 'optimistic',
    mutate: {},
  } as Transaction<typeof schema, unknown>;

  await mutators.item.test.fn({
    args: {id: '456'},
    ctx: undefined,
    tx: mockTx,
  });

  expect(validator['~standard'].validate).toHaveBeenCalledWith({id: '456'});
  expect(capturedArgs).toHaveLength(1);
  expect(capturedArgs[0]).toEqual({
    args: {id: '456'},
    ctx: undefined,
    tx: mockTx,
  });
});

test('mutator.fn throws on validation failure and does not run', async () => {
  const validator: StandardSchemaV1<{id: string}, {id: string}> = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: () => ({
        issues: [{message: 'invalid'}],
      }),
    },
  };

  const fn = vi.fn();

  const testMutator = defineMutatorWithType<typeof schema>()(
    validator,
    ({args, ctx, tx}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
      fn(args, ctx, tx);
      return Promise.resolve();
    },
  );

  const mutators = defineMutatorsWithType<typeof schema>()({
    item: {
      test: testMutator,
    },
  });

  await expect(
    mutators.item.test.fn({
      args: {id: 'bad'},
      ctx: undefined,
      tx: {} as Transaction<typeof schema, unknown>,
    }),
  ).rejects.toThrowErrorMatchingInlineSnapshot(
    `[Error: Validation failed for mutator item.test: invalid]`,
  );

  expect(fn).not.toHaveBeenCalled();
});

test('getMutator looks up by dot-separated name and returns the correct type', () => {
  const mutators = defineMutatorsWithType<typeof schema>()({
    user: {
      create: createUser,
      delete: deleteUser,
    },
    post: {
      publish: publishPost,
    },
  });

  expect(getMutator(mutators, 'user.create')).toBe(mutators.user.create);
  expect(getMutator(mutators, 'user.delete')).toBe(mutators.user.delete);
  expect(getMutator(mutators, 'post.publish')).toBe(mutators.post.publish);
  expect(getMutator(mutators, 'nonexistent')).toBeUndefined();
  expect(getMutator(mutators, 'user.nonexistent')).toBeUndefined();
  expectTypeOf(getMutator(mutators, 'user.create')).toEqualTypeOf<
    | Mutator<
        ReadonlyJSONValue | undefined,
        typeof schema,
        Context,
        DbTransaction
      >
    | undefined
  >();
});

test('mustGetMutator throws for unknown names and returns the correct type', () => {
  const mutators = defineMutatorsWithType<typeof schema>()({
    user: {
      create: createUser,
      delete: deleteUser,
    },
  });

  expect(mustGetMutator(mutators, 'user.create')).toBe(mutators.user.create);
  expectTypeOf(mustGetMutator(mutators, 'user.create')).toEqualTypeOf<
    Mutator<
      ReadonlyJSONValue | undefined,
      typeof schema,
      Context,
      DbTransaction
    >
  >();
  expect(() => mustGetMutator(mutators, 'nonexistent')).toThrow(
    'Mutator not found: nonexistent',
  );
});

test('isMutatorRegistry returns false for non-registries', () => {
  expect(isMutatorRegistry(null)).toBe(false);
  expect(isMutatorRegistry(undefined)).toBe(false);
  expect(isMutatorRegistry({})).toBe(false);
  expect(isMutatorRegistry({user: {create: createUser}})).toBe(false);
});

test('iterateMutators yields all mutators in the registry', () => {
  const mutators = defineMutators({
    user: {
      create: createUser,
      delete: deleteUser,
    },
    post: {
      publish: publishPost,
    },
  });

  const allMutators = [...iterateMutators(mutators)];

  expect(allMutators).toHaveLength(3);
  expect(allMutators).toContain(mutators.user.create);
  expect(allMutators).toContain(mutators.user.delete);
  expect(allMutators).toContain(mutators.post.publish);
});

test('defineMutators extends a registry with overrides', () => {
  const baseMutators = defineMutators({
    user: {
      create: createUser,
      delete: deleteUser,
    },
    post: {
      publish: publishPost,
    },
  });

  const overrideCreate = defineMutator(
    ({args, ctx, tx}: {args: {name: string}; ctx: unknown; tx: unknown}) => {
      void args;
      void ctx;
      void tx;
      return Promise.resolve();
    },
  );

  const archivePost = defineMutator(
    ({args, ctx, tx}: {args: {postId: string}; ctx: unknown; tx: unknown}) => {
      void args;
      void ctx;
      void tx;
      return Promise.resolve();
    },
  );

  const extendedMutators = defineMutators(baseMutators, {
    user: {
      create: overrideCreate, // Override
    },
    post: {
      archive: archivePost, // Add new
    },
  });

  expect(isMutatorRegistry(extendedMutators)).toBe(true);

  // Overridden mutator should have same name but different reference
  expect(extendedMutators.user.create.mutatorName).toBe('user.create');
  expect(extendedMutators.user.create).not.toBe(baseMutators.user.create);

  // Inherited mutator should be the same reference
  expect(extendedMutators.user.delete).toBe(baseMutators.user.delete);
  expect(extendedMutators.user.delete.mutatorName).toBe('user.delete');

  // Inherited from base
  expect(extendedMutators.post.publish).toBe(baseMutators.post.publish);
  expect(extendedMutators.post.publish.mutatorName).toBe('post.publish');

  // New mutator added
  expect(extendedMutators.post.archive.mutatorName).toBe('post.archive');
});

test('defineMutators merges two definition trees', () => {
  const archivePost = defineMutator(
    ({args, ctx, tx}: {args: {postId: string}; ctx: unknown; tx: unknown}) => {
      void args;
      void ctx;
      void tx;
      return Promise.resolve();
    },
  );

  const baseDefs = {
    user: {
      create: createUser,
      delete: deleteUser,
    },
  };

  const overrideDefs = {
    user: {
      delete: deleteUser, // Override (same definition, new mutator instance)
    },
    post: {
      archive: archivePost, // New namespace and mutator
    },
  };

  const mutators = defineMutators(baseDefs, overrideDefs);

  expect(isMutatorRegistry(mutators)).toBe(true);
  expect(mutators.user.create.mutatorName).toBe('user.create');
  expect(mutators.user.delete.mutatorName).toBe('user.delete');
  expect(mutators.post.archive.mutatorName).toBe('post.archive');
});

test('defineMutators deep merges nested namespaces', () => {
  const baseMutators = defineMutators({
    admin: {
      user: {
        create: createUser,
        delete: deleteUser,
      },
    },
  });

  const banUser = defineMutator(
    ({args, ctx, tx}: {args: {userId: string}; ctx: unknown; tx: unknown}) => {
      void args;
      void ctx;
      void tx;
      return Promise.resolve();
    },
  );

  const extendedMutators = defineMutators(baseMutators, {
    admin: {
      user: {
        ban: banUser, // Add new mutator to nested namespace
      },
    },
  });

  // Original mutators preserved
  expect(extendedMutators.admin.user.create).toBe(
    baseMutators.admin.user.create,
  );
  expect(extendedMutators.admin.user.delete).toBe(
    baseMutators.admin.user.delete,
  );

  // New mutator added
  expect(extendedMutators.admin.user.ban.mutatorName).toBe('admin.user.ban');
});

describe('input/output type separation', () => {
  function makeValidator<Input, Output>(
    validate: (data: unknown) => Output,
  ): StandardSchemaV1<Input, Output> {
    return {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: data => {
          try {
            return {value: validate(data)};
          } catch (e) {
            return {issues: [{message: (e as Error).message}]};
          }
        },
      },
    };
  }

  test('callable accepts input type, MutateRequest.args stores input', () => {
    // Validator transforms string to number
    const stringToNumberValidator = makeValidator<string, number>(data => {
      const num = parseInt(data as string, 10);
      if (isNaN(num)) throw new Error('Expected numeric string');
      return num;
    });

    const mutators = defineMutators({
      item: {
        update: defineMutator(
          stringToNumberValidator,
          async ({args}: {args: number; ctx: unknown; tx: unknown}) => {
            // args is the transformed number type
            expectTypeOf(args).toEqualTypeOf<number>();
            void args;
          },
        ),
      },
    });

    // Type test: callable should accept string (input type), not number (output type)
    expectTypeOf(mutators.item.update).parameter(0).toEqualTypeOf<string>();

    // Call with string input
    const mr = mutators.item.update('42');

    // MutateRequest.args should be the original input (string)
    expect(mr.args).toBe('42');
    expectTypeOf(mr.args).toEqualTypeOf<string>();
  });

  test('fn validates and transforms args before passing to mutator', async () => {
    const capturedArgs: unknown[] = [];

    // Validator transforms string to number
    const stringToNumberValidator = makeValidator<string, number>(data => {
      const num = parseInt(data as string, 10);
      if (isNaN(num)) throw new Error('Expected numeric string');
      return num;
    });

    const mutators = defineMutatorsWithType<typeof schema>()({
      item: {
        update: defineMutator(
          stringToNumberValidator,
          async ({args}: {args: number; ctx: unknown; tx: unknown}) => {
            capturedArgs.push(args);
          },
        ),
      },
    });

    const mockTx = {
      location: 'client',
      clientID: 'test-client',
      mutationID: 1,
      reason: 'optimistic',
      mutate: {},
    } as Transaction<typeof schema, unknown>;

    // Call fn with string input (simulating server receiving raw args)
    await mutators.item.update.fn({
      args: '42',
      ctx: undefined,
      tx: mockTx,
    });

    // The mutator function should receive the transformed number
    expect(capturedArgs).toHaveLength(1);
    expect(capturedArgs[0]).toBe(42); // transformed to number
  });

  test('validator with default value transform', async () => {
    const capturedArgs: unknown[] = [];

    // Validator provides default when input is undefined
    const withDefaultValidator = makeValidator<string | undefined, string>(
      data => (data === undefined ? 'default-value' : (data as string)),
    );

    const mutators = defineMutatorsWithType<typeof schema>()({
      item: {
        create: defineMutator(
          withDefaultValidator,
          async ({args}: {args: string; ctx: unknown; tx: unknown}) => {
            capturedArgs.push(args);
          },
        ),
      },
    });

    // Type test: callable should accept string | undefined (input type)
    expectTypeOf(mutators.item.create)
      .parameter(0)
      .toEqualTypeOf<string | undefined>();

    // Call with undefined
    const mr = mutators.item.create(undefined);

    // MutateRequest.args should be the original input (undefined)
    expect(mr.args).toBe(undefined);

    const mockTx = {
      location: 'client',
      clientID: 'test-client',
      mutationID: 1,
      reason: 'optimistic',
      mutate: {},
    } as Transaction<typeof schema, unknown>;

    // When fn is called, it should transform undefined to 'default-value'
    await mutators.item.create.fn({
      args: undefined,
      ctx: undefined,
      tx: mockTx,
    });

    expect(capturedArgs).toHaveLength(1);
    expect(capturedArgs[0]).toBe('default-value'); // transformed
  });

  test('MutateRequest preserves input type for server transmission', () => {
    // This test verifies that when a mutation is sent to the server,
    // the args in MutateRequest are the original input (not transformed)

    const transformValidator = makeValidator<
      {id: string; count: string},
      {id: string; count: number}
    >(data => {
      const input = data as {id: string; count: string};
      return {
        id: input.id,
        count: parseInt(input.count, 10),
      };
    });

    const mutators = defineMutatorsWithType<typeof schema>()({
      item: {
        update: defineMutator(
          transformValidator,
          async ({
            args,
          }: {
            args: {id: string; count: number};
            ctx: unknown;
            tx: unknown;
          }) => {
            // Mutator receives transformed args with count as number
            expectTypeOf(args.count).toEqualTypeOf<number>();
            void args;
          },
        ),
      },
    });

    // Callable accepts input type (count as string)
    expectTypeOf(mutators.item.update).parameter(0).toEqualTypeOf<{
      id: string;
      count: string;
    }>();

    const mr = mutators.item.update({id: 'abc', count: '123'});

    // MutateRequest.args should be input type (count as string)
    // This is what gets sent to the server
    expect(mr.args).toEqual({id: 'abc', count: '123'});
    expectTypeOf(mr.args).toEqualTypeOf<{id: string; count: string}>();
  });
});

describe('type inference', () => {
  test('defineMutator infers args, context, and transaction types', () => {
    type Context = {userId: string};
    type DbTransaction = {db: true};

    const def = defineMutator<
      {id: string},
      typeof schema,
      Context,
      DbTransaction
    >(async ({args, ctx, tx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      expectTypeOf(tx).toEqualTypeOf<
        Transaction<typeof schema, DbTransaction>
      >();
    });

    expectTypeOf<typeof def.fn>().parameter(0).toEqualTypeOf<{
      args: {id: string};
      ctx: Context;
      tx: AnyTransaction;
    }>();
  });

  test('defineMutators preserves types from defineMutator and injects schema', () => {
    type Context1 = {userId: string};
    type Context2 = {some: 'baz'};
    type Context3 = {bool: true};
    type DbTransaction1 = {db: true};
    type DbTransaction2 = {db: false};
    type DbTransaction3 = {db: 3};

    const mutators = defineMutatorsWithType<typeof schema>()({
      def1: defineMutator<
        {id: string},
        typeof schema,
        Context1,
        DbTransaction1
      >(async ({args, ctx, tx}) => {
        expectTypeOf(args).toEqualTypeOf<{id: string}>();
        expectTypeOf(ctx).toEqualTypeOf<Context1>();
        expectTypeOf(tx).toEqualTypeOf<
          Transaction<typeof schema, DbTransaction1>
        >();
      }),
      def2: defineMutator<
        {id: string},
        typeof schema,
        Context2,
        DbTransaction2
      >(async ({args, ctx, tx}) => {
        expectTypeOf(args).toEqualTypeOf<{id: string}>();
        expectTypeOf(ctx).toEqualTypeOf<Context2>();
        expectTypeOf(tx).toEqualTypeOf<
          Transaction<typeof schema, DbTransaction2>
        >();
      }),
      def3: defineMutator(async ({args, ctx, tx}) => {
        expectTypeOf(args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
        expectTypeOf(ctx).toEqualTypeOf<unknown>();
        expectTypeOf(tx).toEqualTypeOf<Transaction<Schema, unknown>>();
      }),
      def4: defineMutator(
        async ({
          args,
          ctx,
          tx,
        }: {
          args: string;
          ctx: Context3;
          tx: Transaction<typeof schema, DbTransaction3>;
        }) => {
          expectTypeOf(args).toEqualTypeOf<string>();
          expectTypeOf(ctx).toEqualTypeOf<Context3>();
          expectTypeOf(tx).toEqualTypeOf<
            Transaction<typeof schema, DbTransaction3>
          >();
        },
      ),
    });

    expectTypeOf(mutators.def1).parameter(0).toEqualTypeOf<{id: string}>();

    const request = mutators.def1({id: '123'});
    expectTypeOf(request.args).toEqualTypeOf<{id: string}>();
    expectTypeOf<typeof request.mutator.fn>().parameter(0).toEqualTypeOf<{
      args: {id: string};
      ctx: Context1;
      tx: Transaction<typeof schema, DbTransaction1>;
    }>();

    const request2 = mutators.def2({id: '123'});
    expectTypeOf(request2.args).toEqualTypeOf<{id: string}>();
    expectTypeOf<typeof request2.mutator.fn>().parameter(0).toEqualTypeOf<{
      args: {id: string};
      ctx: Context2;
      tx: Transaction<typeof schema, DbTransaction2>;
    }>();

    expectTypeOf(mutators.def3)
      .parameter(0)
      .toEqualTypeOf<ReadonlyJSONValue | undefined>();
    const request3 = mutators.def3('whatever');
    expectTypeOf(request3.args).toEqualTypeOf<ReadonlyJSONValue | undefined>();
    expectTypeOf<typeof request3.mutator.fn>().parameter(0).toEqualTypeOf<{
      args: ReadonlyJSONValue | undefined;
      ctx: unknown;
      tx: Transaction<typeof schema, unknown>;
    }>();

    const request4 = mutators.def4('test-string');
    expectTypeOf(request4.args).toEqualTypeOf<string>();
    expectTypeOf<typeof request4.mutator.fn>().parameter(0).toEqualTypeOf<{
      args: string;
      ctx: Context3;
      tx: Transaction<typeof schema, DbTransaction3>;
    }>();
  });

  test('defineMutators should show ts errors for invalid types', () => {
    const invalidDef = {something: 'invalid'};
    const invalidDef2 = {more: 'invalid'};

    expectTypeOf(
      // @ts-expect-error invalid type
      () => defineMutators({invalidDef}),
    ).returns.toBeNever();

    expectTypeOf(
      // @ts-expect-error invalid type
      () => defineMutators({createUser}, {invalidDef2}),
    ).returns.toBeNever();

    expectTypeOf(
      // @ts-expect-error invalid type
      () => defineMutators({invalidDef}, {invalidDef2}),
    ).returns.toBeNever();
  });
});

describe('defineMutatorWithType', () => {
  test('binds schema and context types correctly', () => {
    type Context = {userId: string; role: 'admin' | 'user'};
    type Tx = {db: 'postgres'};

    const define = defineMutatorWithType<typeof schema, Context, Tx>();

    const mutator = define<{id: string}>(async ({args, ctx, tx}) => {
      expectTypeOf(args).toEqualTypeOf<{id: string}>();
      expectTypeOf(ctx).toEqualTypeOf<Context>();
      expectTypeOf(tx).toEqualTypeOf<Transaction<typeof schema, Tx>>();
    });

    const mutators = defineMutatorsWithType<typeof schema>()({test: mutator});

    expectTypeOf(mutators.test).toEqualTypeOf<
      Mutator<{id: string}, typeof schema, Context, Tx>
    >();
  });

  test('defaults context and wrapped transaction to unknown', () => {
    const define = defineMutatorWithType<typeof schema>();

    const mutator = define(async ({ctx, tx}) => {
      expectTypeOf(ctx).toEqualTypeOf<unknown>();
      expectTypeOf(tx).toEqualTypeOf<Transaction<typeof schema, unknown>>();
    });

    const mutators = defineMutatorsWithType<typeof schema>()({test: mutator});

    expectTypeOf(mutators.test).toEqualTypeOf<
      Mutator<ReadonlyJSONValue | undefined, typeof schema, unknown, unknown>
    >();
  });

  test('works with validator', () => {
    type Context = {workspaceId: string};
    const define = defineMutatorWithType<typeof schema, Context>();

    const mutator = define(
      ((v: unknown) => v) as unknown as StandardSchemaV1<
        {raw: number},
        {validated: string}
      >,
      async ({args, ctx}) => {
        expectTypeOf(args).toEqualTypeOf<{validated: string}>();
        expectTypeOf(ctx).toEqualTypeOf<Context>();
      },
    );

    const mutators = defineMutatorsWithType<typeof schema>()({test: mutator});

    expectTypeOf(mutators.test).toEqualTypeOf<
      Mutator<{raw: number}, typeof schema, Context, unknown>
    >();
  });

  test('produces a working mutator at runtime', async () => {
    const define = defineMutatorWithType<typeof schema, Context>();

    const fn = vi.fn();
    const mutator = define<{name: string}>(({args, ctx}) => {
      fn(args, ctx);
      return Promise.resolve();
    });

    const mutators = defineMutatorsWithType<typeof schema>()({test: mutator});
    expect(mutators.test.mutatorName).toBe('test');

    await mutators.test.fn({
      args: {name: 'alice'},
      ctx: {userId: '123'},
      tx: {} as Transaction<typeof schema, unknown>,
    });

    expect(fn).toHaveBeenCalledWith({name: 'alice'}, {userId: '123'});
  });
});

describe('isMutatorRegistry type tests', () => {
  test('preserves registry type after type guard', () => {
    const mutators = defineMutatorsWithType<typeof schema>()({
      user: {
        create: createUser,
      },
    });

    assert(isMutatorRegistry(mutators), 'mutators is not a MutatorRegistry');

    expectTypeOf(mutators.user.create).toEqualTypeOf<
      Mutator<{name: string}, typeof schema, Context, DbTransaction>
    >();
  });

  test('narrows unknown to registry type', () => {
    const maybeRegistry: unknown = defineMutators({
      test: createUser,
    });

    assert(
      isMutatorRegistry(maybeRegistry),
      'maybeRegistry is not a MutatorRegistry',
    );

    expectTypeOf(maybeRegistry).toHaveProperty('test');
  });
});
