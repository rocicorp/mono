// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
import type {AnyTransaction} from './custom.ts';
import {
  defineMutators,
  getMutator,
  isMutatorRegistry,
  iterateMutators,
  mustGetMutator,
} from './mutator-registry.ts';
import {defineMutator} from './mutator.ts';

const createUser = defineMutator(
  ({args, ctx, tx}: {args: {name: string}; ctx: unknown; tx: unknown}) => {
    void args;
    void ctx;
    void tx;
    return Promise.resolve();
  },
);

const deleteUser = defineMutator(
  ({args, ctx, tx}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
    void args;
    void ctx;
    void tx;
    return Promise.resolve();
  },
);

const publishPost = defineMutator(
  ({args, ctx, tx}: {args: {postId: string}; ctx: unknown; tx: unknown}) => {
    void args;
    void ctx;
    void tx;
    return Promise.resolve();
  },
);

test('defineMutators creates a registry with nested mutators', () => {
  const mutators = defineMutators({
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

test('calling a mutator returns a MutationRequest', () => {
  const mutators = defineMutators({
    user: {
      create: createUser,
    },
  });

  const mr = mutators.user.create({name: 'Alice'});

  expect(mr.mutator).toBe(mutators.user.create);
  expect(mr.args).toEqual({name: 'Alice'});
});

test('mutator.fn executes the definition with args, ctx, and tx', async () => {
  const capturedArgs: unknown[] = [];
  const testMutator = defineMutator(
    ({args, ctx, tx}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
      capturedArgs.push({args, ctx, tx});
      return Promise.resolve();
    },
  );

  const mutators = defineMutators({
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
    query: {},
  } as AnyTransaction;
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

  const testMutator = defineMutator(
    validator,
    ({args, ctx, tx}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
      capturedArgs.push({args, ctx, tx});
      return Promise.resolve();
    },
  );

  const mutators = defineMutators({
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
    query: {},
  } as AnyTransaction;

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

test('getMutator looks up by dot-separated name', () => {
  const mutators = defineMutators({
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
});

test('mustGetMutator throws for unknown names', () => {
  const mutators = defineMutators({
    user: {
      create: createUser,
    },
  });

  expect(mustGetMutator(mutators, 'user.create')).toBe(mutators.user.create);
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

  test('callable accepts input type, MutationRequest.args stores input', () => {
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

    // MutationRequest.args should be the original input (string)
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

    const mutators = defineMutators({
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
      query: {},
    } as AnyTransaction;

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

    const mutators = defineMutators({
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

    // MutationRequest.args should be the original input (undefined)
    expect(mr.args).toBe(undefined);

    const mockTx = {
      location: 'client',
      clientID: 'test-client',
      mutationID: 1,
      reason: 'optimistic',
      mutate: {},
      query: {},
    } as AnyTransaction;

    // When fn is called, it should transform undefined to 'default-value'
    await mutators.item.create.fn({
      args: undefined,
      ctx: undefined,
      tx: mockTx,
    });

    expect(capturedArgs).toHaveLength(1);
    expect(capturedArgs[0]).toBe('default-value'); // transformed
  });

  test('MutationRequest preserves input type for server transmission', () => {
    // This test verifies that when a mutation is sent to the server,
    // the args in MutationRequest are the original input (not transformed)

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

    const mutators = defineMutators({
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

    // MutationRequest.args should be input type (count as string)
    // This is what gets sent to the server
    expect(mr.args).toEqual({id: 'abc', count: '123'});
    expectTypeOf(mr.args).toEqualTypeOf<{id: string; count: string}>();
  });
});
