// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {describe, expect, expectTypeOf, test, vi} from 'vitest';
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
import {
  defineMutator,
  defineMutatorWithType,
  type MutationRequest,
  type Mutator,
} from './mutator.ts';

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

describe('Type Tests', () => {
  test('Mutator has correctly typed fn property', () => {
    type Context = {userId: string};

    const define = defineMutatorWithType<Schema, Context>();
    const createItem = define(
      async ({
        args,
        ctx,
        tx,
      }: {
        args: {name: string; count: number};
        ctx: Context;
        tx: Transaction<Schema>;
      }) => {
        void args;
        void ctx;
        void tx;
      },
    );

    const defineMutators_ = defineMutatorsWithType<Schema, Context>();
    const mutators = defineMutators_({
      item: {
        create: createItem,
      },
    });

    // The mutator should be a Mutator type
    expectTypeOf(mutators.item.create).toEqualTypeOf<
      Mutator<Schema, Context, {name: string; count: number}, unknown>
    >();

    // mutatorName should be string
    expectTypeOf(mutators.item.create.mutatorName).toEqualTypeOf<string>();

    // fn should be a function with typed parameters
    expectTypeOf(mutators.item.create.fn).toEqualTypeOf<
      (options: {
        args: {name: string; count: number};
        ctx: Context;
        tx: AnyTransaction;
      }) => Promise<void>
    >();
  });

  test('calling Mutator returns correctly typed MutationRequest', () => {
    type Context = {userId: string};

    const define = defineMutatorWithType<Schema, Context>();
    const updateItem = define(
      async ({
        args,
        ctx,
        tx,
      }: {
        args: {id: string; name: string};
        ctx: Context;
        tx: Transaction<Schema>;
      }) => {
        void args;
        void ctx;
        void tx;
      },
    );

    const defineMutators_ = defineMutatorsWithType<Schema, Context>();
    const mutators = defineMutators_({
      item: {
        update: updateItem,
      },
    });

    const mr = mutators.item.update({id: '123', name: 'test'});

    // MutationRequest should have typed mutator and args
    expectTypeOf(mr).toEqualTypeOf<
      MutationRequest<Schema, Context, {id: string; name: string}, unknown>
    >();
    expectTypeOf(mr.args).toEqualTypeOf<{id: string; name: string}>();
    expectTypeOf(mr.mutator).toEqualTypeOf<
      Mutator<Schema, Context, {id: string; name: string}, unknown>
    >();

    // Can access fn from the MutationRequest's mutator
    expectTypeOf(mr.mutator.fn).toBeFunction();
  });

  test('fn property preserves args type through server-mutator pattern', () => {
    type ServerContext = {userId: string; isAdmin: boolean};

    const define = defineMutatorWithType<Schema, ServerContext>();
    const deleteItem = define(
      async ({
        args,
        ctx,
        tx,
      }: {
        args: {id: string};
        ctx: ServerContext;
        tx: Transaction<Schema>;
      }) => {
        void args;
        void ctx;
        void tx;
      },
    );

    const defineMutators_ = defineMutatorsWithType<Schema, ServerContext>();
    const mutators = defineMutators_({
      item: {
        delete: deleteItem,
      },
    });

    // Simulate server-side pattern: get the mutator and call its fn
    const mutator = mutators.item.delete;

    // The fn should accept the correct types
    // This is the key test: fn.args should be typed as {id: string}
    type FnArgs = Parameters<typeof mutator.fn>[0]['args'];
    expectTypeOf<FnArgs>().toEqualTypeOf<{id: string}>();

    type FnCtx = Parameters<typeof mutator.fn>[0]['ctx'];
    expectTypeOf<FnCtx>().toEqualTypeOf<ServerContext>();
  });
});
