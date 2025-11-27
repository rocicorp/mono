import {expect, test} from 'vitest';
import {
  defineMutators,
  getMutator,
  isMutatorRegistry,
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
