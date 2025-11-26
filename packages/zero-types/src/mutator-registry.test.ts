import {expect, test} from 'vitest';
import {defineMutator} from './mutator.ts';
import {
  defineMutators,
  getMutator,
  mustGetMutator,
  isMutatorRegistry,
} from './mutator-registry.ts';

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

test('mutatorName is read-only', () => {
  const mutators = defineMutators({
    user: {
      create: createUser,
    },
  });

  expect(() => {
    // @ts-expect-error - mutatorName is readonly
    mutators.user.create.mutatorName = 'foo';
  }).toThrow(TypeError);
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
