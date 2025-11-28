import {afterEach, describe, expect, expectTypeOf, test, vi} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import type {MutatorResult} from './custom.ts';
import {zeroForTest} from './test-utils.ts';

afterEach(() => vi.restoreAllMocks());

const schema = createSchema({
  tables: [
    table('user')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

type Schema = typeof schema;
type MutatorTx = Transaction<Schema>;

const defineMutator = defineMutatorWithType<Schema, unknown, MutatorTx>();
const defineMutators = defineMutatorsWithType<Schema, unknown>();

describe('zero.mutate(mr) with MutationRequest', () => {
  test('can call mutate with a MutationRequest', async () => {
    const createUser = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const mutators = defineMutators({
      user: {
        create: createUser,
      },
    });

    const z = zeroForTest({
      schema,
      mutators,
    });

    // Create the MutationRequest by calling the mutator
    const mr = mutators.user.create({id: '1', name: 'Alice'});

    // Call z.mutate(mr) to execute it
    const result = z.mutate(mr);

    // Should return a MutatorResult with client and server promises
    expectTypeOf(result).toEqualTypeOf<MutatorResult>();

    await z.close();
  });

  test('throws when mutator is not registered', async () => {
    const createUser = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    // Create mutators but don't pass to Zero
    const mutators = defineMutators({
      user: {
        create: createUser,
      },
    });

    const z = zeroForTest({
      schema,
      // No mutators passed
    });

    const mr = mutators.user.create({id: '1', name: 'Alice'});

    // Should throw because the mutator is not registered
    expect(() => z.mutate(mr)).toThrow(
      'Mutator "user.create" is not registered',
    );

    await z.close();
  });

  test('throws when a different mutator instance is used', async () => {
    const createUser1 = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const createUser2 = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const mutators1 = defineMutators({
      user: {
        create: createUser1,
      },
    });

    const mutators2 = defineMutators({
      user: {
        create: createUser2,
      },
    });

    const z = zeroForTest({
      schema,
      mutators: mutators1,
    });

    // Create MutationRequest from mutators2 (not registered with z)
    const mr = mutators2.user.create({id: '1', name: 'Alice'});

    // Should throw because the exact mutator instance doesn't match
    expect(() => z.mutate(mr)).toThrow(
      'Mutator "user.create" is not registered',
    );

    await z.close();
  });

  test('mutate still works as an object for CRUD operations', async () => {
    const z = zeroForTest({
      schema,
    });

    // TODO(arv): Why is `id` below not readonly?

    // z.mutate should still work as an object for CRUD (when no custom mutators passed)
    expectTypeOf(z.mutate.user.insert).toEqualTypeOf<
      (value: {id: string; readonly name: string}) => Promise<void>
    >();
    expectTypeOf(z.mutate.user.update).toEqualTypeOf<
      (value: {id: string; readonly name?: string | undefined}) => Promise<void>
    >();
    expectTypeOf(z.mutate.user.upsert).toEqualTypeOf<
      (value: {id: string; readonly name: string}) => Promise<void>
    >();
    expectTypeOf(z.mutate.user.delete).toEqualTypeOf<
      (id: {id: string}) => Promise<void>
    >();

    await z.close();
  });

  test('mutate is callable when mutators passed', async () => {
    const createUser = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const mutators = defineMutators({
      user: {
        create: createUser,
      },
    });

    const z = zeroForTest({
      schema,
      mutators,
    });

    // z.mutate is callable with a MutationRequest
    expectTypeOf(z.mutate).toBeCallableWith(
      mutators.user.create({id: '1', name: 'Alice'}),
    );

    await z.close();
  });

  test('two zero instances with different mutator instances of same name', async () => {
    const createUser1 = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const createUser2 = defineMutator(
      async ({tx, args}: {tx: MutatorTx; args: {id: string; name: string}}) => {
        await tx.mutate.user.insert(args);
      },
    );

    const mutators1 = defineMutators({
      user: {
        create: createUser1,
      },
    });

    const mutators2 = defineMutators({
      user: {
        create: createUser2,
      },
    });

    const z1 = zeroForTest({
      schema,
      mutators: mutators1,
    });

    const z2 = zeroForTest({
      schema,
      mutators: mutators2,
    });

    // MutationRequest from mutators1 works with z1
    const mr1 = mutators1.user.create({id: '1', name: 'Alice'});
    const result1 = z1.mutate(mr1);
    expectTypeOf(result1).toEqualTypeOf<MutatorResult>();

    // MutationRequest from mutators2 works with z2
    const mr2 = mutators2.user.create({id: '2', name: 'Bob'});
    const result2 = z2.mutate(mr2);
    expectTypeOf(result2).toEqualTypeOf<MutatorResult>();

    // MutationRequest from mutators1 does NOT work with z2
    expect(() => z2.mutate(mr1)).toThrow(
      'Mutator "user.create" is not registered',
    );

    // MutationRequest from mutators2 does NOT work with z1
    expect(() => z1.mutate(mr2)).toThrow(
      'Mutator "user.create" is not registered',
    );

    await z1.close();
    await z2.close();
  });
});
