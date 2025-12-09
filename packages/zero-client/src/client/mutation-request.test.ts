import {afterEach, describe, expect, expectTypeOf, test, vi} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {createCRUDBuilder} from '../../../zql/src/mutate/crud.ts';
import type {
  DeleteID,
  InsertValue,
  Transaction,
  UpdateValue,
  UpsertValue,
} from '../../../zql/src/mutate/custom.ts';
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

const crud = createCRUDBuilder(schema);

type Schema = typeof schema;
type MutatorTx = Transaction<Schema>;

const defineMutatorTyped = defineMutatorWithType<Schema, unknown, MutatorTx>();
const defineUserMutator = defineMutatorTyped<{id: string; name: string}>;
const defineUserMutators = (def: ReturnType<typeof defineUserMutator>) =>
  defineMutatorsWithType<Schema>()({user: {create: def}});

describe('zero.mutate(mr) with MutationRequest', () => {
  test('can call mutate with a MutationRequest', async () => {
    const createUser = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const mutators = defineUserMutators(createUser);

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
    const createUser = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    // Create mutators but don't pass to Zero
    const mutators = defineUserMutators(createUser);

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
    const createUser1 = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const createUser2 = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const mutators1 = defineUserMutators(createUser1);

    const mutators2 = defineUserMutators(createUser2);

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
      schema: {...schema, enableLegacyMutators: true},
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
    const createUser = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const mutators = defineUserMutators(createUser);

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
    const createUser1 = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const createUser2 = defineUserMutator(async ({tx, args}) => {
      await tx.mutate(crud.user.insert(args));
    });

    const mutators1 = defineUserMutators(createUser1);

    const mutators2 = defineUserMutators(createUser2);

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

describe('CRUD patterns on client', () => {
  describe('modern pattern: tx.mutate(crud.table.op(args))', () => {
    test('works without enableLegacyMutators (default)', async () => {
      const schemaModern = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
        // enableLegacyMutators not set - defaults to false
      });

      const crudModern = createCRUDBuilder(schemaModern);

      const mutators = defineMutatorsWithType<typeof schemaModern>()({
        user: {
          create: defineMutatorWithType<typeof schemaModern>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            // Modern pattern: pass CRUD request to tx.mutate()
            await tx.mutate(crudModern.user.insert(args));
          }),
          update: defineMutatorWithType<typeof schemaModern>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            await tx.mutate(crudModern.user.update(args));
          }),
          delete: defineMutatorWithType<typeof schemaModern>()<{id: string}>(
            async ({tx, args}) => {
              await tx.mutate(crudModern.user.delete(args));
            },
          ),
        },
      });

      const z = zeroForTest({
        schema: schemaModern,
        mutators,
      });

      // Verify tx.mutate is callable but NOT an object with CRUD methods
      const mr = mutators.user.create({id: '1', name: 'Alice'});
      const result = z.mutate(mr);
      expectTypeOf(result).toEqualTypeOf<MutatorResult>();
      await result.client;

      // Type test: z.mutate should NOT have direct CRUD methods
      // @ts-expect-error - user table should not exist when legacy mutators disabled
      void z.mutate.user;

      await z.close();
    });

    test('works with enableLegacyMutators: false explicitly', async () => {
      const schemaExplicit = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: false,
      });

      const crudExplicit = createCRUDBuilder(schemaExplicit);

      const mutators = defineMutatorsWithType<typeof schemaExplicit>()({
        user: {
          create: defineMutatorWithType<typeof schemaExplicit>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            await tx.mutate(crudExplicit.user.insert(args));
          }),
        },
      });

      const z = zeroForTest({
        schema: schemaExplicit,
        mutators,
      });

      const mr = mutators.user.create({id: '1', name: 'Bob'});
      await z.mutate(mr).client;

      // Type test: z.mutate should NOT have direct CRUD methods
      // @ts-expect-error - user table should not exist when legacy mutators disabled
      void z.mutate.user;

      await z.close();
    });

    test('works with enableLegacyMutators: true', async () => {
      const schemaLegacy = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: true,
      });

      const crudLegacy = createCRUDBuilder(schemaLegacy);

      const mutators = defineMutatorsWithType<typeof schemaLegacy>()({
        user: {
          create: defineMutatorWithType<typeof schemaLegacy>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            // Modern pattern also works when legacy is enabled
            await tx.mutate(crudLegacy.user.insert(args));
          }),
        },
      });

      const z = zeroForTest({
        schema: schemaLegacy,
        mutators,
      });

      const mr = mutators.user.create({id: '1', name: 'Charlie'});
      await z.mutate(mr).client;

      // Type test: z.mutate should ALSO have direct CRUD methods
      expectTypeOf(z.mutate.user.insert).toBeFunction();

      await z.close();
    });
  });

  describe('legacy pattern: tx.mutate.table.op(args)', () => {
    test('only available when enableLegacyMutators: true', async () => {
      const schemaLegacy = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: true,
      });

      type LegacyTx = Transaction<typeof schemaLegacy>;

      const mutators = defineMutatorsWithType<typeof schemaLegacy>()({
        user: {
          create: defineMutatorWithType<typeof schemaLegacy>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            // Legacy pattern: direct method call on tx.mutate
            await tx.mutate.user.insert(args);
          }),
          update: defineMutatorWithType<typeof schemaLegacy>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            await tx.mutate.user.update(args);
          }),
          upsert: defineMutatorWithType<typeof schemaLegacy>()<{
            id: string;
            name: string;
          }>(async ({tx, args}) => {
            await tx.mutate.user.upsert(args);
          }),
          delete: defineMutatorWithType<typeof schemaLegacy>()<{id: string}>(
            async ({tx, args}) => {
              await tx.mutate.user.delete(args);
            },
          ),
        },
      });

      const z = zeroForTest({
        schema: schemaLegacy,
        mutators,
      });

      // All CRUD operations should work
      await z.mutate(mutators.user.create({id: '1', name: 'Legacy User'}))
        .client;
      await z.mutate(mutators.user.update({id: '1', name: 'Updated User'}))
        .client;
      await z.mutate(mutators.user.upsert({id: '2', name: 'Upserted User'}))
        .client;
      await z.mutate(mutators.user.delete({id: '1'})).client;

      // Type test: tx.mutate should have legacy methods with correct signatures
      type TxMutateType = LegacyTx['mutate'];

      // Verify 'user' property exists on MutateCRUD when enableLegacyMutators is true
      type HasUserProp = 'user' extends keyof TxMutateType ? true : false;
      expectTypeOf<HasUserProp>().toEqualTypeOf<true>();

      // Verify UserMutators has all CRUD operations
      type UserMutators = TxMutateType['user'];
      type UserTableSchema = (typeof schemaLegacy)['tables']['user'];

      expectTypeOf<UserMutators['insert']>().toEqualTypeOf<
        (value: InsertValue<UserTableSchema>) => Promise<void>
      >();
      expectTypeOf<UserMutators['update']>().toEqualTypeOf<
        (value: UpdateValue<UserTableSchema>) => Promise<void>
      >();
      expectTypeOf<UserMutators['upsert']>().toEqualTypeOf<
        (value: UpsertValue<UserTableSchema>) => Promise<void>
      >();
      expectTypeOf<UserMutators['delete']>().toEqualTypeOf<
        (id: DeleteID<UserTableSchema>) => Promise<void>
      >();

      await z.close();
    });

    test('tx.mutate has no direct CRUD methods when enableLegacyMutators is false', () => {
      const schemaModern = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: false,
      });

      type ModernTx = Transaction<typeof schemaModern>;

      // Type test: tx.mutate should NOT have legacy methods when disabled
      // Instead we verify the type shape
      type MutateType = ModernTx['mutate'];

      // MutateType should be callable
      expectTypeOf<MutateType>().toBeCallableWith(
        {} as {
          schema: typeof schemaModern;
          table: 'user';
          kind: 'insert';
          args: {id: string; name: string};
        },
      );

      // MutateType should NOT have 'user' property
      type HasUserProp = 'user' extends keyof MutateType ? true : false;
      expectTypeOf<HasUserProp>().toEqualTypeOf<false>();
    });
  });

  describe('both patterns together', () => {
    test('can use both modern and legacy patterns in same mutator when enableLegacyMutators: true', async () => {
      const schemaWithBoth = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
              email: string(),
            })
            .primaryKey('id'),
        ],
        enableLegacyMutators: true,
      });

      const crudWithBoth = createCRUDBuilder(schemaWithBoth);

      const mutators = defineMutatorsWithType<typeof schemaWithBoth>()({
        user: {
          createWithBothPatterns: defineMutatorWithType<
            typeof schemaWithBoth
          >()<{
            id: string;
            name: string;
            email: string;
          }>(async ({tx, args}) => {
            // Can use modern pattern
            await tx.mutate(crudWithBoth.user.insert(args));
            // Can also use legacy pattern for update
            await tx.mutate.user.update({
              id: args.id,
              name: args.name + ' (verified)',
            });
          }),
        },
      });

      const z = zeroForTest({
        schema: schemaWithBoth,
        mutators,
      });

      await z.mutate(
        mutators.user.createWithBothPatterns({
          id: '1',
          name: 'Mixed',
          email: 'mixed@test.com',
        }),
      ).client;

      await z.close();
    });
  });

  describe('CRUD request builder types', () => {
    test('createCRUDBuilder produces correct types', () => {
      const testSchema = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
          table('post')
            .columns({
              id: string(),
              title: string(),
              userId: string(),
            })
            .primaryKey('id'),
        ],
      });

      const testCrud = createCRUDBuilder(testSchema);

      // Verify insert request type
      const insertReq = testCrud.user.insert({id: '1', name: 'Test'});
      expectTypeOf(insertReq.schema).toEqualTypeOf<typeof testSchema>();
      expectTypeOf(insertReq.table).toEqualTypeOf<'user'>();
      expectTypeOf(insertReq.kind).toEqualTypeOf<'insert'>();
      // Args type has readonly modifiers from InsertValue
      expect(insertReq.args).toEqual({id: '1', name: 'Test'});

      // Verify update request type
      const updateReq = testCrud.user.update({id: '1', name: 'Updated'});
      expectTypeOf(updateReq.kind).toEqualTypeOf<'update'>();

      // Verify upsert request type
      const upsertReq = testCrud.user.upsert({id: '1', name: 'Upserted'});
      expectTypeOf(upsertReq.kind).toEqualTypeOf<'upsert'>();

      // Verify delete request type
      const deleteReq = testCrud.user.delete({id: '1'});
      expectTypeOf(deleteReq.kind).toEqualTypeOf<'delete'>();
      expectTypeOf(deleteReq.args).toEqualTypeOf<{id: string}>();

      // Verify different tables have their own types
      const postInsert = testCrud.post.insert({
        id: '1',
        title: 'Post',
        userId: 'u1',
      });
      expectTypeOf(postInsert.table).toEqualTypeOf<'post'>();
    });

    test('CRUD request builder validates table names at runtime', () => {
      const testSchema = createSchema({
        tables: [
          table('user')
            .columns({
              id: string(),
              name: string(),
            })
            .primaryKey('id'),
        ],
      });

      const testCrud = createCRUDBuilder(testSchema);

      // @ts-expect-error - nonexistent table
      expect(() => testCrud.nonexistent).toThrow(
        'Table nonexistent does not exist in schema',
      );
    });
  });
});
