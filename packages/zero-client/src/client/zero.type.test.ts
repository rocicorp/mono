import {expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  defineMutatorWithType,
  type MutateRequest,
} from '../../../zql/src/mutate/mutator.ts';
import type {DBMutator} from './crud.ts';
import type {MutatorResult} from './custom.ts';
import {zeroForTest} from './test-utils.ts';

import type {ImmutableArray} from '../../../shared/src/immutable.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';

test('run', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });

  const mutators = defineMutatorsWithType<typeof schema>()({
    insertIssue: defineMutatorWithType<typeof schema>()(async ({tx}) => {
      expect('issues' in tx.mutate).toBe(true);
      await tx.mutate.issues.insert({id: 'a', value: 1});

      expect('noSuchTable' in tx.mutate).toBe(false);

      // oxlint-disable-next-line no-constant-condition
      if (false) {
        // @ts-expect-error - noSuchTable does not exist
        await tx.mutate.noSuchTable.insert({id: 'x'});
      }
    }),
  } as const);
  const z = zeroForTest({
    schema,
    mutators,
  });

  const builder = createBuilder(schema);
  await z.mutate(mutators.insertIssue()).client;

  const x = await z.run(builder.issues);
  expectTypeOf(x).toEqualTypeOf<
    {
      readonly id: string;
      readonly value: number;
    }[]
  >();
  expect(x).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
});

test('materialize', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          value: number(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });
  const mutators = defineMutatorsWithType<typeof schema>()({
    insertIssue: defineMutatorWithType<typeof schema>()(({tx}) =>
      tx.mutate.issues.insert({id: 'a', value: 1}),
    ),
  });
  const z = zeroForTest({
    schema,
    mutators,
  });
  const builder = createBuilder(schema);
  await z.mutate(mutators.insertIssue()).client;

  const m = z.materialize(builder.issues);
  expectTypeOf(m.data).toEqualTypeOf<
    {
      readonly id: string;
      readonly value: number;
    }[]
  >();

  let gotData: unknown;
  m.addListener(d => {
    gotData = d;
    expectTypeOf(d).toEqualTypeOf<
      ImmutableArray<{
        readonly id: string;
        readonly value: number;
      }>
    >();
  });

  expect(gotData).toEqual([{id: 'a', value: 1, [refCountSymbol]: 1}]);
});

test('legacy mutators enabled - CRUD methods available in types', () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });

  const z = zeroForTest({schema});

  // Verify CRUD methods exist in types
  expectTypeOf(z.mutate.issues.insert).toBeFunction();
  expectTypeOf(z.mutate.issues.update).toBeFunction();
  expectTypeOf(z.mutate.issues.delete).toBeFunction();
  expectTypeOf(z.mutate.issues.upsert).toBeFunction();

  // Verify return types are Promise<void>
  expectTypeOf(
    z.mutate.issues.insert({id: 'test', title: 'test'}),
  ).toEqualTypeOf<Promise<void>>();
  expectTypeOf(z.mutate.issues.update({id: 'test'})).toEqualTypeOf<
    Promise<void>
  >();
  expectTypeOf(z.mutate.issues.delete({id: 'test'})).toEqualTypeOf<
    Promise<void>
  >();
});

test('legacy mutators disabled - table mutators do not exist', () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: false,
  });

  // Verify runtime value
  expect(schema.enableLegacyMutators).toBe(false);

  const z = zeroForTest({schema});

  // Type test: DBMutator should be empty when enableLegacyMutators is false
  type TestDBMutator = DBMutator<typeof schema>;
  expectTypeOf<TestDBMutator>().toEqualTypeOf<{}>();

  // Verify table mutators do not exist when legacy mutators disabled
  // mutate is still callable with MutateRequest even when legacy mutators disabled
  expectTypeOf(z.mutate).toEqualTypeOf<
    {} & ((
      // oxlint-disable-next-line no-explicit-any
      mr: MutateRequest<any, typeof schema, unknown, any>,
    ) => MutatorResult)
  >();

  // @ts-expect-error - issues table should not exist when legacy mutators disabled
  void z.mutate.issues;
});

test('legacy mutators undefined - defaults to disabled', () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    // enableLegacyMutators not specified - should default to false
  });

  const z = zeroForTest({schema});

  // Should not have CRUD methods by default
  // @ts-expect-error - issues table should not exist when legacy mutators disabled (default)
  void z.mutate.issues;

  expectTypeOf(z.mutate).toBeFunction();
});

test('CRUD and custom mutators work together with enableLegacyMutators: true', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
          status: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: true,
  });

  const zql = createBuilder(schema);

  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        // Custom mutator that uses CRUD internally
        closeIssue: async (
          tx: Transaction<typeof schema>,
          {id}: {id: string},
        ) => {
          await tx.mutate.issues.update({id, status: 'closed'});
        },
        // Another custom mutator
        createAndClose: async (
          tx: any, // oxlint-disable-line @typescript-eslint/no-explicit-any
          {id, title}: {id: string; title: string},
        ) => {
          await tx.mutate.issues.insert({id, title, status: 'open'});
          await tx.mutate.issues.update({id, status: 'closed'});
        },
      },
    },
  });

  // Type-level: Verify both CRUD and custom mutators are available
  expectTypeOf(z.mutate.issues.insert).toBeFunction();
  expectTypeOf(z.mutate.issues.update).toBeFunction();
  expectTypeOf(z.mutate.issues.delete).toBeFunction();
  expectTypeOf(z.mutate.issue.closeIssue).toBeFunction();
  expectTypeOf(z.mutate.issue.createAndClose).toBeFunction();

  // Runtime: Verify both work
  await z.mutate.issues.insert({id: '1', title: 'Test Issue', status: 'open'});
  const result = z.mutate.issue.closeIssue({id: '1'});
  await result.client;

  const issues = await z.run(zql.issues.where('id', '1').one());
  expect(issues?.status).toBe('closed');
});

test('Custom mutators still work when enableLegacyMutators: false', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
          status: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: false,
  });

  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        // Custom mutator that doesn't rely on CRUD
        customCreate: (
          _tx: Transaction<typeof schema>,
          {id, title}: {id: string; title: string},
        ) => {
          // In real usage, this would use server-side implementation
          void id;
          void title;
          return Promise.resolve();
        },
      },
    },
  });

  // Type-level: Verify table mutators are NOT available but custom mutators ARE
  // @ts-expect-error - issues table should not exist when legacy mutators disabled
  z.mutate.issues;
  expectTypeOf(z.mutate.issue.customCreate).toBeFunction();

  // Runtime: Verify custom mutator can be called
  await z.mutate.issue.customCreate({id: '1', title: 'Test'});
});

test('tx.mutate has no table properties when enableLegacyMutators: false', async () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    enableLegacyMutators: false,
  });

  const mutators = defineMutatorsWithType<typeof schema>()({
    testMutator: defineMutatorWithType<typeof schema>()(({tx}) => {
      // Runtime test: 'issues' should NOT be in tx.mutate when legacy mutators disabled
      expect('issues' in tx.mutate).toBe(false);

      // Also verify a non-existent table is also false
      expect('noSuchTable' in tx.mutate).toBe(false);

      return Promise.resolve();
    }),
  } as const);

  const z = zeroForTest({
    schema,
    mutators,
  });

  await z.mutate(mutators.testMutator()).client;
});
