import {expect, expectTypeOf, test} from 'vitest';
import {zeroForTest} from './test-utils.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {DBMutator} from './crud.ts';

import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/named.ts';
import type {ImmutableArray} from '../../../shared/src/immutable.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';

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
  });
  const z = zeroForTest({
    server: null,
    schema,
  });
  const builder = createBuilder(schema);
  await z.mutate.issues.insert({id: 'a', value: 1});

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
  });
  const z = zeroForTest({
    server: null,
    schema,
  });
  const builder = createBuilder(schema);
  await z.mutate.issues.insert({id: 'a', value: 1});

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
  expectTypeOf<TestDBMutator>().toEqualTypeOf<{}>(); // eslint-disable-line @typescript-eslint/ban-types

  // Verify table mutators do not exist when legacy mutators disabled
  expectTypeOf(z.mutate).toEqualTypeOf<{}>(); // eslint-disable-line @typescript-eslint/ban-types

  // @ts-expect-error - issues table should not exist when legacy mutators disabled
  z.mutate.issues;
});

test('legacy mutators undefined - defaults to enabled', () => {
  const schema = createSchema({
    tables: [
      table('issues')
        .columns({
          id: string(),
          title: string(),
        })
        .primaryKey('id'),
    ],
    // enableLegacyMutators not specified - should default to true
  });

  const z = zeroForTest({schema});

  // Should have CRUD methods by default
  expectTypeOf(z.mutate.issues.insert).toBeFunction();
  expectTypeOf(z.mutate.issues.update).toBeFunction();
  expectTypeOf(z.mutate.issues.delete).toBeFunction();
  expectTypeOf(z.mutate.issues.upsert).toBeFunction();
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

  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        // Custom mutator that uses CRUD internally
        closeIssue: async (
          tx: Transaction<typeof schema>,
          {id}: {id: string},
        ) => {
          // eslint-disable-line @typescript-eslint/no-explicit-any
          await tx.mutate.issues.update({id, status: 'closed'});
        },
        // Another custom mutator
        createAndClose: async (
          tx: any, // eslint-disable-line @typescript-eslint/no-explicit-any
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
  await z.mutate.issue.closeIssue({id: '1'});

  const issues = await z.query.issues.where('id', '1').one();
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
          // eslint-disable-line @typescript-eslint/no-explicit-any
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
