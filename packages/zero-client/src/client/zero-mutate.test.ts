import {describe, expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import type {BatchMutator, DBMutator} from './crud.ts';
import {zeroForTest} from './test-utils.ts';

describe('mutateBatch', () => {
  test('mutateBatch is undefined when enableLegacyMutators is false', () => {
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

    const z = zeroForTest({schema});

    // Type test: BatchMutator should be undefined when enableLegacyMutators is false
    type TestBatchMutator = BatchMutator<typeof schema>;
    expectTypeOf<TestBatchMutator>().toEqualTypeOf<undefined>();

    // Type test: mutateBatch should be undefined
    expectTypeOf(z.mutateBatch).toEqualTypeOf<undefined>();

    // Runtime test: mutateBatch should be undefined
    expect(z.mutateBatch).toBe(undefined);
  });

  test('mutateBatch is undefined when enableLegacyMutators is not set (default)', () => {
    const schema = createSchema({
      tables: [
        table('issues')
          .columns({
            id: string(),
            title: string(),
          })
          .primaryKey('id'),
      ],
      // enableLegacyMutators not specified - defaults to false
    });

    const z = zeroForTest({schema});

    // Type test: BatchMutator should be undefined when enableLegacyMutators defaults to false
    type TestBatchMutator = BatchMutator<typeof schema>;
    expectTypeOf<TestBatchMutator>().toEqualTypeOf<undefined>();

    // Type test: mutateBatch should be undefined
    expectTypeOf(z.mutateBatch).toEqualTypeOf<undefined>();

    // Runtime test: mutateBatch should be undefined
    expect(z.mutateBatch).toBe(undefined);
  });

  test('mutateBatch is callable when enableLegacyMutators is true', async () => {
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

    // Type test: BatchMutator should be a function when enableLegacyMutators is true
    type TestBatchMutator = BatchMutator<typeof schema>;
    expectTypeOf<TestBatchMutator>().toBeFunction();

    // Type test: mutateBatch should be callable
    expectTypeOf(z.mutateBatch).toBeFunction();

    // Runtime test: mutateBatch should be defined and work
    expect(z.mutateBatch).toBeDefined();
    expect(typeof z.mutateBatch).toBe('function');

    await z.mutateBatch(async m => {
      await m.issues.insert({id: '1', title: 'test'});
    });
  });
});

describe('mutate', () => {
  test('mutate has no CRUD methods when enableLegacyMutators is false', () => {
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

    const z = zeroForTest({schema});

    // Type test: DBMutator should be empty when enableLegacyMutators is false
    type TestDBMutator = DBMutator<typeof schema>;
    expectTypeOf<TestDBMutator>().toEqualTypeOf<{}>();

    // Type test: mutate should not have table CRUD methods
    // @ts-expect-error - issues table should not exist when legacy mutators disabled
    void z.mutate.issues;

    // Runtime test: mutate.issues should not exist
    expect('issues' in z.mutate).toBe(false);
  });

  test('mutate has no CRUD methods when enableLegacyMutators is not set (default)', () => {
    const schema = createSchema({
      tables: [
        table('issues')
          .columns({
            id: string(),
            title: string(),
          })
          .primaryKey('id'),
      ],
      // enableLegacyMutators not specified - defaults to false
    });

    const z = zeroForTest({schema});

    // Type test: DBMutator should be empty when enableLegacyMutators defaults to false
    type TestDBMutator = DBMutator<typeof schema>;
    expectTypeOf<TestDBMutator>().toEqualTypeOf<{}>();

    // Type test: mutate should not have table CRUD methods
    // @ts-expect-error - issues table should not exist when legacy mutators disabled (default)
    void z.mutate.issues;

    // Runtime test: mutate.issues should not exist
    expect('issues' in z.mutate).toBe(false);
  });

  test('mutate has CRUD methods when enableLegacyMutators is true', async () => {
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

    // Type test: DBMutator should have table mutators when enableLegacyMutators is true
    type TestDBMutator = DBMutator<typeof schema>;
    expectTypeOf<TestDBMutator>().toHaveProperty('issues');

    // Type test: mutate should have CRUD methods
    expectTypeOf(z.mutate.issues.insert).toBeFunction();
    expectTypeOf(z.mutate.issues.update).toBeFunction();
    expectTypeOf(z.mutate.issues.delete).toBeFunction();
    expectTypeOf(z.mutate.issues.upsert).toBeFunction();

    // Runtime test: mutate.issues should be defined and have CRUD methods
    expect(z.mutate.issues).toBeDefined();
    expect(typeof z.mutate.issues.insert).toBe('function');
    expect(typeof z.mutate.issues.update).toBe('function');
    expect(typeof z.mutate.issues.delete).toBe('function');
    expect(typeof z.mutate.issues.upsert).toBe('function');

    // Runtime test: CRUD methods should work
    await z.mutate.issues.insert({id: '1', title: 'test'});
  });
});

describe('both legacy flags undefined (default behavior)', () => {
  test('schema with neither enableLegacyMutators nor enableLegacyQueries set defaults to disabled', () => {
    const schema = createSchema({
      tables: [
        table('issues')
          .columns({
            id: string(),
            title: string(),
          })
          .primaryKey('id'),
      ],
      // Neither enableLegacyMutators nor enableLegacyQueries specified
    });

    const z = zeroForTest({schema});

    // Both should default to disabled behavior

    // mutateBatch should be undefined
    expect(z.mutateBatch).toBe(undefined);

    // CRUD mutators should not exist on z.mutate
    expect('issues' in z.mutate).toBe(false);

    // z.mutate should still be callable (for custom mutators)
    expect(typeof z.mutate).toBe('function');
  });
});
