import {describe, expect, test, vi} from 'vitest';
import {zeroData} from '../../../replicache/src/transactions.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../../zql/src/mutate/mutator.ts';
import {schema as testSchema} from '../../../zql/src/query/test/test-schemas.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {extendReplicacheMutators} from './make-replicache-mutators.ts';
import type {WriteTransaction} from './replicache-types.ts';

type Schema = typeof testSchema;

describe('extendReplicacheMutators', () => {
  test('processes MutatorDefinition at top level', () => {
    const lc = createSilentLogContext();
    const context = {userId: '123'};
    const mutateObject: Record<string, unknown> = {};

    const mockMutatorFn = vi.fn(async () => {});
    const mutator = defineMutator(mockMutatorFn);

    const mutators = defineMutators({
      createUser: mutator,
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('createUser');
    expect(typeof mutateObject['createUser']).toBe('function');
  });

  test('processes nested MutatorDefinition', () => {
    const lc = createSilentLogContext();
    const context = {userId: '456'};
    const mutateObject: Record<string, unknown> = {};

    const mockMutatorFn = vi.fn(async () => {});
    const updateMutator = defineMutator(mockMutatorFn);

    const mutators = defineMutators({
      users: {
        update: updateMutator,
      },
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('users.update');
    expect(typeof mutateObject['users.update']).toBe('function');
  });

  test('processes deeply nested MutatorDefinition', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const mockMutatorFn = vi.fn(async () => {});
    const deepMutator = defineMutator(mockMutatorFn);

    const mutators = defineMutators({
      level1: {
        level2: {
          level3: {
            action: deepMutator,
          },
        },
      },
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('level1.level2.level3.action');
  });

  test('processes legacy CustomMutatorImpl functions', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const legacyMutator = async (_tx: Transaction<Schema>, _args: unknown) => {
      // legacy mutator implementation
    };

    const mutators: CustomMutatorDefs = {
      legacy: {
        action: legacyMutator,
      },
    };

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    // Legacy mutators use '|' separator
    expect(mutateObject).toHaveProperty('legacy|action');
    expect(typeof mutateObject['legacy|action']).toBe('function');
  });

  test('processes mixed MutatorDefinition at different levels', () => {
    const lc = createSilentLogContext();
    const context = {userId: 'mixed'};
    const mutateObject: Record<string, unknown> = {};

    const createMutator = defineMutator(async () => {});
    const updateMutator = defineMutator(async () => {});

    const mutators = defineMutators({
      users: {
        create: createMutator,
      },
      updateUser: updateMutator,
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('users.create');
    expect(mutateObject).toHaveProperty('updateUser');
  });

  test('created mutator calls the underlying MutatorDefinition with correct args', async () => {
    const lc = createSilentLogContext();
    const testContext = {userId: '789', role: 'admin'};
    const context = testContext;
    const mutateObject: Record<string, unknown> = {};

    const mockMutatorFn = vi.fn(({args, ctx, tx}) => {
      expect(args).toEqual({id: '1', title: 'Test'});
      expect(ctx).toBe(testContext);
      expect(tx).toBeDefined();
      expect(tx).toHaveProperty('mutate');
      expect(tx).toHaveProperty('query');
      expect(tx).toHaveProperty('clientID');
      expect(tx).toHaveProperty('mutationID');
      return Promise.resolve();
    });

    const mutator = defineMutator(mockMutatorFn);

    const mutators = defineMutators({
      updateIssue: mutator,
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    const replicacheMutator = mutateObject['updateIssue'] as (
      repTx: WriteTransaction,
      args: unknown,
    ) => Promise<void>;

    const mockRepTx = {
      clientID: 'client-1',
      set: vi.fn(),
      get: vi.fn(),
      del: vi.fn(),
      scan: vi.fn(),
      mutationID: 42,
      reason: 'initial',
      location: 'local',
      [zeroData]: {
        ivmSources: {},
        token: undefined,
        context: testContext,
      },
    } as unknown as WriteTransaction;

    await replicacheMutator(mockRepTx, {id: '1', title: 'Test'});

    expect(mockMutatorFn).toHaveBeenCalledTimes(1);
    const callArgs = mockMutatorFn.mock.calls[0][0];
    expect(callArgs.args).toEqual({id: '1', title: 'Test'});
    expect(callArgs.ctx).toBe(testContext);
    expect(callArgs.tx).toBeDefined();
    expect(callArgs.tx.clientID).toBe('client-1');
    expect(callArgs.tx.mutationID).toBe(42);
  });

  test('handles multiple mutators at same level', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const createMutator = defineMutator(async () => {});
    const updateMutator = defineMutator(async () => {});
    const deleteMutator = defineMutator(async () => {});

    const mutators = defineMutators({
      users: {
        create: createMutator,
        update: updateMutator,
        delete: deleteMutator,
      },
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('users.create');
    expect(mutateObject).toHaveProperty('users.update');
    expect(mutateObject).toHaveProperty('users.delete');
  });

  test('handles empty mutators object', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const mutators = {};

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(Object.keys(mutateObject)).toHaveLength(0);
  });

  test('ignores non-object and non-function entries', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const mutators = {
      'valid': async (
        _tx: Transaction<Schema>,
        _args: unknown,
        _ctx: unknown,
      ) => {},
      'invalid': null,
      'text': 'noop',
      '~': 'phantom',
    } as unknown as CustomMutatorDefs;

    expect(() =>
      extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject),
    ).not.toThrow();
    expect(Object.keys(mutateObject)).toEqual(['valid']);
  });

  test('context is shared across mutators', async () => {
    const lc = createSilentLogContext();
    const sharedContext = {counter: 0};
    const context = sharedContext;
    const mutateObject: Record<string, unknown> = {};

    const incrementMutator = defineMutator<
      undefined,
      Schema,
      {counter: number}
    >(({ctx}: {ctx: {counter: number}}) => {
      ctx.counter++;
      return Promise.resolve();
    });

    const getMutator = defineMutator<undefined, Schema, {counter: number}>(
      ({ctx}: {ctx: {counter: number}}) => {
        // Read the counter value
        void ctx.counter;
        return Promise.resolve();
      },
    );

    const mutators = defineMutators({
      increment: incrementMutator,
      get: getMutator,
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    const mockRepTx = {
      clientID: 'client-1',
      set: vi.fn(),
      mutationID: 1,
      [zeroData]: {
        ivmSources: {},
        token: undefined,
        context: sharedContext,
      },
    } as unknown as WriteTransaction;

    const increment = mutateObject['increment'] as (
      tx: WriteTransaction,
      args: unknown,
    ) => Promise<void>;

    await increment(mockRepTx, undefined);
    await increment(mockRepTx, undefined);

    expect(sharedContext.counter).toBe(2);
  });

  test('processes deeply nested legacy CustomMutatorImpl functions', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const legacyMutator = async (
      _tx: Transaction<Schema>,
      _args: unknown,
    ) => {};

    const mutators: CustomMutatorDefs = {
      api: {
        v1: {
          legacy: legacyMutator,
        },
      },
    };

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('api|v1|legacy');
  });

  test('mutator receives transaction wrapper', async () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    let receivedTx: Transaction<Schema> | undefined;

    const mutator = defineMutator<undefined, Schema>(({tx}) => {
      receivedTx = tx;
      return Promise.resolve();
    });

    const mutators = defineMutators({
      test: mutator,
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    const mockRepTx = {
      clientID: 'client-1',
      set: vi.fn(),
      get: vi.fn(),
      del: vi.fn(),
      scan: vi.fn(),
      mutationID: 1,
      reason: 'test',
      location: 'local',
      [zeroData]: {
        ivmSources: {},
        token: undefined,
        context: {},
      },
    } as unknown as WriteTransaction;

    const replicacheMutator = mutateObject['test'] as (
      tx: WriteTransaction,
      args: unknown,
    ) => Promise<void>;

    await replicacheMutator(mockRepTx, undefined);

    expect(receivedTx).toBeDefined();
    expect(receivedTx).toHaveProperty('mutate');
    expect(receivedTx).toHaveProperty('query');
  });

  test('namespace prefix is correctly maintained during recursion', () => {
    const lc = createSilentLogContext();
    const context = {};
    const mutateObject: Record<string, unknown> = {};

    const m1 = defineMutator(async () => {});
    const m2 = defineMutator(async () => {});
    const m3 = defineMutator(async () => {});
    const m4 = defineMutator(async () => {});

    const mutators = defineMutators({
      a: {
        m1,
        b: {
          m2,
          c: {
            m3,
          },
        },
      },
      x: {
        y: {
          m4,
        },
      },
    });

    extendReplicacheMutators(lc, context, mutators, testSchema, mutateObject);

    expect(mutateObject).toHaveProperty('a.m1');
    expect(mutateObject).toHaveProperty('a.b.m2');
    expect(mutateObject).toHaveProperty('a.b.c.m3');
    expect(mutateObject).toHaveProperty('x.y.m4');
  });
});
