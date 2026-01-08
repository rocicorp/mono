import type {StandardSchemaV1} from '@standard-schema/spec';
import type {MockInstance} from 'vitest';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {promiseUndefined} from '../../shared/src/resolved-promises.ts';
import type {MaybePromise} from '../../shared/src/types.ts';
import {ApplicationError} from '../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import {
  type CleanupResultsArg,
  CRUD_MUTATION_NAME,
  type MutationResponse,
} from '../../zero-protocol/src/push.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import {
  defineMutatorsWithType,
  mustGetMutator,
} from '../../zql/src/mutate/mutator-registry.ts';
import {
  defineMutatorWithType,
  type AnyMutator,
} from '../../zql/src/mutate/mutator.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {
  getMutation,
  handleMutateRequest,
  type Database,
  type TransactionProviderHooks,
} from './process-mutations.ts';

const baseQuery = {
  schema: 'test_schema',
  appID: 'test_app',
};

const TEST_TIMESTAMP = 1743127752952;

type CustomMutationShape = {
  readonly type: 'custom';
  readonly id: number;
  readonly clientID: string;
  readonly name: string;
  readonly args: readonly ReadonlyJSONValue[];
  readonly timestamp: number;
};

type CrudMutationShape = {
  readonly type: 'crud';
  readonly id: number;
  readonly clientID: string;
  readonly name: string;
  readonly args: readonly [{readonly ops: readonly ReadonlyJSONValue[]}];
  readonly timestamp: number;
};

type MutationShape = CustomMutationShape | CrudMutationShape;

type TransactionInput = Parameters<Database<undefined>['transaction']>[1];

type DeleteMutationResultsCall = CleanupResultsArg;

type TrackingDatabaseOptions = {
  readonly lastMutationIDProvider?: (params: {
    readonly mutationID: number | undefined;
    readonly transactionCount: number;
    readonly transactionInput?: TransactionInput;
  }) => number | bigint;
  readonly transactionErrorProvider?: (params: {
    readonly mutationID: number | undefined;
    readonly transactionCount: number;
    readonly transactionInput?: TransactionInput;
  }) => Error | undefined;
  readonly postTransactionErrorProvider?: (params: {
    readonly mutationID: number | undefined;
    readonly transactionCount: number;
    readonly transactionInput?: TransactionInput;
  }) => Error | undefined;
  readonly deleteMutationResultsErrorProvider?: () => Error | undefined;
};

function createTrackingDatabase(options: TrackingDatabaseOptions = {}): {
  readonly db: Database<undefined>;
  readonly recordedLMIDs: Array<number | bigint>;
  readonly recordedResults: MutationResponse[];
  readonly recordedDeletions: DeleteMutationResultsCall[];
} {
  const recordedLMIDs: Array<number | bigint> = [];
  const recordedResults: MutationResponse[] = [];
  const recordedDeletions: DeleteMutationResultsCall[] = [];
  let transactionCount = 0;

  return {
    db: {
      transaction<R>(
        callback: (
          tx: undefined,
          transactionHooks: TransactionProviderHooks,
        ) => MaybePromise<R>,
        transactionInput?: TransactionInput,
      ): Promise<R> {
        transactionCount++;
        const mutationID = transactionInput?.mutationID;
        const transactionError = options.transactionErrorProvider?.({
          mutationID,
          transactionCount,
          transactionInput,
        });
        if (transactionError) {
          return Promise.reject(transactionError);
        }
        // Track whether updateClientMutationID was called in this transaction
        let lmidWasUpdated = false;
        const resultPromise = Promise.resolve(
          callback(undefined, {
            updateClientMutationID: () => {
              const mutationIDForUpdate = transactionInput?.mutationID ?? 0;

              const customLastMutationID = options.lastMutationIDProvider?.({
                mutationID: mutationIDForUpdate,
                transactionCount,
                transactionInput,
              });
              const lastMutationID =
                customLastMutationID !== undefined
                  ? customLastMutationID
                  : BigInt(mutationIDForUpdate);
              lmidWasUpdated = true;
              return Promise.resolve({lastMutationID});
            },
            writeMutationResult: result => {
              recordedResults.push(result);
              return Promise.resolve();
            },
            deleteMutationResults: args => {
              const error = options.deleteMutationResultsErrorProvider?.();
              if (error) {
                return Promise.reject(error);
              }
              recordedDeletions.push(args);
              return Promise.resolve();
            },
          }),
        );
        return resultPromise.then(result => {
          const postTransactionError = options.postTransactionErrorProvider?.({
            mutationID,
            transactionCount,
            transactionInput,
          });
          if (postTransactionError) {
            throw postTransactionError;
          }

          // Only record LMID if updateClientMutationID was called (simulates tx commit)
          if (lmidWasUpdated) {
            recordedLMIDs.push(transactionInput?.mutationID ?? 0);
          }

          return result;
        });
      },
    },
    recordedLMIDs,
    recordedResults,
    recordedDeletions,
  };
}

function makeCustomMutation(
  overrides: Partial<CustomMutationShape> = {},
): CustomMutationShape {
  return {
    type: 'custom',
    id: overrides.id ?? 1,
    clientID: overrides.clientID ?? 'cid',
    name: overrides.name ?? 'doThing',
    args: overrides.args ?? [{}],
    timestamp: overrides.timestamp ?? TEST_TIMESTAMP,
  };
}

function makeCrudMutation(
  overrides: Partial<CrudMutationShape> = {},
): CrudMutationShape {
  return {
    type: 'crud',
    id: overrides.id ?? 1,
    clientID: overrides.clientID ?? 'cid',
    name: overrides.name ?? CRUD_MUTATION_NAME,
    args: overrides.args ?? [{ops: []}],
    timestamp: overrides.timestamp ?? TEST_TIMESTAMP,
  };
}

function makePushBody(mutations: readonly MutationShape[]): ReadonlyJSONValue {
  return {
    clientGroupID: 'cg',
    mutations,
    pushVersion: 1,
    schemaVersion: 1,
    timestamp: TEST_TIMESTAMP,
    requestID: 'req',
  } as const;
}

describe('handleMutateRequest', () => {
  let consoleErrorSpy: MockInstance<typeof console.error>;
  let consoleWarnSpy: MockInstance<typeof console.warn>;

  beforeEach(() => {
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
    consoleWarnSpy.mockRestore();
  });

  test('pre-transaction errors advance LMID and persist mutation result', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    await handleMutateRequest(
      trackingDb,
      () => {
        throw new Error('never got to db tx');
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
    );

    expect(recordedLMIDs).toEqual([1, 2]);
    expect(recordedResults).toEqual([
      {
        id: {clientID: 'cid', id: 1},
        result: {
          error: 'app',
          message: 'never got to db tx',
        },
      },
      {
        id: {clientID: 'cid', id: 2},
        result: {
          error: 'app',
          message: 'never got to db tx',
        },
      },
    ]);
  });

  test('pre-transaction application errors are persisted with details', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const response = await handleMutateRequest(
      trackingDb,
      () => {
        throw new ApplicationError('failed before transact', {
          details: {phase: 'pre'},
        });
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1})]),
    );

    expect(recordedLMIDs).toEqual([1]);
    expect(recordedResults).toEqual([
      {
        id: {clientID: 'cid', id: 1},
        result: {
          error: 'app',
          message: 'failed before transact',
          details: {phase: 'pre'},
        },
      },
    ]);
    expect(response).toEqual({mutations: recordedResults});
  });

  test('post-commit application errors are logged but not persisted', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    await handleMutateRequest(
      trackingDb,
      async (transact, _mutation) => {
        await transact((_tx, _name, _args) => Promise.resolve());
        throw new Error('post-processing failed');
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
    );

    // Post-commit errors: LMID is always updated
    expect(recordedLMIDs).toEqual([1, 2]);
    // writeMutationResult is not called because they were successful mutations
    expect(recordedResults).toEqual([]);

    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'PushProcessor',
      expect.stringContaining('Post-commit mutation handler failed'),
      expect.stringContaining('"message":"post-processing failed"'),
    );
  });

  test('transaction errors are persisted as application errors and LMID is updated', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    await handleMutateRequest(
      trackingDb,
      (transact, mutation) =>
        transact(async (_tx, _name, _args) => {
          await promiseUndefined;
          if (mutation.id === 1) {
            throw new Error('mutator exploded');
          }
        }),
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
    );

    // Transaction errors: LMID is updated for all mutations
    expect(recordedLMIDs).toEqual([1, 2]);
    // writeMutationResult is only called for the error
    expect(recordedResults).toEqual([
      {
        id: {clientID: 'cid', id: 1},
        result: {
          error: 'app',
          message: expect.stringContaining('mutator exploded'),
        },
      },
    ]);
  });

  test('multiple failed mutations are persisted with details and LMID is updated', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    await handleMutateRequest(
      trackingDb,
      (transact, mutation) =>
        transact(async (_tx, _name, _args) => {
          await promiseUndefined;
          if (mutation.id === 1) {
            throw new Error('first mutation failed');
          }
          if (mutation.id === 3) {
            throw new ApplicationError('third mutation failed', {
              details: {code: 'CUSTOM_ERROR'},
            });
          }
          // mutations 2 and 4 succeed
        }),
      baseQuery,
      makePushBody([
        makeCustomMutation({id: 1, name: 'mutation1'}),
        makeCustomMutation({id: 2, name: 'mutation2'}),
        makeCustomMutation({id: 3, name: 'mutation3'}),
        makeCustomMutation({id: 4, name: 'mutation4'}),
      ]),
    );

    // LMID is updated only once for each mutation
    expect(recordedLMIDs).toEqual([1, 2, 3, 4]);
    // writeMutationResult is only called for failed mutations
    expect(recordedResults).toEqual([
      {
        id: {clientID: 'cid', id: 1},
        result: {
          error: 'app',
          message: expect.stringContaining('first mutation failed'),
        },
      },
      {
        id: {clientID: 'cid', id: 3},
        result: {
          error: 'app',
          message: 'third mutation failed',
          details: {code: 'CUSTOM_ERROR'},
        },
      },
    ]);
  });

  test('returns database error when the transaction fails to open', async () => {
    const {
      db: failingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      transactionErrorProvider: () => new Error('db unavailable'),
    });

    const response = await handleMutateRequest(
      failingDb,
      (transact, mutation) =>
        transact(async (_tx, _name, _args) => {
          await promiseUndefined;
          void mutation;
        }),
      baseQuery,
      makePushBody([makeCustomMutation()]),
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining('db unavailable'),
      mutationIDs: [{id: 1, clientID: 'cid'}],
    });

    // LMID is not persisted
    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);
  });

  test('returns database error when a later transaction fails to open', async () => {
    const {
      db: flakyDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      transactionErrorProvider: ({mutationID}) =>
        mutationID === 2 ? new Error('db unavailable') : undefined,
    });
    const body = makePushBody([
      makeCustomMutation({id: 1}),
      makeCustomMutation({id: 2}),
    ]);

    const response = await handleMutateRequest(
      flakyDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      body,
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining('db unavailable'),
      mutationIDs: [{id: 2, clientID: 'cid'}],
    });

    // The first mutation's LMID is persisted
    expect(recordedLMIDs).toEqual([1]);
    expect(recordedResults).toEqual([]);
  });

  test('returns parse error when the push body validation fails', async () => {
    let callbackInvoked = false;
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const response = await handleMutateRequest(
      trackingDb,
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      baseQuery,
      'invalid body',
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Failed to parse push body'),
    });

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);

    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'PushProcessor',
      expect.stringContaining('Failed to parse push body'),
      expect.stringContaining('invalid body'),
    );
  });

  test('returns parse error when the query parameter validation fails', async () => {
    let callbackInvoked = false;
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const response = await handleMutateRequest(
      trackingDb,
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      {appID: baseQuery.appID},
      makePushBody([makeCustomMutation()]),
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Failed to parse push query parameters'),
    });

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);

    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'PushProcessor',
      expect.stringContaining('Failed to parse push query parameters'),
      expect.stringContaining('Missing property'),
    );
  });

  test('returns internal error when a non-custom mutation is provided', async () => {
    let callbackInvoked = false;
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const response = await handleMutateRequest(
      trackingDb,
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      baseQuery,
      makePushBody([makeCrudMutation()]),
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('Expected custom mutation'),
    });

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);
  });

  test('returns unsupported push version error', async () => {
    let callbackInvoked = false;
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const response = await handleMutateRequest(
      trackingDb,
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      baseQuery,
      {
        clientGroupID: 'cg',
        mutations: [makeCustomMutation()],
        pushVersion: 2,
        schemaVersion: 1,
        timestamp: TEST_TIMESTAMP,
        requestID: 'req',
      },
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.UnsupportedPushVersion,
      message: 'Unsupported push version: 2',
      mutationIDs: [{id: 1, clientID: 'cid'}],
    });

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);
  });

  test('returns out of order mutation error when mutation ID is too high', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      lastMutationIDProvider: () => BigInt(0),
    });

    const response = await handleMutateRequest(
      trackingDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 5})]),
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: expect.stringContaining('expected 0'),
      mutationIDs: [{id: 5, clientID: 'cid'}],
    });

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);
  });

  test('returns out of order mutation error and stops processing remaining mutations', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      lastMutationIDProvider: () => BigInt(1),
    });

    const response = await handleMutateRequest(
      trackingDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      makePushBody([
        makeCustomMutation({id: 1}),
        makeCustomMutation({id: 10}),
        makeCustomMutation({id: 11}),
      ]),
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: expect.stringContaining('expected 1'),
      // unprocessed mutations are returned
      mutationIDs: [
        {id: 10, clientID: 'cid'},
        {id: 11, clientID: 'cid'},
      ],
    });

    expect(recordedLMIDs).toEqual([1]);
    expect(recordedResults).toEqual([]);
  });

  test('returns already processed response when mutation ID is too low', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({lastMutationIDProvider: () => BigInt(5)});

    await handleMutateRequest(
      trackingDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 3})]),
    );

    expect(recordedLMIDs).toEqual([]);
    expect(recordedResults).toEqual([]);
  });

  test('continues processing after already processed mutation', async () => {
    // First mutation has lastMutationID = 5, second has lastMutationID = 2
    const {
      db: dbWithVaryingID,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      lastMutationIDProvider: ({transactionCount}) =>
        transactionCount === 1 ? 5 : 2,
    });

    await handleMutateRequest(
      dbWithVaryingID,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 3}), makeCustomMutation({id: 2})]),
    );

    // only the second mutation's LMID is persisted, since the first one is considered already processed
    expect(recordedLMIDs).toEqual([2]);
    // Neither already processed nor successful mutations call writeMutationResult
    expect(recordedResults).toEqual([]);
  });

  test('returns database error when transaction fails during execute phase', async () => {
    const {
      db: failingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase({
      postTransactionErrorProvider: ({mutationID}) =>
        mutationID === 2
          ? new Error('constraint violation during write')
          : undefined,
    });

    const response = await handleMutateRequest(
      failingDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining('constraint violation during write'),
      mutationIDs: [{id: 2, clientID: 'cid'}],
    });

    // only the first mutation's LMID is persisted
    expect(recordedLMIDs).toEqual([1]);
    expect(recordedResults).toEqual([]);
  });

  test('processes push requests when provided with a Request object', async () => {
    const {
      db: trackingDb,
      recordedLMIDs,
      recordedResults,
    } = createTrackingDatabase();

    const request = new Request(
      `https://example.com/push?schema=${baseQuery.schema}&appID=${baseQuery.appID}`,
      {
        method: 'POST',
        body: JSON.stringify(makePushBody([makeCustomMutation({id: 1})])),
        headers: {'content-type': 'application/json'},
      },
    );

    await handleMutateRequest(
      trackingDb,
      (transact, _mutation) =>
        transact((_tx, _name, _args) => promiseUndefined),
      request,
    );

    expect(recordedLMIDs).toEqual([1]);
    // Successful mutations don't call writeMutationResult
    expect(recordedResults).toEqual([]);
  });
});

describe('getMutation', () => {
  const rootMutator = async () => {};
  const childMutator = async () => {};
  const grandchildMutator = async () => {};
  const pipeMutator = async () => {};

  const mutators = {
    root: rootMutator,
    nested: {
      child: childMutator,
      deeper: {
        grandchild: grandchildMutator,
      },
    },
    pipe: {
      ns: {
        action: pipeMutator,
      },
    },
  } satisfies CustomMutatorDefs<unknown>;

  test('returns root-level mutators', () => {
    expect(getMutation(mutators, 'root')).toBe(rootMutator);
  });

  test('returns arbitrarily deep dot-delimited mutators', () => {
    expect(getMutation(mutators, 'nested.deeper.grandchild')).toBe(
      grandchildMutator,
    );
  });

  test('returns arbitrarily deep pipe-delimited mutators', () => {
    expect(getMutation(mutators, 'pipe|ns|action')).toBe(pipeMutator);
  });

  test('throws when a mutator does not exist', () => {
    expect(() => getMutation(mutators, 'nested.missing')).toThrow(
      'could not find mutator nested.missing',
    );
  });
});

const testSchema = createSchema({
  tables: [table('item').columns({id: string()}).primaryKey('id')],
});

type MutatorInvoker = (
  // oxlint-disable-next-line no-explicit-any
  mutators: any,
  name: string,
  tx: unknown,
  args: unknown,
) => Promise<void>;

const mutatorInvokers: Array<{name: string; invoke: MutatorInvoker}> = [
  {
    name: 'mustGetMutator',
    invoke: (mutators, name, tx, args) => {
      const mutator = mustGetMutator(mutators, name) as AnyMutator;
      // oxlint-disable-next-line no-explicit-any
      return mutator.fn({tx: tx as any, args, ctx: undefined});
    },
  },
  {
    name: 'getMutation (deprecated)',
    invoke: (mutators, name, tx, args) => {
      const mutator = getMutation(mutators, name);
      return mutator(tx, args, undefined);
    },
  },
];

describe.each(mutatorInvokers)(
  'handleMutateRequest with MutatorDefinition validator ($name)',
  ({invoke}) => {
    test('validator is invoked when processing mutation', async () => {
      const validator: StandardSchemaV1<{id: string}, {id: string}> = {
        '~standard': {
          version: 1,
          vendor: 'test',
          validate: vi.fn(input => ({value: input})),
        },
      };

      const mutatorFn = vi.fn(() => Promise.resolve());

      const testMutator = defineMutatorWithType<typeof testSchema>()(
        validator,
        mutatorFn,
      );

      const mutators = defineMutatorsWithType<typeof testSchema>()({
        item: {
          update: testMutator,
        },
      });

      const {db: trackingDb} = createTrackingDatabase();

      await handleMutateRequest(
        trackingDb,
        (transact, _mutation) =>
          transact((tx, name, args) => invoke(mutators, name, tx, args)),
        baseQuery,
        makePushBody([
          makeCustomMutation({name: 'item.update', args: [{id: 'test-123'}]}),
        ]),
      );

      expect(validator['~standard'].validate).toHaveBeenCalledWith({
        id: 'test-123',
      });
      expect(mutatorFn).toHaveBeenCalled();
    });

    test('validation failure returns application error response', async () => {
      const validator: StandardSchemaV1<{id: string}, {id: string}> = {
        '~standard': {
          version: 1,
          vendor: 'test',
          validate: () => ({
            issues: [{message: 'id must be a valid UUID'}],
          }),
        },
      };

      const mutatorFn = vi.fn(() => Promise.resolve());

      const testMutator = defineMutatorWithType<typeof testSchema>()(
        validator,
        mutatorFn,
      );

      const mutators = defineMutatorsWithType<typeof testSchema>()({
        item: {
          update: testMutator,
        },
      });

      const {
        db: trackingDb,
        recordedLMIDs,
        recordedResults,
      } = createTrackingDatabase();

      const response = await handleMutateRequest(
        trackingDb,
        (transact, _mutation) =>
          transact((tx, name, args) => invoke(mutators, name, tx, args)),
        baseQuery,
        makePushBody([
          makeCustomMutation({name: 'item.update', args: [{id: 'invalid'}]}),
        ]),
      );

      // The mutator function should NOT have been called due to validation failure
      expect(mutatorFn).not.toHaveBeenCalled();

      // LMID should still be advanced for the failed mutation
      expect(recordedLMIDs).toEqual([1]);

      // The response should contain the application error
      expect(response).toEqual({
        mutations: [
          {
            id: {clientID: 'cid', id: 1},
            result: {
              error: 'app',
              message: expect.stringContaining('id must be a valid UUID'),
            },
          },
        ],
      });

      // writeMutationResult should be called with the error
      expect(recordedResults).toEqual([
        {
          id: {clientID: 'cid', id: 1},
          result: {
            error: 'app',
            message: expect.stringContaining('id must be a valid UUID'),
          },
        },
      ]);
    });

    test('validator transforms args before passing to mutator', async () => {
      const capturedArgs: unknown[] = [];

      // Validator transforms string id to uppercase
      const uppercaseValidator: StandardSchemaV1<{id: string}, {id: string}> = {
        '~standard': {
          version: 1,
          vendor: 'test',
          validate: input => ({
            value: {id: ((input as {id: string}).id ?? '').toUpperCase()},
          }),
        },
      };

      const testMutator = defineMutatorWithType<typeof testSchema>()(
        uppercaseValidator,
        ({args}) => {
          capturedArgs.push(args);
          return Promise.resolve();
        },
      );

      const mutators = defineMutatorsWithType<typeof testSchema>()({
        item: {
          update: testMutator,
        },
      });

      const {db: trackingDb} = createTrackingDatabase();

      await handleMutateRequest(
        trackingDb,
        (transact, _mutation) =>
          transact((tx, name, args) => invoke(mutators, name, tx, args)),
        baseQuery,
        makePushBody([
          makeCustomMutation({name: 'item.update', args: [{id: 'lowercase'}]}),
        ]),
      );

      // The mutator should receive the transformed (uppercased) args
      expect(capturedArgs).toHaveLength(1);
      expect(capturedArgs[0]).toEqual({id: 'LOWERCASE'});
    });
  },
);
