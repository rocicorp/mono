import {describe, expect, test} from 'vitest';

import {CRUD_MUTATION_NAME} from '../../zero-protocol/src/push.ts';
import type {Database, TransactionProviderHooks} from './process-mutations.ts';
import {handleMutationRequest} from './process-mutations.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {MaybePromise} from '../../shared/src/types.ts';
import {promiseUndefined} from '../../shared/src/resolved-promises.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import {ApplicationError} from '../../zero-protocol/src/application-error.ts';

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

class SuccessDatabase implements Database<undefined> {
  transaction<R>(
    callback: (
      tx: undefined,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    transactionInput?: Parameters<Database<undefined>['transaction']>[1],
  ): Promise<R> {
    return Promise.resolve(
      callback(undefined, {
        updateClientMutationID() {
          return Promise.resolve({
            lastMutationID: BigInt(transactionInput?.mutationID ?? 0),
          });
        },
        writeMutationResult() {
          return Promise.resolve();
        },
      }),
    );
  }
}

class FailingDatabase implements Database<undefined> {
  readonly #message: string;

  constructor(message: string) {
    this.#message = message;
  }

  transaction(): Promise<never> {
    return Promise.reject(new Error(this.#message));
  }
}

class CustomError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CustomError';
  }
}

class DatabaseWithMutationID implements Database<undefined> {
  readonly #lastMutationID: number;

  constructor(lastMutationID: number) {
    this.#lastMutationID = lastMutationID;
  }

  transaction<R>(
    callback: (
      tx: undefined,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    _transactionInput?: Parameters<Database<undefined>['transaction']>[1],
  ): Promise<R> {
    return Promise.resolve(
      callback(undefined, {
        updateClientMutationID: () =>
          Promise.resolve({
            lastMutationID: this.#lastMutationID,
          }),
        writeMutationResult() {
          return Promise.resolve();
        },
      }),
    );
  }
}

class DatabaseFailingDuringExecute implements Database<undefined> {
  readonly #message: string;

  constructor(message: string) {
    this.#message = message;
  }

  async transaction<R>(
    callback: (
      tx: undefined,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    transactionInput?: Parameters<Database<undefined>['transaction']>[1],
  ): Promise<R> {
    // Transaction opens successfully, but fails after callback completes
    await Promise.resolve(
      callback(undefined, {
        updateClientMutationID: () =>
          Promise.resolve({
            lastMutationID: BigInt(transactionInput?.mutationID ?? 0),
          }),
        writeMutationResult: () => Promise.resolve(),
      }),
    );
    // Simulate failure during commit phase (after callback completes)
    throw new Error(this.#message);
  }
}

describe('handleMutationRequest', () => {
  test('wraps application errors thrown before transact', async () => {
    const response = await handleMutationRequest(
      () => {
        throw new Error('never got to db tx');
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
      'info',
    );

    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 1,
          },
          result: {
            error: 'app',
            message: 'never got to db tx',
          },
        },
        {
          id: {
            clientID: 'cid',
            id: 2,
          },
          result: {
            error: 'app',
            message: 'never got to db tx',
          },
        },
      ],
    });
  });

  test('keeps ApplicationError details', async () => {
    const response = await handleMutationRequest(
      () => {
        throw new ApplicationError('validation failed', {
          details: {code: 'INVALID_INPUT'},
        });
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
      'info',
    );

    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 1,
          },
          result: {
            error: 'app',
            message: 'validation failed',
            details: {code: 'INVALID_INPUT'},
          },
        },
        {
          id: {
            clientID: 'cid',
            id: 2,
          },
          result: {
            error: 'app',
            message: 'validation failed',
            details: {code: 'INVALID_INPUT'},
          },
        },
      ],
    });
  });

  test('wraps application errors thrown after the transaction completes', async () => {
    const db = new SuccessDatabase();

    const response = await handleMutationRequest(
      async (transact, mutation) => {
        await transact(db, () => Promise.resolve());
        if (mutation.id === 1) {
          throw new Error('post-processing failed');
        }
        throw new CustomError('another post-processing failed');
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
      'info',
    );

    // Verify both failed mutations are included in the response
    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 1,
          },
          result: {
            error: 'app',
            message: expect.stringContaining('post-processing failed'),
          },
        },
        {
          id: {
            clientID: 'cid',
            id: 2,
          },
          result: {
            error: 'app',
            message: expect.stringContaining('another post-processing failed'),
            details: {
              name: 'CustomError',
            },
          },
        },
      ],
    });
  });

  test('wraps application errors thrown inside the transaction and continues processing', async () => {
    const db = new SuccessDatabase();
    const body = makePushBody([
      makeCustomMutation({id: 1}),
      makeCustomMutation({id: 2}),
    ]);

    const response = await handleMutationRequest(
      (transact, mutation) =>
        transact(db, async () => {
          await promiseUndefined;
          if (mutation.id === 1) {
            throw new Error('mutator exploded');
          }
        }),
      baseQuery,
      body,
      'info',
    );

    // Verify failed mutation is included alongside successful one
    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 1,
          },
          result: {
            error: 'app',
            message: expect.stringContaining('mutator exploded'),
          },
        },
        {
          id: {
            clientID: 'cid',
            id: 2,
          },
          result: {},
        },
      ],
    });
  });

  test('includes all failed mutations in response with different error types', async () => {
    const db = new SuccessDatabase();
    const body = makePushBody([
      makeCustomMutation({id: 1, name: 'mutation1'}),
      makeCustomMutation({id: 2, name: 'mutation2'}),
      makeCustomMutation({id: 3, name: 'mutation3'}),
      makeCustomMutation({id: 4, name: 'mutation4'}),
    ]);

    const response = await handleMutationRequest(
      (transact, mutation) =>
        transact(db, async () => {
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
      body,
      'info',
    );

    // Verify all mutations (both failed and successful) are included
    expect(response).toEqual({
      mutations: [
        {
          id: {clientID: 'cid', id: 1},
          result: {
            error: 'app',
            message: expect.stringContaining('first mutation failed'),
          },
        },
        {
          id: {clientID: 'cid', id: 2},
          result: {},
        },
        {
          id: {clientID: 'cid', id: 3},
          result: {
            error: 'app',
            message: 'third mutation failed',
            details: {code: 'CUSTOM_ERROR'},
          },
        },
        {
          id: {clientID: 'cid', id: 4},
          result: {},
        },
      ],
    });
  });

  test('returns database error when the transaction fails to open', async () => {
    const failingDb = new FailingDatabase('db unavailable');

    const response = await handleMutationRequest(
      (transact, mutation) =>
        transact(failingDb, async () => {
          await promiseUndefined;
          void mutation;
        }),
      baseQuery,
      makePushBody([makeCustomMutation()]),
      'info',
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining('db unavailable'),
    });
  });

  test('returns database error when a later transaction fails to open', async () => {
    const failingDb = new FailingDatabase('db unavailable');
    const successDb = new SuccessDatabase();
    const body = makePushBody([
      makeCustomMutation({id: 1}),
      makeCustomMutation({id: 2}),
    ]);

    const response = await handleMutationRequest(
      (transact, mutation) => {
        if (mutation.id === 1) {
          return transact(successDb, () => promiseUndefined);
        }
        return transact(failingDb, () => promiseUndefined);
      },
      baseQuery,
      body,
      'info',
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining('db unavailable'),
    });
  });

  test('returns parse error when the push body validation fails', async () => {
    let callbackInvoked = false;

    const response = await handleMutationRequest(
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      baseQuery,
      'invalid body',
      'info',
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Failed to parse push body'),
    });
  });

  test('returns parse error when the query parameter validation fails', async () => {
    let callbackInvoked = false;

    const response = await handleMutationRequest(
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      {appID: baseQuery.appID},
      makePushBody([makeCustomMutation()]),
      'info',
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Failed to parse push query parameters'),
    });
  });

  test('returns internal error when a non-custom mutation is provided', async () => {
    let callbackInvoked = false;

    const response = await handleMutationRequest(
      () => {
        callbackInvoked = true;
        throw new Error('should not run');
      },
      baseQuery,
      makePushBody([makeCrudMutation()]),
      'info',
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Internal,
      message: expect.stringContaining('Expected custom mutation'),
    });
  });

  test('returns unsupported push version error', async () => {
    let callbackInvoked = false;

    const response = await handleMutationRequest(
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
      'info',
    );

    expect(callbackInvoked).toBe(false);
    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.UnsupportedPushVersion,
      message: 'Unsupported push version: 2',
      mutationIDs: [{id: 1, clientID: 'cid'}],
    });
  });

  test('returns out of order mutation error when mutation ID is too high', async () => {
    const db = new DatabaseWithMutationID(0);

    const response = await handleMutationRequest(
      (transact, _mutation) => transact(db, () => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 5})]),
      'info',
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: expect.stringContaining('expected 0'),
      mutationIDs: [{id: 5, clientID: 'cid'}],
    });
  });

  test('returns out of order mutation error and stops processing remaining mutations', async () => {
    const db = new DatabaseWithMutationID(1);

    const response = await handleMutationRequest(
      (transact, _mutation) => transact(db, () => promiseUndefined),
      baseQuery,
      makePushBody([
        makeCustomMutation({id: 1}),
        makeCustomMutation({id: 10}),
        makeCustomMutation({id: 11}),
      ]),
      'info',
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: expect.stringContaining('expected 1'),
      // Only unprocessed mutations are included
      mutationIDs: [
        {id: 10, clientID: 'cid'},
        {id: 11, clientID: 'cid'},
      ],
    });
  });

  test('returns already processed response when mutation ID is too low', async () => {
    const db = new DatabaseWithMutationID(5);

    const response = await handleMutationRequest(
      (transact, _mutation) => transact(db, () => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 3})]),
      'info',
    );

    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 3,
          },
          result: {
            error: 'alreadyProcessed',
            details: expect.stringContaining('with ID 3'),
          },
        },
      ],
    });
  });

  test('continues processing after already processed mutation', async () => {
    // First mutation has lastMutationID = 5, second has lastMutationID = 2
    let callCount = 0;
    const dbWithVaryingID = {
      transaction: <R>(
        callback: (
          tx: undefined,
          transactionHooks: TransactionProviderHooks,
        ) => MaybePromise<R>,
      ) => {
        callCount++;
        const lastMutationID = callCount === 1 ? 5 : 2;
        return Promise.resolve(
          callback(undefined, {
            updateClientMutationID: () => Promise.resolve({lastMutationID}),
            writeMutationResult: () => Promise.resolve(),
          }),
        );
      },
    } as Database<undefined>;

    const response = await handleMutationRequest(
      (transact, _mutation) =>
        transact(dbWithVaryingID, () => promiseUndefined),
      baseQuery,
      makePushBody([makeCustomMutation({id: 3}), makeCustomMutation({id: 2})]),
      'info',
    );

    expect(response).toEqual({
      mutations: [
        {
          id: {clientID: 'cid', id: 3},
          result: {
            error: 'alreadyProcessed',
            details: expect.stringContaining('with ID 3'),
          },
        },
        {
          id: {clientID: 'cid', id: 2},
          result: {},
        },
      ],
    });
  });

  test('returns database error when transaction fails during execute phase', async () => {
    const db = new SuccessDatabase();
    const failingDb = new DatabaseFailingDuringExecute(
      'constraint violation during write',
    );

    const response = await handleMutationRequest(
      (transact, mutation) => {
        if (mutation.id === 1) {
          return transact(db, () => promiseUndefined);
        }
        return transact(failingDb, () => promiseUndefined);
      },
      baseQuery,
      makePushBody([makeCustomMutation({id: 1}), makeCustomMutation({id: 2})]),
      'info',
    );

    expect(response).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Database,
      message: expect.stringContaining(
        'Database transaction failed after opening',
      ),
      mutationIDs: [{id: 2, clientID: 'cid'}],
    });
  });
});
