import type postgres from 'postgres';
import {assert, describe, expect, test, vi} from 'vitest';
import type {MaybePromise} from '../../shared/src/types.ts';
import * as v from '../../shared/src/valita.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import {type PushBody} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {
  defineMutators,
  type AnyMutatorRegistry,
} from '../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../zql/src/mutate/mutator.ts';
import {PostgresJSConnection} from './adapters/postgresjs.ts';
import type {CustomMutatorDefs} from './custom.ts';
import type {
  Database,
  TransactionProviderHooks,
  TransactionProviderInput,
} from './process-mutations.ts';
import {PushProcessor} from './push-processor.ts';
import {ZQLDatabase} from './zql-database.ts';

const baseBody = {
  pushVersion: 1,
  requestID: 'test_request_id',
  timestamp: 1234567890,
  schemaVersion: 1,
  clientGroupID: 'test_client_group',
  mutations: [],
} satisfies PushBody;

const mockSchema = {
  tables: {},
  relationships: {},
} satisfies Schema;

const mockPgClient = {} as postgres.Sql;

// Mock mutators
const mockMutators = {} as CustomMutatorDefs<unknown>;

describe('PushProcessor', () => {
  test('should accept Record<string, string> as params', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const params: Record<string, string> = {
      schema: 'test_schema',
      appID: 'test_client_group',
    };

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, params, baseBody);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('should accept URLSearchParams as params', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const urlParams = new URLSearchParams();
    urlParams.append('schema', 'test_schema');
    urlParams.append('appID', 'test_client_group');

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, urlParams, baseBody);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('should accept Request as a param', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const req = new Request(
      'https://example.com?schema=test_schema&appID=test_client_group',
      {
        method: 'POST',
        body: JSON.stringify(baseBody),
      },
    );

    const spy = vi.spyOn(v, 'parse');
    await processor.process(mockMutators, req);

    expect(spy.mock.calls[1][0]).toMatchInlineSnapshot(`
      {
        "appID": "test_client_group",
        "schema": "test_schema",
      }
    `);
  });

  test('invalid params return parse error', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(mockPgClient), mockSchema),
    );

    const invalidParams: Record<string, string> = {
      // Missing schema and appID
    };

    const result = await processor.process(
      mockMutators,
      invalidParams,
      baseBody,
    );

    assert('kind' in result, 'expected push failed response');

    expect(result).toMatchObject({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.Parse,
      message: expect.stringContaining('Missing property schema'),
    });
  });
});

type FakeTransaction = {
  location: 'server';
  reason: 'authoritative';
  clientID: string;
  mutationID: number;
  mutate: Record<string, unknown>;
  query: Record<string, unknown>;
  run: ReturnType<typeof vi.fn>;
};

function createFakeTransaction(): FakeTransaction {
  return {
    location: 'server',
    reason: 'authoritative',
    clientID: 'test_client',
    mutationID: 1,
    mutate: {},
    query: {},
    run: vi.fn(),
  };
}

class FakeDatabase implements Database<FakeTransaction> {
  #lastMutationID = 0;
  readonly #tx: FakeTransaction;

  constructor(tx: FakeTransaction = createFakeTransaction()) {
    this.#tx = tx;
  }

  transaction<R>(
    callback: (
      tx: FakeTransaction,
      transactionHooks: TransactionProviderHooks,
    ) => MaybePromise<R>,
    _transactionInput?: TransactionProviderInput,
  ): Promise<R> {
    const hooks: TransactionProviderHooks = {
      updateClientMutationID: () => {
        this.#lastMutationID += 1;
        return Promise.resolve({lastMutationID: this.#lastMutationID});
      },
      writeMutationResult: () => Promise.resolve(),
      deleteMutationResults: () => Promise.resolve(),
    };

    return Promise.resolve(callback(this.#tx, hooks));
  }
}

const dispatchTestParams = {
  schema: 'test_schema',
  appID: 'test_app',
} as const;

const dispatchBodyBase = {
  pushVersion: 1,
  requestID: 'dispatch_request',
  timestamp: 987654321,
  schemaVersion: 1,
  clientGroupID: 'dispatch_group',
} as const;

function makeMutationBody(mutatorName: string): PushBody {
  return {
    ...dispatchBodyBase,
    mutations: [
      {
        type: 'custom',
        clientID: 'test_client',
        id: 1,
        name: mutatorName,
        timestamp: 0,
        args: ['payload'],
      },
    ],
  } satisfies PushBody;
}

describe('mutator resolution', () => {
  test('resolves legacy pipe separated keys', async () => {
    const processor = new PushProcessor<
      Schema,
      FakeDatabase,
      CustomMutatorDefs<FakeTransaction>
    >(new FakeDatabase());

    const mutator = vi.fn(async () => {});
    const mutators: CustomMutatorDefs<FakeTransaction> = {
      foo: {
        bar: mutator,
      },
    };

    const response = await processor.process(
      mutators,
      dispatchTestParams,
      makeMutationBody('foo|bar'),
    );

    expect(mutator).toHaveBeenCalledTimes(1);
    expect(mutator).toHaveBeenCalledWith(expect.any(Object), 'payload');
    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'test_client',
            id: 1,
          },
          result: {},
        },
      ],
    });
  });

  test('resolves dot separated keys used by new mutators', async () => {
    const processor = new PushProcessor<
      Schema,
      FakeDatabase,
      CustomMutatorDefs<FakeTransaction>
    >(new FakeDatabase());

    const mutator = vi.fn(async () => {});
    const mutators: CustomMutatorDefs<FakeTransaction> = {
      foo: {
        bar: mutator,
      },
    };

    const response = await processor.process(
      mutators,
      dispatchTestParams,
      makeMutationBody('foo.bar'),
    );

    expect(mutator).toHaveBeenCalledTimes(1);
    expect(mutator).toHaveBeenCalledWith(expect.any(Object), 'payload');
    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'test_client',
            id: 1,
          },
          result: {},
        },
      ],
    });
  });
});

describe('mutator calling conventions', () => {
  test('defineMutator implementations receive tx args and ctx', async () => {
    const fakeTx = createFakeTransaction();
    const context = {requestID: 'req-123'};
    const processor = new PushProcessor<
      Schema,
      FakeDatabase,
      // oxlint-disable-next-line no-explicit-any
      AnyMutatorRegistry | CustomMutatorDefs<any>,
      typeof context
    >(new FakeDatabase(fakeTx), context);

    const capture = vi.fn();
    const mutators = defineMutators({
      foo: {
        bar: defineMutator(capture),
      },
    });

    const response = await processor.process(
      mutators,
      dispatchTestParams,
      makeMutationBody('foo.bar'),
    );

    expect(capture).toHaveBeenCalledTimes(1);
    expect(capture).toHaveBeenCalledWith({
      tx: fakeTx,
      args: 'payload',
      ctx: context,
    });
    expect(response).toEqual({
      mutations: [
        {
          id: {
            clientID: 'test_client',
            id: 1,
          },
          result: {},
        },
      ],
    });
  });

  test('legacy custom mutators receive db transaction and args', async () => {
    const fakeTx = createFakeTransaction();
    const processor = new PushProcessor<
      Schema,
      FakeDatabase,
      CustomMutatorDefs<FakeTransaction>,
      {env: string}
    >(new FakeDatabase(fakeTx), {env: 'test'});

    const mutator = vi.fn(async () => {});
    const mutators: CustomMutatorDefs<FakeTransaction> = {
      foo: {
        bar: mutator,
      },
    };

    await processor.process(
      mutators,
      dispatchTestParams,
      makeMutationBody('foo.bar'),
    );

    expect(mutator).toHaveBeenCalledTimes(1);
    expect(mutator).toHaveBeenCalledWith(fakeTx, 'payload');
  });
});
