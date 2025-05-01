import {testDBs} from '../../zero-cache/src/test/db.ts';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {getClientsTableDefinition} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';

import {PushProcessor} from './push-processor.ts';
import {ZQLPostgresJSAdapter} from './zql-postgresjs-provider.ts';
import type {PushBody} from '../../zero-protocol/src/push.ts';
import {
  customMutatorKey,
  type ServerTransaction,
} from '../../zql/src/mutate/custom.ts';
import {ZQLDatabaseProvider} from './zql-provider.ts';
import type {CustomMutatorDefs} from './custom.ts';
import {consoleLogSink} from '@rocicorp/logger';
import {sleep} from '../../shared/src/sleep.ts';

let pg: PostgresDB;
const params = {
  schema: 'zero_0',
  appID: 'zero',
};
beforeEach(async () => {
  pg = await testDBs.create('zero-pg-web');
  await pg.unsafe(`
    CREATE SCHEMA IF NOT EXISTS zero_0;
    ${getClientsTableDefinition('zero_0')}
  `);
});

function makePush(
  mid: number,
  mutatorName: string = customMutatorKey('foo', 'bar'),
): PushBody {
  return {
    pushVersion: 1,
    clientGroupID: 'cgid',
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: 42,
    mutations: [
      {
        type: 'custom',
        clientID: 'cid',
        id: mid,
        name: mutatorName,
        timestamp: 42,
        args: [],
      },
    ],
  };
}

function makePushBatch(
  mutations: [mid: number, mutatorName: string][],
): PushBody {
  return {
    pushVersion: 1,
    clientGroupID: 'cgid',
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: 42,
    mutations: mutations.map(([mid, mutatorName]) => ({
      type: 'custom',
      clientID: 'cid',
      id: mid,
      name: mutatorName,
      timestamp: 42,
      args: [],
    })),
  };
}

const schema = {
  tables: {},
  relationships: {},
  version: 1,
};

const mutators = {
  foo: {
    bar: () => Promise.resolve(),
    baz: () => Promise.reject(new Error('application error')),
  },
} as const;

describe('out of order mutation', () => {
  test('first mutation is out of order', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );
    const result = await processor.process(mutators, params, makePush(15));

    expect(result).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 15,
          },
          result: {
            details: 'Client cid sent mutation ID 15 but expected 1',
            error: 'oooMutation',
          },
        },
      ],
    });

    await checkClientsTable(pg, undefined);
  });

  test('later mutations are out of order', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    expect(await processor.process(mutators, params, makePush(1))).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 1,
          },
          result: {},
        },
      ],
    });

    expect(await processor.process(mutators, params, makePush(3))).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 3,
          },
          result: {
            details: 'Client cid sent mutation ID 3 but expected 2',
            error: 'oooMutation',
          },
        },
      ],
    });

    await checkClientsTable(pg, 1);
  });
});

test('first mutation', async () => {
  const processor = new PushProcessor(
    new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
  );

  expect(await processor.process(mutators, params, makePush(1))).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {},
      },
    ],
  });

  await checkClientsTable(pg, 1);
});

test('previously seen mutation', async () => {
  const processor = new PushProcessor(
    new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
  );

  await processor.process(mutators, params, makePush(1));
  await processor.process(mutators, params, makePush(2));
  await processor.process(mutators, params, makePush(3));

  expect(await processor.process(mutators, params, makePush(2))).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 2,
        },
        result: {
          error: 'alreadyProcessed',
          details:
            'Ignoring mutation from cid with ID 2 as it was already processed. Expected: 4',
        },
      },
    ],
  });

  await checkClientsTable(pg, 3);
});

test('lmid still moves forward if the mutator implementation throws', async () => {
  const processor = new PushProcessor(
    new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
  );

  await processor.process(mutators, params, makePush(1));
  await processor.process(mutators, params, makePush(2));
  const result = await processor.process(
    mutators,
    params,
    makePush(3, customMutatorKey('foo', 'baz')),
  );
  expect(result).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 3,
        },
        result: {
          error: 'app',
          details: 'application error',
        },
      },
    ],
  });
  await checkClientsTable(pg, 3);
});

test('mutators with and without namespaces', async () => {
  const processor = new PushProcessor(
    new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
  );
  const mutators = {
    namespaced: {
      pass: () => Promise.resolve(),
      reject: () => Promise.reject(new Error('application error')),
    },
    topPass: () => Promise.resolve(),
    topReject: () => Promise.reject(new Error('application error')),
  };

  expect(
    await processor.process(
      mutators,
      params,
      makePush(1, customMutatorKey('namespaced', 'pass')),
    ),
  ).toMatchInlineSnapshot(`
    {
      "mutations": [
        {
          "id": {
            "clientID": "cid",
            "id": 1,
          },
          "result": {},
        },
      ],
    }
  `);
  expect(await processor.process(mutators, params, makePush(2, 'topPass')))
    .toMatchInlineSnapshot(`
          {
            "mutations": [
              {
                "id": {
                  "clientID": "cid",
                  "id": 2,
                },
                "result": {},
              },
            ],
          }
        `);

  expect(
    await processor.process(
      mutators,
      params,
      makePush(3, customMutatorKey('namespaced', 'reject')),
    ),
  ).toMatchInlineSnapshot(`
    {
      "mutations": [
        {
          "id": {
            "clientID": "cid",
            "id": 3,
          },
          "result": {
            "details": "application error",
            "error": "app",
          },
        },
      ],
    }
  `);
  expect(await processor.process(mutators, params, makePush(4, 'topReject')))
    .toMatchInlineSnapshot(`
          {
            "mutations": [
              {
                "id": {
                  "clientID": "cid",
                  "id": 4,
                },
                "result": {
                  "details": "application error",
                  "error": "app",
                },
              },
            ],
          }
        `);

  await checkClientsTable(pg, 4);
});

describe('post commit tasks', () => {
  let consoleErrorMock: ReturnType<typeof vi.spyOn>;
  let mutators: CustomMutatorDefs<ServerTransaction<typeof schema, unknown>>;
  let noErrorMock: ReturnType<typeof vi.fn>;
  let taskErrorMock: ReturnType<typeof vi.fn>;
  let mutationErrorMock: ReturnType<typeof vi.fn>;
  let asyncResolutionMock: ReturnType<typeof vi.fn>;

  let resolveAsyncResolution: (value: unknown) => void;

  const taskException = new Error('post mutation task error');

  beforeEach(() => {
    consoleErrorMock = vi
      .spyOn(consoleLogSink, 'log')
      .mockImplementation(() => undefined);
    noErrorMock = vi.fn().mockResolvedValue(undefined);
    taskErrorMock = vi.fn().mockRejectedValue(taskException);
    mutationErrorMock = vi.fn().mockResolvedValue(undefined);

    asyncResolutionMock = vi.fn().mockImplementation(
      () =>
        new Promise(resolve => {
          resolveAsyncResolution = resolve;
        }),
    );

    mutators = {
      postCommit: {
        noError: tx => {
          tx.after(noErrorMock);
          return Promise.resolve();
        },
        taskError: tx => {
          tx.after(taskErrorMock);
          return Promise.resolve();
        },
        mutationError: tx => {
          tx.after(mutationErrorMock);
          return Promise.reject(new Error('mutator error'));
        },
        asyncResolution: tx => {
          tx.after(asyncResolutionMock);
          return Promise.resolve();
        },
      },
    };
  });

  afterEach(() => {
    consoleErrorMock.mockReset();
  });

  test('tasks execute if mutator succeeds', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    await processor.process(
      mutators,
      params,
      makePushBatch([
        [1, customMutatorKey('postCommit', 'noError')],
        [2, customMutatorKey('postCommit', 'taskError')],
      ]),
    );

    expect(noErrorMock).toHaveBeenCalledTimes(1);
    expect(taskErrorMock).toHaveBeenCalledTimes(1);
  });

  test('tasks do not execute if mutator throws', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    await processor.process(
      mutators,
      params,
      makePushBatch([
        [1, customMutatorKey('postCommit', 'noError')],
        [2, customMutatorKey('postCommit', 'mutationError')],
      ]),
    );

    expect(noErrorMock).toHaveBeenCalledTimes(1);
    expect(mutationErrorMock).toHaveBeenCalledTimes(0);
  });

  test('tasks throws are sent to the logger', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    await processor.process(
      mutators,
      params,
      makePushBatch([
        [1, customMutatorKey('postCommit', 'noError')],
        [2, customMutatorKey('postCommit', 'taskError')],
      ]),
    );

    expect(noErrorMock).toHaveBeenCalledTimes(1);
    expect(taskErrorMock).toHaveBeenCalledTimes(1);

    expect(consoleErrorMock).toHaveBeenCalledTimes(1);
    expect(consoleErrorMock.mock.calls[0][0]).toEqual('error');
    expect(consoleErrorMock.mock.calls[0][2]).toEqual(taskException);
  });

  test('.process() waits for tasks to settle by default', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    const processPromise = processor.process(
      mutators,
      params,
      makePushBatch([[1, customMutatorKey('postCommit', 'asyncResolution')]]),
    );

    let processCompleted = false;
    void processPromise.then(() => {
      processCompleted = true;
    });

    // Wait for the full mutation flow to complete w/ PG.
    for (let i = 0; i < 10; i++) {
      await sleep(10);
    }

    expect(asyncResolutionMock).toHaveBeenCalledTimes(1);
    expect(processCompleted).toBe(false);

    resolveAsyncResolution(undefined);

    await sleep(1);

    expect(processCompleted).toBe(true);
  });

  test('.process() does not wait for tasks to settle with async: true', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
      {async: true},
    );

    const processPromise = processor.process(
      mutators,
      params,
      makePushBatch([[1, customMutatorKey('postCommit', 'asyncResolution')]]),
    );

    let processCompleted = false;
    void processPromise.then(() => {
      processCompleted = true;
    });

    // Wait for the full mutation flow to complete w/ PG.
    for (let i = 0; i < 10; i++) {
      await sleep(10);
    }

    expect(asyncResolutionMock).toHaveBeenCalledTimes(1);
    expect(processCompleted).toBe(true);

    // Test close() behavior
    const closePromise = processor.close();
    let closeCompleted = false;
    void closePromise.then(() => {
      closeCompleted = true;
    });

    // Close should not resolve while tasks are pending
    await sleep(1);
    expect(closeCompleted).toBe(false);

    // Resolving the task should allow close() to complete
    resolveAsyncResolution(undefined);
    await sleep(1);
    expect(closeCompleted).toBe(true);
  });

  test('.process() does not accept new mutations after close() is called', async () => {
    const processor = new PushProcessor(
      new ZQLDatabaseProvider(new ZQLPostgresJSAdapter(pg), schema),
    );

    void processor.close();

    await expect(
      processor.process(
        mutators,
        params,
        makePushBatch([[1, customMutatorKey('postCommit', 'noError')]]),
      ),
    ).rejects.toThrow(
      'PushProcessor has been closed and cannot process any more mutations',
    );
  });
});

async function checkClientsTable(
  pg: PostgresDB,
  expectedLmid: number | undefined,
) {
  const result = await pg.unsafe(
    `select "lastMutationID" from "zero_0"."clients" where "clientID" = $1`,
    ['cid'],
  );
  expect(result).toEqual(
    expectedLmid === undefined ? [] : [{lastMutationID: BigInt(expectedLmid)}],
  );
}
