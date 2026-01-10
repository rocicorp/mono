import {assert, beforeEach, describe, expect, test} from 'vitest';
import {zip} from '../../shared/src/arrays.ts';
import {
  getClientsTableDefinition,
  getMutationsTableDefinition,
} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/error.ts';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {ApplicationError} from '../../zero-protocol/src/application-error.ts';
import {ErrorKind} from '../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../zero-protocol/src/error-reason.ts';
import {
  CLEANUP_RESULTS_MUTATION_NAME,
  type MutationResult,
  type PushBody,
} from '../../zero-protocol/src/push.ts';
import {customMutatorKey} from '../../zql/src/mutate/custom.ts';
import {PostgresJSConnection} from './adapters/postgresjs.ts';
import {OutOfOrderMutation} from './process-mutations.ts';
import {PushProcessor} from './push-processor.ts';
import {ZQLDatabase} from './zql-database.ts';

let pg: PostgresDB;
const params = {
  schema: 'zero_0',
  appID: 'zero',
};
beforeEach(async () => {
  pg = await testDBs.create('push-processor-test');
  await pg.unsafe(`
    CREATE SCHEMA IF NOT EXISTS zero_0;
    ${getClientsTableDefinition('zero_0')}
    ${getMutationsTableDefinition('zero_0')}
  `);

  return async () => {
    await testDBs.drop(pg);
  };
});

function makePush(
  mid: number | number[],
  mutatorName: string | string[] = customMutatorKey('|', ['foo', 'bar']),
  clientID: string = 'cid',
): PushBody {
  const mids = Array.isArray(mid) ? mid : [mid];
  const mutatorNames = Array.isArray(mutatorName) ? mutatorName : [mutatorName];
  return {
    pushVersion: 1,
    clientGroupID: 'cgid',
    requestID: 'rid',
    schemaVersion: 1,
    timestamp: 42,
    mutations: zip(mids, mutatorNames).map(([mid, mutatorName]) => ({
      type: 'custom',
      clientID,
      id: mid,
      name: mutatorName,
      timestamp: 42,
      args: [],
    })),
  };
}

const mutators = {
  foo: {
    bar: () => Promise.resolve(),
    baz: () => Promise.reject(new Error('application error')),
  },
} as const;

describe('out of order mutation', () => {
  test('first mutation is out of order', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(pg), {
        tables: {},
        relationships: {},
        version: 1,
      }),
    );
    const result = await processor.process(mutators, params, makePush(15));

    assert('kind' in result, 'expected push failed response');

    expect(result).toEqual({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: 'Client cid sent mutation ID 15 but expected 1',
      mutationIDs: [
        {
          clientID: 'cid',
          id: 15,
        },
      ],
    });

    await checkClientsTable(pg, undefined);
    // OOO does not write a result
    await checkMutationsTable(pg, []);
  });

  test('later mutations are out of order', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(pg), {
        tables: {},
        relationships: {},
        version: 1,
      }),
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

    const result = await processor.process(mutators, params, makePush(3));

    assert('kind' in result, 'expected push failed response');

    expect(result).toEqual({
      kind: ErrorKind.PushFailed,
      origin: ErrorOrigin.Server,
      reason: ErrorReason.OutOfOrderMutation,
      message: 'Client cid sent mutation ID 3 but expected 2',
      mutationIDs: [
        {
          clientID: 'cid',
          id: 3,
        },
      ],
    });

    await checkClientsTable(pg, 1);
    // only error responses are written
    await checkMutationsTable(pg, []);
  });
});

test('first mutation', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
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
  // only error responses are written
  await checkMutationsTable(pg, []);
});

test('previously seen mutation', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
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
  await checkMutationsTable(pg, []);
});

test('lmid still moves forward if the mutator implementation throws', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );

  await processor.process(mutators, params, makePush(1));
  await processor.process(mutators, params, makePush(2));
  const result = await processor.process(
    mutators,
    params,
    makePush(3, customMutatorKey('|', ['foo', 'baz'])),
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
          message: 'application error',
        },
      },
    ],
  });
  await checkClientsTable(pg, 3);
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 3n,
      result: {
        error: 'app',
        message: 'application error',
      },
    },
  ]);
});

test('processes all mutations, even if all mutations throw app errors', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );

  expect(
    await processor.process(
      mutators,
      params,
      makePush(
        [1, 2, 3, 4],
        Array(4).fill(customMutatorKey('|', ['foo', 'baz'])),
      ),
    ),
  ).toEqual({
    mutations: Array.from({length: 4}, (_, i) => ({
      id: {
        clientID: 'cid',
        id: i + 1,
      },
      result: {
        error: 'app',
        message: 'application error',
      },
    })),
  });

  await checkClientsTable(pg, 4);
  await checkMutationsTable(
    pg,
    Array.from({length: 4}, (_, i) => ({
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: BigInt(i + 1),
      result: {
        error: 'app',
        message: 'application error',
      },
    })),
  );
});

test('processes all mutations, even if all mutations have been seen before', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );

  // process a bunch of successful mutations first
  await processor.process(
    mutators,
    params,
    makePush(
      [1, 2, 3, 4],
      Array(4).fill(customMutatorKey('|', ['foo', 'bar'])),
    ),
  );

  async function resend(basis: number, mutator: string) {
    expect(
      await processor.process(
        mutators,
        params,
        makePush(
          Array.from({length: 4}, (_, i) => basis + i + 1),
          Array(4).fill(mutator),
        ),
      ),
    ).toEqual({
      mutations: Array.from({length: 4}, (_, i) => ({
        id: {
          clientID: 'cid',
          id: basis + i + 1,
        },
        result: {
          details: `Ignoring mutation from cid with ID ${basis + i + 1} as it was already processed. Expected: ${basis + 4 + 1}`,
          error: 'alreadyProcessed',
        },
      })),
    });
  }

  // re-send the same mutations
  await resend(0, customMutatorKey('|', ['foo', 'bar']));

  // process a bunch of mutations that throw app errors
  await processor.process(
    mutators,
    params,
    makePush(
      [5, 6, 7, 8],
      Array(4).fill(customMutatorKey('|', ['foo', 'baz'])),
    ),
  );

  // re-send the same mutations that throw app errors
  await resend(4, customMutatorKey('|', ['foo', 'baz']));

  expect(
    await pg`select "clientGroupID", "clientID", "mutationID", "result" from "zero_0"."mutations" order by "mutationID"`,
  ).toMatchInlineSnapshot(`
    Result [
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 5n,
        "result": {
          "error": "app",
          "message": "application error",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 6n,
        "result": {
          "error": "app",
          "message": "application error",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 7n,
        "result": {
          "error": "app",
          "message": "application error",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 8n,
        "result": {
          "error": "app",
          "message": "application error",
        },
      },
    ]
  `);
});

test('continues processing if all mutations throw in error mode with "MutationAlreadyProcessedError"', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // oxlint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new MutationAlreadyProcessedError('cid', 1, 2);
    }

    throw new ApplicationError('application error');
  };
  const processor = new PushProcessor(db);

  expect(
    await processor.process(
      mutators,
      params,
      makePush(
        Array.from({length: 4}, (_, i) => i + 1),
        Array(4).fill('foo|bar'),
      ),
    ),
  ).toEqual({
    mutations: Array.from({length: 4}, (_, i) => ({
      id: {
        clientID: 'cid',
        id: i + 1,
      },
      result: {
        details: `Ignoring mutation from cid with ID 1 as it was already processed. Expected: 2`,
        error: 'alreadyProcessed',
      },
    })),
  });

  await checkClientsTable(pg, undefined);
  await checkMutationsTable(pg, []);
});

test('bails processing if all mutations throw in error mode with "OutOfOrderMutation"', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // oxlint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new OutOfOrderMutation('cid', 1, 2);
    }

    throw new ApplicationError('application error');
  };
  const processor = new PushProcessor(db);

  const response = await processor.process(
    mutators,
    params,
    makePush(
      Array.from({length: 4}, (_, i) => i + 1),
      Array(4).fill('foo|bar'),
    ),
  );

  assert('kind' in response, 'expected push failed response');

  expect(response).toEqual({
    kind: ErrorKind.PushFailed,
    origin: ErrorOrigin.Server,
    reason: ErrorReason.OutOfOrderMutation,
    message: 'Client cid sent mutation ID 1 but expected 2',
    mutationIDs: [
      {clientID: 'cid', id: 1},
      {clientID: 'cid', id: 2},
      {clientID: 'cid', id: 3},
      {clientID: 'cid', id: 4},
    ],
  });

  await checkClientsTable(pg, undefined);
  await checkMutationsTable(pg, []);
});

test('bails processing if a mutation throws an unknown error in error mode', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // oxlint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new Error('unknown');
    }

    throw new ApplicationError('application error');
  };
  const processor = new PushProcessor(db);

  const response = await processor.process(
    mutators,
    params,
    makePush(
      Array.from({length: 4}, (_, i) => i + 1),
      Array(4).fill('foo|bar'),
    ),
  );

  assert('kind' in response, 'expected push failed response');

  expect(response).toEqual({
    kind: ErrorKind.PushFailed,
    origin: ErrorOrigin.Server,
    reason: ErrorReason.Database,
    message: 'Failed to open database transaction: unknown',
    mutationIDs: [
      {clientID: 'cid', id: 1},
      {clientID: 'cid', id: 2},
      {clientID: 'cid', id: 3},
      {clientID: 'cid', id: 4},
    ],
    details: {
      name: 'DatabaseTransactionError',
    },
  });
  // These are not written since error mode fails too
  await checkClientsTable(pg, undefined);
  await checkMutationsTable(pg, []);
});

test('stops processing mutations as soon as it hits an out of order mutation', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );

  const response = await processor.process(
    mutators,
    params,
    makePush(
      [1, 2, 5, 4],
      [
        customMutatorKey('|', ['foo', 'bar']),
        customMutatorKey('|', ['foo', 'bar']),
        customMutatorKey('|', ['foo', 'bar']),
        customMutatorKey('|', ['foo', 'bar']),
      ],
    ),
  );

  assert('kind' in response, 'expected push failed response');

  // only *unprocessed* mutations are included in the response
  expect(response).toEqual({
    kind: ErrorKind.PushFailed,
    origin: ErrorOrigin.Server,
    reason: ErrorReason.OutOfOrderMutation,
    message: 'Client cid sent mutation ID 5 but expected 3',
    mutationIDs: [
      {
        clientID: 'cid',
        id: 5,
      },
      {
        clientID: 'cid',
        id: 4,
      },
    ],
  });
  await checkClientsTable(pg, 2);
  await checkMutationsTable(pg, []);
});

test('a mutation throws an app error then an ooo mutation error', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // oxlint-disable-next-line require-await
  db.transaction = async () => {
    if (c.c++ === 0) {
      throw new ApplicationError('application error');
    }
    throw new OutOfOrderMutation('cid', 1, 2);
  };
  const processor = new PushProcessor(db);

  // We should still catch and correctly report errors
  // even when running in error mode
  const response = await processor.process(
    mutators,
    params,
    makePush(1, customMutatorKey('|', ['foo', 'baz'])),
  );

  assert('kind' in response, 'expected push failed response');

  expect(response).toEqual({
    kind: ErrorKind.PushFailed,
    origin: ErrorOrigin.Server,
    reason: ErrorReason.OutOfOrderMutation,
    message: 'Client cid sent mutation ID 1 but expected 2',
    mutationIDs: [{clientID: 'cid', id: 1}],
  });

  // These are empty since the error mode fails too
  // and does not write to the database
  await checkClientsTable(pg, undefined);
  await checkMutationsTable(pg, []);
});

test('mutation throws an app error then an already processed error', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // oxlint-disable-next-line require-await
  db.transaction = async () => {
    if (c.c++ === 0) {
      throw new ApplicationError('application error');
    }
    throw new MutationAlreadyProcessedError('cid', 1, 2);
  };
  const processor = new PushProcessor(db);

  // We should still catch and correctly report errors
  // even when running in error mode
  const response = await processor.process(
    mutators,
    params,
    makePush(1, customMutatorKey('|', ['foo', 'baz'])),
  );

  // When MutationAlreadyProcessedError is thrown during error retry,
  // it returns a mutation response (not a server error)
  expect(response).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {
          error: 'alreadyProcessed',
          details:
            'Ignoring mutation from cid with ID 1 as it was already processed. Expected: 2',
        },
      },
    ],
  });
  await checkClientsTable(pg, undefined);
  await checkMutationsTable(pg, []);
});

test('mutators with and without namespaces', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );
  const mutators = {
    namespaced: {
      pass: () => Promise.resolve(),
      reject: () => Promise.reject(new Error('application error')),
    },
    topPass: () => Promise.resolve(),
    topReject: () =>
      Promise.reject(
        new ApplicationError('application error with details', {
          details: {key: 'value1'},
        }),
      ),
  };

  expect(
    await processor.process(
      mutators,
      params,
      makePush(1, customMutatorKey('|', ['namespaced', 'pass'])),
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
      makePush(3, customMutatorKey('|', ['namespaced', 'reject'])),
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
            "error": "app",
            "message": "application error",
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
              "details": {
                "key": "value1",
              },
              "error": "app",
              "message": "application error with details",
            },
          },
        ],
      }
    `);

  await checkClientsTable(pg, 4);
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 3n,
      result: {
        error: 'app',
        message: 'application error',
      },
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 4n,
      result: {
        error: 'app',
        message: 'application error with details',
        details: {
          key: 'value1',
        },
      },
    },
  ]);
});

test('mutators with arbitrary depth nesting', async () => {
  const processor = new PushProcessor(
    new ZQLDatabase(new PostgresJSConnection(pg), {
      tables: {},
      relationships: {},
      version: 1,
    }),
  );
  const mutators = {
    level1: {
      level2: {
        level3: {
          pass: () => Promise.resolve(),
          reject: () => Promise.reject(new Error('deep application error')),
        },
      },
    },
    deep: {
      nested: {
        structure: {
          works: {
            fine: () => Promise.resolve(),
          },
        },
      },
    },
  };

  // Test 3-level nesting - pass
  expect(
    await processor.process(
      mutators,
      params,
      makePush(1, 'level1|level2|level3|pass'),
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

  // Test 3-level nesting - reject
  expect(
    await processor.process(
      mutators,
      params,
      makePush(2, 'level1|level2|level3|reject'),
    ),
  ).toMatchInlineSnapshot(`
    {
      "mutations": [
        {
          "id": {
            "clientID": "cid",
            "id": 2,
          },
          "result": {
            "error": "app",
            "message": "deep application error",
          },
        },
      ],
    }
  `);

  // Test 5-level nesting
  expect(
    await processor.process(
      mutators,
      params,
      makePush(3, 'deep|nested|structure|works|fine'),
    ),
  ).toMatchInlineSnapshot(`
    {
      "mutations": [
        {
          "id": {
            "clientID": "cid",
            "id": 3,
          },
          "result": {},
        },
      ],
    }
  `);

  await checkClientsTable(pg, 3);
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 2n,
      result: {
        error: 'app',
        message: 'deep application error',
      },
    },
  ]);
});

describe('cleanup mutations', () => {
  test('cleanup mutation deletes mutation results from database', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(pg), {
        tables: {},
        relationships: {},
        version: 1,
      }),
    );

    // First, create some mutation results by processing failing mutations
    await processor.process(
      mutators,
      params,
      makePush([1, 2, 3], Array(3).fill(customMutatorKey('|', ['foo', 'baz']))),
    );

    // Verify results were written
    await checkMutationsTable(pg, [
      {
        clientGroupID: 'cgid',
        clientID: 'cid',
        mutationID: 1n,
        result: {error: 'app', message: 'application error'},
      },
      {
        clientGroupID: 'cgid',
        clientID: 'cid',
        mutationID: 2n,
        result: {error: 'app', message: 'application error'},
      },
      {
        clientGroupID: 'cgid',
        clientID: 'cid',
        mutationID: 3n,
        result: {error: 'app', message: 'application error'},
      },
    ]);
    await checkClientsTable(pg, 3);

    // Send cleanup mutation to delete results up to mutation 2
    const cleanupResult = await processor.process(mutators, params, {
      pushVersion: 1,
      clientGroupID: 'cgid',
      requestID: 'cleanup-rid',
      schemaVersion: 1,
      timestamp: 42,
      mutations: [
        {
          type: 'custom',
          clientID: 'cid',
          id: 0,
          name: CLEANUP_RESULTS_MUTATION_NAME,
          timestamp: 42,
          args: [
            {
              clientGroupID: 'cgid',
              clientID: 'cid',
              upToMutationID: 2,
            },
          ],
        },
      ],
    });

    // Verify cleanup returns empty mutations (fire-and-forget)
    expect(cleanupResult).toEqual({mutations: []});

    // Verify only mutation 3 remains (mutations 1 and 2 were deleted)
    await checkMutationsTable(pg, [
      {
        clientGroupID: 'cgid',
        clientID: 'cid',
        mutationID: 3n,
        result: {error: 'app', message: 'application error'},
      },
    ]);

    // Verify LMID was not affected
    await checkClientsTable(pg, 3);
  });

  test('cleanup mutation does not invoke user mutators', async () => {
    const callTracker = {called: false};
    const trackingMutators = {
      foo: {
        bar: () => {
          callTracker.called = true;
          return Promise.resolve();
        },
        baz: () => Promise.reject(new Error('application error')),
      },
    };

    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(pg), {
        tables: {},
        relationships: {},
        version: 1,
      }),
    );

    // Send cleanup mutation
    await processor.process(trackingMutators, params, {
      pushVersion: 1,
      clientGroupID: 'cgid',
      requestID: 'cleanup-rid',
      schemaVersion: 1,
      timestamp: 42,
      mutations: [
        {
          type: 'custom',
          clientID: 'cid',
          id: 0,
          name: CLEANUP_RESULTS_MUTATION_NAME,
          timestamp: 42,
          args: [
            {
              clientGroupID: 'cgid',
              clientID: 'cid',
              upToMutationID: 5,
            },
          ],
        },
      ],
    });

    // Verify user mutator was NOT called
    expect(callTracker.called).toBe(false);

    // Verify no LMID was set (cleanup doesn't track LMID)
    await checkClientsTable(pg, undefined);
  });

  test('bulk cleanup mutation deletes all mutations for multiple clients', async () => {
    const processor = new PushProcessor(
      new ZQLDatabase(new PostgresJSConnection(pg), {
        tables: {},
        relationships: {},
        version: 1,
      }),
    );

    // Create mutation results for multiple clients by processing failing mutations
    // Client 1: mutations 1, 2, 3
    await processor.process(
      mutators,
      params,
      makePush(
        [1, 2, 3],
        Array(3).fill(customMutatorKey('|', ['foo', 'baz'])),
        'client1',
      ),
    );
    // Client 2: mutations 1, 2
    await processor.process(
      mutators,
      params,
      makePush(
        [1, 2],
        Array(2).fill(customMutatorKey('|', ['foo', 'baz'])),
        'client2',
      ),
    );
    // Client 3: mutation 1 (should NOT be deleted)
    await processor.process(
      mutators,
      params,
      makePush([1], [customMutatorKey('|', ['foo', 'baz'])], 'client3'),
    );

    // Verify all results were written (6 total mutations)
    const beforeCleanup = await pg.unsafe(
      `select "clientID", "mutationID" from "zero_0"."mutations" order by "clientID", "mutationID"`,
    );
    expect(beforeCleanup).toEqual([
      {clientID: 'client1', mutationID: 1n},
      {clientID: 'client1', mutationID: 2n},
      {clientID: 'client1', mutationID: 3n},
      {clientID: 'client2', mutationID: 1n},
      {clientID: 'client2', mutationID: 2n},
      {clientID: 'client3', mutationID: 1n},
    ]);

    // Send bulk cleanup mutation to delete all mutations for client1 and client2
    const cleanupResult = await processor.process(mutators, params, {
      pushVersion: 1,
      clientGroupID: 'cgid',
      requestID: 'cleanup-rid',
      schemaVersion: 1,
      timestamp: 42,
      mutations: [
        {
          type: 'custom',
          clientID: 'client1',
          id: 0,
          name: CLEANUP_RESULTS_MUTATION_NAME,
          timestamp: 42,
          args: [
            {
              type: 'bulk',
              clientGroupID: 'cgid',
              clientIDs: ['client1', 'client2'],
            },
          ],
        },
      ],
    });

    // Verify cleanup returns empty mutations (fire-and-forget)
    expect(cleanupResult).toEqual({mutations: []});

    // Verify only client3's mutation remains
    const afterCleanup = await pg.unsafe(
      `select "clientID", "mutationID" from "zero_0"."mutations" order by "clientID", "mutationID"`,
    );
    expect(afterCleanup).toEqual([{clientID: 'client3', mutationID: 1n}]);
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

async function checkMutationsTable(
  pg: PostgresDB,
  expected: {
    clientGroupID: string;
    clientID: string;
    mutationID: bigint;
    result: MutationResult;
  }[],
) {
  const result = await pg.unsafe(
    `select "clientGroupID", "clientID", "mutationID", "result" from "zero_0"."mutations" order by "mutationID"`,
  );
  expect(result).toEqual(expected);
}
