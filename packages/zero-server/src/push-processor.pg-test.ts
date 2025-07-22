import {testDBs} from '../../zero-cache/src/test/db.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {
  getClientsTableDefinition,
  getMutationsTableDefinition,
} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';

import {OutOfOrderMutation, PushProcessor} from './push-processor.ts';
import {PostgresJSConnection} from './adapters/postgresjs.ts';
import type {MutationResult, PushBody} from '../../zero-protocol/src/push.ts';
import {customMutatorKey} from '../../zql/src/mutate/custom.ts';
import {ZQLDatabase} from './zql-database.ts';
import {zip} from '../../shared/src/arrays.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';

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
    ${getMutationsTableDefinition('zero_0')}
  `);
});

function makePush(
  mid: number | number[],
  mutatorName: string | string[] = customMutatorKey('foo', 'bar'),
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
      clientID: 'cid',
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
    await checkMutationsTable(pg, [
      {
        clientGroupID: 'cgid',
        clientID: 'cid',
        mutationID: 1n,
        result: {},
      },
    ]);
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
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 1n,
      result: {},
    },
  ]);
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
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 1n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 2n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 3n,
      result: {},
    },
  ]);
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
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 1n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 2n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 3n,
      result: {
        error: 'app',
        details: 'application error',
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
      makePush([1, 2, 3, 4], Array(4).fill(customMutatorKey('foo', 'baz'))),
    ),
  ).toEqual({
    mutations: Array.from({length: 4}, (_, i) => ({
      id: {
        clientID: 'cid',
        id: i + 1,
      },
      result: {
        error: 'app',
        details: 'application error',
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
        details: 'application error',
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
    makePush([1, 2, 3, 4], Array(4).fill(customMutatorKey('foo', 'bar'))),
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
  await resend(0, customMutatorKey('foo', 'bar'));

  // process a bunch of mutations that throw app errors
  await processor.process(
    mutators,
    params,
    makePush([5, 6, 7, 8], Array(4).fill(customMutatorKey('foo', 'baz'))),
  );

  // re-send the same mutations that throw app errors
  await resend(4, customMutatorKey('foo', 'baz'));

  expect(
    await pg`select "clientGroupID", "clientID", "mutationID", "result" from "zero_0"."mutations" order by "mutationID"`,
  ).toMatchInlineSnapshot(`
    Result [
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 1n,
        "result": {},
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 2n,
        "result": {},
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 3n,
        "result": {},
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 4n,
        "result": {},
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 5n,
        "result": {
          "details": "application error",
          "error": "app",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 6n,
        "result": {
          "details": "application error",
          "error": "app",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 7n,
        "result": {
          "details": "application error",
          "error": "app",
        },
      },
      {
        "clientGroupID": "cgid",
        "clientID": "cid",
        "mutationID": 8n,
        "result": {
          "details": "application error",
          "error": "app",
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
  // eslint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new MutationAlreadyProcessedError('cid', 1, 2);
    }

    throw new Error('application error');
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
  // eslint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new OutOfOrderMutation('cid', 1, 2);
    }

    throw new Error('application error');
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
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {
          details: 'Client cid sent mutation ID 1 but expected 2',
          error: 'oooMutation',
        },
      },
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
  // eslint-disable-next-line require-await
  db.transaction = async () => {
    c.c++;
    if (c.c % 2 === 0) {
      throw new Error('unknown');
    }

    throw new Error('application error');
  };
  const processor = new PushProcessor(db);

  await expect(
    processor.process(
      mutators,
      params,
      makePush(
        Array.from({length: 4}, (_, i) => i + 1),
        Array(4).fill('foo|bar'),
      ),
    ),
  ).rejects.toThrow('unknown');
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

  expect(
    await processor.process(
      mutators,
      params,
      makePush(
        [1, 2, 5, 4],
        [
          customMutatorKey('foo', 'bar'),
          customMutatorKey('foo', 'bar'),
          customMutatorKey('foo', 'bar'),
          customMutatorKey('foo', 'bar'),
        ],
      ),
    ),
  ).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {},
      },
      {
        id: {
          clientID: 'cid',
          id: 2,
        },
        result: {},
      },
      {
        id: {
          clientID: 'cid',
          id: 5,
        },
        result: {
          details: 'Client cid sent mutation ID 5 but expected 3',
          error: 'oooMutation',
        },
      },
    ],
  });
  await checkClientsTable(pg, 2);
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 1n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 2n,
      result: {},
    },
  ]);
});

test('a mutation throws an app error then an ooo mutation error', async () => {
  const db = new ZQLDatabase(new PostgresJSConnection(pg), {
    tables: {},
    relationships: {},
    version: 1,
  });
  const c = {c: 0};
  // eslint-disable-next-line require-await
  db.transaction = async () => {
    if (c.c++ === 0) {
      throw new Error('application error');
    }
    throw new OutOfOrderMutation('cid', 1, 2);
  };
  const processor = new PushProcessor(db);

  // We should still catch and correctly report errors
  // even when running in error mode
  expect(
    await processor.process(
      mutators,
      params,
      makePush(1, customMutatorKey('foo', 'baz')),
    ),
  ).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {
          details: 'Client cid sent mutation ID 1 but expected 2',
          error: 'oooMutation',
        },
      },
    ],
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
  // eslint-disable-next-line require-await
  db.transaction = async () => {
    if (c.c++ === 0) {
      throw new Error('application error');
    }
    throw new MutationAlreadyProcessedError('cid', 1, 2);
  };
  const processor = new PushProcessor(db);

  // We should still catch and correctly report errors
  // even when running in error mode
  expect(
    await processor.process(
      mutators,
      params,
      makePush(1, customMutatorKey('foo', 'baz')),
    ),
  ).toEqual({
    mutations: [
      {
        id: {
          clientID: 'cid',
          id: 1,
        },
        result: {
          details:
            'Ignoring mutation from cid with ID 1 as it was already processed. Expected: 2',
          error: 'alreadyProcessed',
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
  await checkMutationsTable(pg, [
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 1n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 2n,
      result: {},
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 3n,
      result: {
        error: 'app',
        details: 'application error',
      },
    },
    {
      clientGroupID: 'cgid',
      clientID: 'cid',
      mutationID: 4n,
      result: {
        error: 'app',
        details: 'application error',
      },
    },
  ]);
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
