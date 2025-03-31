import {testDBs} from '../../zero-cache/src/test/db.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import {getClientsTableDefinition} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';

import {PushProcessor} from './web.ts';
import type {PushBody} from '../../zero-protocol/src/push.ts';
import {customMutatorKey} from '../../zql/src/mutate/custom.ts';
import {Connection} from './test/util.ts';

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

const mutators = {
  foo: {
    bar: () => Promise.resolve(),
    baz: () => Promise.reject(new Error('application error')),
  },
} as const;

describe('out of order mutation', () => {
  test('first mutation is out of order', async () => {
    const processor = new PushProcessor(
      {
        tables: {},
        relationships: {},
        version: 1,
      },
      () => new Connection(pg),
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
      {
        tables: {},
        relationships: {},
        version: 1,
      },
      () => new Connection(pg),
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
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
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
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
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
        result: {},
      },
    ],
  });

  await checkClientsTable(pg, 3);
});

test('lmid still moves forward if the mutator implementation throws', async () => {
  const processor = new PushProcessor(
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
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
