import {testDBs} from '../../zero-cache/src/test/db.ts';
import {beforeEach, describe, expect, test} from 'vitest';
import type {
  PostgresDB,
  PostgresTransaction,
} from '../../zero-cache/src/types/pg.ts';
import {getClientsTableDefinition} from '../../zero-cache/src/services/change-source/pg/schema/shard.ts';
import type {DBConnection, DBTransaction, Row} from './db.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import {PushProcessor} from './web.ts';
import type {PushBody} from '../../zero-protocol/src/push.ts';
import {customMutatorKey} from '../../zql/src/mutate/custom.ts';

let pg: PostgresDB;
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
      '0',
      {
        tables: {},
        relationships: {},
        version: 1,
      },
      () => new Connection(pg),
      mutators,
    );
    const result = await processor.process({}, makePush(15));

    expect(result).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 15,
          },
          result: {
            error: 'ooo-mutation',
          },
        },
      ],
    });

    await checkClientsTable(pg, undefined);
  });

  test('later mutations are out of order', async () => {
    const processor = new PushProcessor(
      '0',
      {
        tables: {},
        relationships: {},
        version: 1,
      },
      () => new Connection(pg),
      mutators,
    );

    expect(await processor.process({}, makePush(1))).toEqual({
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

    expect(await processor.process({}, makePush(3))).toEqual({
      mutations: [
        {
          id: {
            clientID: 'cid',
            id: 3,
          },
          result: {
            error: 'ooo-mutation',
          },
        },
      ],
    });

    await checkClientsTable(pg, 1);
  });
});

test('first mutation', async () => {
  const processor = new PushProcessor(
    '0',
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
    mutators,
  );

  expect(await processor.process({}, makePush(1))).toEqual({
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
    '0',
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
    mutators,
  );

  await processor.process({}, makePush(1));
  await processor.process({}, makePush(2));
  await processor.process({}, makePush(3));

  expect(await processor.process({}, makePush(2))).toEqual({
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
    '0',
    {
      tables: {},
      relationships: {},
      version: 1,
    },
    () => new Connection(pg),
    mutators,
  );

  await processor.process({}, makePush(1));
  await processor.process({}, makePush(2));
  const result = await processor.process(
    {},
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

class Connection implements DBConnection<PostgresTransaction> {
  readonly #pg: PostgresDB;
  constructor(pg: PostgresDB) {
    this.#pg = pg;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#pg.unsafe(sql, params as JSONValue[]);
  }

  transaction<T>(
    fn: (tx: DBTransaction<PostgresTransaction>) => Promise<T>,
  ): Promise<T> {
    return pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<T>;
  }
}

class Transaction implements DBTransaction<PostgresTransaction> {
  readonly wrappedTransaction: PostgresTransaction;
  constructor(pgTx: PostgresTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}
