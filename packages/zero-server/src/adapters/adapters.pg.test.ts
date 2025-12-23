import type {StandardSchemaV1} from '@standard-schema/spec';
import {eq} from 'drizzle-orm';
import {drizzle as drizzleNodePg} from 'drizzle-orm/node-postgres';
import {pgTable, text} from 'drizzle-orm/pg-core';
import {drizzle as drizzlePostgresJs} from 'drizzle-orm/postgres-js';
import {Client, Pool, type PoolClient} from 'pg';
import type {ExpectStatic} from 'vitest';
import {afterEach, beforeEach, describe, expectTypeOf, test} from 'vitest';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import {nanoid} from '../../../zero-client/src/util/nanoid.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {
  addContextToQuery,
  defineQueries,
  defineQuery,
} from '../../../zql/src/query/query-registry.ts';
import type {ZQLDatabase} from '../zql-database.ts';
import {zeroDrizzle, type DrizzleTransaction} from './drizzle.ts';
import {zeroNodePg} from './pg.ts';
import {zeroPostgresJS} from './postgresjs.ts';

let postgresJsClient: PostgresDB;

// test all the ways to get a client in pg
let nodePgPool: Pool;
let nodePgPoolClient: PoolClient;
let nodePgClient: Client;

beforeEach(async () => {
  postgresJsClient = await testDBs.create('adapters-pg-test');
  nodePgPool = new Pool({
    connectionString: getConnectionURI(postgresJsClient),
  });
  nodePgPoolClient = await nodePgPool.connect();
  nodePgClient = new Client({
    connectionString: getConnectionURI(postgresJsClient),
  });

  await nodePgClient.connect();

  await postgresJsClient.unsafe(`
    CREATE TABLE IF NOT EXISTS "user" (
      id TEXT PRIMARY KEY,
      name TEXT,
      status TEXT
    )
  `);
});

afterEach(async () => {
  // Ensure all node-postgres clients are closed before dropping the DB
  await nodePgPoolClient.release();
  await nodePgClient.end();
  await nodePgPool.end();

  // Drop the per-test database to avoid global teardown force-terminating connections
  await testDBs.drop(postgresJsClient);
});

type UserStatus = 'active' | 'inactive';

const userTable = pgTable('user', {
  id: text('id').primaryKey().$type<`user_${string}`>(),
  name: text('name'),
  status: text('status').$type<UserStatus>().notNull(),
});

const drizzleSchema = {
  user: userTable,
};

const user = table('user')
  .columns({
    id: string(),
    name: string().optional(),
    status: string<UserStatus>(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [user],
  enableLegacyMutators: true,
  enableLegacyQueries: true,
});

const getRandomUser = () => {
  const id = nanoid();
  return {
    id: `user_${id}`,
    name: `User ${id}`,
    status: Math.random() > 0.5 ? 'active' : 'inactive',
  } as const;
};

const mockTransactionInput = {
  upstreamSchema: '',
  clientGroupID: '',
  clientID: '',
  mutationID: 0,
} as const;

async function exerciseMutations<WrappedTransaction>(
  zql: ZQLDatabase<typeof schema, WrappedTransaction>,
  expect: ExpectStatic,
) {
  const baseUser = getRandomUser();
  const alternateStatus: UserStatus =
    baseUser.status === 'active' ? 'inactive' : 'active';
  const updatedName = `${baseUser.name} (updated)`;

  await zql.transaction(async tx => {
    await tx.mutate.user.insert(baseUser);

    const inserted = await tx.run(tx.query.user.where('id', '=', baseUser.id));
    expect(inserted).toHaveLength(1);
    expect(inserted[0]?.status).toBe(baseUser.status);
    expect(inserted[0]?.name).toBe(baseUser.name);

    await tx.mutate.user.upsert({
      ...baseUser,
      name: updatedName,
      status: alternateStatus,
    });

    const afterUpsert = await tx.run(
      tx.query.user.where('id', '=', baseUser.id),
    );
    expect(afterUpsert[0]?.name).toBe(updatedName);
    expect(afterUpsert[0]?.status).toBe(alternateStatus);

    await tx.mutate.user.upsert({
      id: baseUser.id,
      status: baseUser.status,
    });

    const afterPartialUpsert = await tx.run(
      tx.query.user.where('id', '=', baseUser.id),
    );
    expect(afterPartialUpsert[0]?.name).toBe(updatedName);
    expect(afterPartialUpsert[0]?.status).toBe(baseUser.status);

    await tx.mutate.user.update({
      id: baseUser.id,
      name: undefined,
      status: alternateStatus,
    });

    const afterUpdate = await tx.run(
      tx.query.user.where('id', '=', baseUser.id),
    );
    expect(afterUpdate[0]?.name).toBe(updatedName);
    expect(afterUpdate[0]?.status).toBe(alternateStatus);

    await tx.mutate.user.delete({id: baseUser.id});

    const afterDelete = await tx.run(
      tx.query.user.where('id', '=', baseUser.id),
    );
    expect(afterDelete).toHaveLength(0);

    const namelessInsert = {
      id: `user_${nanoid()}`,
      status: 'inactive' as UserStatus,
    };
    await tx.mutate.user.insert(namelessInsert);

    const namelessRow = await tx.run(
      tx.query.user.where('id', '=', namelessInsert.id),
    );
    expect(namelessRow).toHaveLength(1);
    expect(namelessRow[0]?.name ?? null).toBeNull();

    await tx.mutate.user.upsert({
      id: namelessInsert.id,
      status: 'active' as UserStatus,
    });

    const namelessAfterUpsert = await tx.run(
      tx.query.user.where('id', '=', namelessInsert.id),
    );
    expect(namelessAfterUpsert[0]?.name ?? null).toBeNull();
    expect(namelessAfterUpsert[0]?.status).toBe('active');

    await tx.mutate.user.delete({id: namelessInsert.id});

    const cleanupCheck = await tx.run(
      tx.query.user.where('id', '=', namelessInsert.id),
    );
    expect(cleanupCheck).toHaveLength(0);
  }, mockTransactionInput);
}

describe('node-postgres', () => {
  test('querying', async ({expect}) => {
    const clients = [nodePgClient, nodePgPoolClient, nodePgPool];

    for (const client of clients) {
      const newUser = getRandomUser();

      await client.query(
        `
        INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)
      `,
        [newUser.id, newUser.name, newUser.status],
      );

      const zql = zeroNodePg(schema, client);

      const zqlQuery = await zql.transaction(tx => {
        const result = tx.query.user.where('id', '=', newUser.id);

        return result;
      }, mockTransactionInput);

      const resultZQL = await zql.run(zqlQuery);

      const resultClientQuery = await zql.transaction(async tx => {
        const result = await tx.dbTransaction.query(
          'SELECT * FROM "user" WHERE id = $1',
          [newUser.id],
        );
        return result;
      }, mockTransactionInput);

      expect(resultZQL[0]?.name).toEqual(newUser.name);
      expect(resultZQL[0]?.id).toEqual(newUser.id);

      for (const row of resultClientQuery) {
        expect(row.name).toBe(newUser.name);
        expect(row.id).toBe(newUser.id);
      }
    }
  });

  test('mutations', async ({expect}) => {
    const clients = [nodePgClient, nodePgPoolClient, nodePgPool];

    for (const client of clients) {
      const zql = zeroNodePg(schema, client);
      await exerciseMutations(zql, expect);
    }
  });
});

describe('postgres-js', () => {
  test('querying', async ({expect}) => {
    const newUser = getRandomUser();

    await postgresJsClient`
      INSERT INTO "user" (id, name, status) VALUES (${newUser.id}, ${newUser.name}, ${newUser.status})
    `;

    const zql = zeroPostgresJS(schema, postgresJsClient);

    const zqlQuery = await zql.transaction(tx => {
      const result = tx.query.user.where('id', '=', newUser.id);
      return result;
    }, mockTransactionInput);

    const resultZQL = await zql.run(zqlQuery);

    const resultClientQuery = await zql.transaction(async tx => {
      const result = await tx.dbTransaction.query(
        'SELECT * FROM "user" WHERE id = $1',
        [newUser.id],
      );
      return result;
    }, mockTransactionInput);

    expect(resultZQL[0]?.name).toEqual(newUser.name);
    expect(resultZQL[0]?.id).toEqual(newUser.id);

    for await (const row of resultClientQuery) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }
  });

  test('mutations', async ({expect}) => {
    const zql = zeroPostgresJS(schema, postgresJsClient);
    await exerciseMutations(zql, expect);
  });
});

describe('drizzle and node-postgres', () => {
  let pool: ReturnType<typeof drizzleNodePg<typeof drizzleSchema, Pool>>;
  let client: ReturnType<typeof drizzleNodePg<typeof drizzleSchema, Client>>;
  let poolClient: ReturnType<
    typeof drizzleNodePg<typeof drizzleSchema, PoolClient>
  >;

  beforeEach(() => {
    pool = drizzleNodePg(nodePgPool, {
      schema: drizzleSchema,
    });
    client = drizzleNodePg(nodePgClient, {
      schema: drizzleSchema,
    });
    poolClient = drizzleNodePg(nodePgPoolClient, {
      schema: drizzleSchema,
    });
  });

  test('types - implicit schema generic', () => {
    const poolTx = null as unknown as DrizzleTransaction<typeof pool>;
    const clientTx = null as unknown as DrizzleTransaction<typeof client>;
    const poolClientTx = null as unknown as DrizzleTransaction<
      typeof poolClient
    >;

    const poolTxUser = null as unknown as Awaited<
      ReturnType<typeof poolTx.query.user.findFirst>
    >;
    const clientTxUser = null as unknown as Awaited<
      ReturnType<typeof clientTx.query.user.findFirst>
    >;
    const poolClientTxUser = null as unknown as Awaited<
      ReturnType<typeof poolClientTx.query.user.findFirst>
    >;

    expectTypeOf(poolTxUser).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
    expectTypeOf(clientTxUser).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
    expectTypeOf(poolClientTxUser).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
  });

  test('types - explicit schema generic', () => {
    const s = null as unknown as DrizzleTransaction<typeof drizzleSchema>;

    const user = null as unknown as Awaited<
      ReturnType<typeof s.query.user.findFirst>
    >;

    expectTypeOf(user).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
  });

  test('querying', async ({expect}) => {
    // loop through all the possible ways to create a client
    const clients = [pool, client, poolClient];

    for (const client of clients) {
      const newUser = getRandomUser();

      await client.insert(drizzleSchema.user).values(newUser);

      const zql = zeroDrizzle(schema, client);

      const zqlQuery = await zql.transaction(
        tx => tx.query.user.where('id', '=', newUser.id),
        mockTransactionInput,
      );
      const resultZQL = await zql.run(zqlQuery);

      const resultClientQuery = await zql.transaction(async tx => {
        const result = await tx.dbTransaction.query(
          'SELECT * FROM "user" WHERE id = $1',
          [newUser.id],
        );
        return result;
      }, mockTransactionInput);

      const resultDrizzleQuery = await zql.transaction(async tx => {
        const result =
          await tx.dbTransaction.wrappedTransaction.query.user.findFirst({
            where: eq(drizzleSchema.user.id, newUser.id),
          });
        return result;
      }, mockTransactionInput);

      expect(resultZQL[0]?.name).toEqual(newUser.name);
      expect(resultZQL[0]?.id).toEqual(newUser.id);

      for await (const row of resultClientQuery) {
        expect(row.name).toBe(newUser.name);
        expect(row.id).toBe(newUser.id);
      }

      expect(resultDrizzleQuery?.name).toEqual(newUser.name);
      expect(resultDrizzleQuery?.id).toEqual(newUser.id);
    }
  });

  test('mutations', async ({expect}) => {
    const clients = [pool, client, poolClient];

    for (const drizzleClient of clients) {
      const zql = zeroDrizzle(schema, drizzleClient);
      await exerciseMutations(zql, expect);
    }
  });

  test('type portability - inferred types should not reference internal drizzle paths', () => {
    function getZQL() {
      return zeroDrizzle(schema, client);
    }

    const zql = getZQL();

    type TxType = DrizzleTransaction<typeof client>;

    expectTypeOf<
      Awaited<
        ReturnType<
          Awaited<ReturnType<TxType['query']['user']['findFirst']>['execute']>
        >
      >
    >().toExtend<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
    expectTypeOf(zql).toExtend<ZQLDatabase<typeof schema, TxType>>();
  });
});

describe('drizzle and postgres-js', () => {
  let client: ReturnType<typeof drizzlePostgresJs<typeof drizzleSchema>>;

  beforeEach(() => {
    client = drizzlePostgresJs(postgresJsClient, {
      schema: drizzleSchema,
    });
  });

  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await client.insert(drizzleSchema.user).values(newUser);

    const zql = zeroDrizzle(schema, client);

    const q = await zql.transaction(
      tx => tx.query.user.where('id', '=', newUser.id),
      mockTransactionInput,
    );

    const result = await zql.run(q);

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('types - implicit schema generic', () => {
    const s = null as unknown as DrizzleTransaction<typeof client>;

    const user = null as unknown as Awaited<
      ReturnType<typeof s.query.user.findFirst>
    >;

    expectTypeOf(user).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
  });

  test('types - explicit schema generic', () => {
    const s = null as unknown as DrizzleTransaction<typeof drizzleSchema>;

    const user = null as unknown as Awaited<
      ReturnType<typeof s.query.user.findFirst>
    >;

    expectTypeOf(user).toEqualTypeOf<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
  });

  test('querying', async ({expect}) => {
    const newUser = getRandomUser();

    await client.insert(drizzleSchema.user).values(newUser);

    const zql = zeroDrizzle(schema, client);

    const resultZQL = await zql.transaction(
      tx => tx.run(tx.query.user.where('id', '=', newUser.id)),
      mockTransactionInput,
    );

    const resultClientQuery = await zql.transaction(async tx => {
      const result = await tx.dbTransaction.query(
        'SELECT * FROM "user" WHERE id = $1',
        [newUser.id],
      );
      return result;
    }, mockTransactionInput);

    const resultDrizzleQuery = await zql.transaction(async tx => {
      const result =
        await tx.dbTransaction.wrappedTransaction.query.user.findFirst({
          where: eq(drizzleSchema.user.id, newUser.id),
        });
      return result;
    }, mockTransactionInput);

    expect(resultZQL[0]?.name).toEqual(newUser.name);
    expect(resultZQL[0]?.id).toEqual(newUser.id);

    for await (const row of resultClientQuery) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }

    expect(resultDrizzleQuery?.name).toEqual(newUser.name);
    expect(resultDrizzleQuery?.id).toEqual(newUser.id);
  });

  test('mutations', async ({expect}) => {
    const zql = zeroDrizzle(schema, client);
    await exerciseMutations(zql, expect);
  });

  test('type portability', () => {
    function getZQL() {
      return zeroDrizzle(schema, client);
    }

    const zql = getZQL();

    type TxType = DrizzleTransaction<typeof client>;

    expectTypeOf<
      Awaited<
        ReturnType<
          Awaited<ReturnType<TxType['query']['user']['findFirst']>['execute']>
        >
      >
    >().toExtend<
      | {
          id: `user_${string}`;
          name: string | null;
          status: UserStatus;
        }
      | undefined
    >();
    expectTypeOf(zql).toExtend<ZQLDatabase<typeof schema, TxType>>();
  });
});

describe('custom queries (from defineQueries) in ServerTransaction', () => {
  const zql = createBuilder(schema);

  test('tx.run() works with custom query without validator', async ({
    expect,
  }) => {
    const customQueries = defineQueries({
      allUsers: defineQuery(() => zql.user.orderBy('id', 'asc')),
      userById: defineQuery(({args}: {args: {id: string}}) =>
        zql.user.where('id', args.id).one(),
      ),
    });

    const zqlDB = zeroPostgresJS(schema, postgresJsClient);
    const testUser = getRandomUser();

    const result = await zqlDB.transaction(async tx => {
      // Insert a user
      await tx.mutate.user.insert(testUser);

      // Query all users using custom query
      const allUsers = await tx.run(
        addContextToQuery(customQueries.allUsers(), {}),
      );

      // Query specific user using custom query
      const specificUser = await tx.run(
        addContextToQuery(customQueries.userById({id: testUser.id}), {}),
      );

      return {allUsers, specificUser};
    }, mockTransactionInput);

    expect(result.allUsers.length).toBeGreaterThan(0);
    expect(result.allUsers.some(u => u.id === testUser.id)).toBe(true);
    expect(result.specificUser).toEqual(
      expect.objectContaining({
        id: testUser.id,
        name: testUser.name,
        status: testUser.status,
      }),
    );
  });

  test('tx.run() works with custom query with transforming validator', async ({
    expect,
  }) => {
    // Validator that transforms input string to ensure it's a valid status
    const statusValidator: StandardSchemaV1<
      {status: string},
      {status: UserStatus}
    > = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: data => {
          const status = (data as {status: string}).status.toLowerCase();
          if (status !== 'active' && status !== 'inactive') {
            return {
              issues: [{message: 'status must be active or inactive'}],
            };
          }
          return {
            value: {status: status as UserStatus},
          };
        },
      },
    };

    const customQueries = defineQueries({
      usersByStatus: defineQuery(
        statusValidator,
        ({args}: {args: {status: UserStatus}}) =>
          zql.user.where('status', args.status),
      ),
    });

    const zqlDB = zeroPostgresJS(schema, postgresJsClient);
    const activeUser = {...getRandomUser(), status: 'active' as UserStatus};
    const inactiveUser = {
      ...getRandomUser(),
      status: 'inactive' as UserStatus,
    };

    const result = await zqlDB.transaction(async tx => {
      await tx.mutate.user.insert(activeUser);
      await tx.mutate.user.insert(inactiveUser);

      // Query using mixed case input that gets transformed to lowercase
      const activeUsers = await tx.run(
        addContextToQuery(customQueries.usersByStatus({status: 'ACTIVE'}), {}),
      );

      return {activeUsers};
    }, mockTransactionInput);

    expect(result.activeUsers.length).toBeGreaterThan(0);
    expect(result.activeUsers.every(u => u.status === 'active')).toBe(true);
    expect(result.activeUsers.some(u => u.id === activeUser.id)).toBe(true);
  });

  test('tx.run() throws when custom query validator fails', async ({
    expect,
  }) => {
    const throwingValidator: StandardSchemaV1<{id: string}, {id: string}> = {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: data => ({
          issues: [
            {
              message: `Validation failed for ID: ${(data as {id: string}).id}`,
            },
          ],
        }),
      },
    };

    const customQueries = defineQueries({
      userById: defineQuery(throwingValidator, ({args}: {args: {id: string}}) =>
        zql.user.where('id', args.id).one(),
      ),
    });

    const zqlDB = zeroPostgresJS(schema, postgresJsClient);

    await expect(
      zqlDB.transaction(async tx => {
        await tx.run(
          addContextToQuery(customQueries.userById({id: 'invalid-id'}), {}),
        );
      }, mockTransactionInput),
    ).rejects.toThrow('Validation failed for ID: invalid-id');
  });

  test('tx.run() works with nested custom queries', async ({expect}) => {
    const customQueries = defineQueries({
      users: {
        all: defineQuery(() => zql.user),
        active: defineQuery(() => zql.user.where('status', 'active')),
        inactive: defineQuery(() => zql.user.where('status', 'inactive')),
        byName: defineQuery(({args}: {args: {name: string}}) =>
          zql.user.where('name', args.name),
        ),
      },
    });

    const zqlDB = zeroPostgresJS(schema, postgresJsClient);
    const user1 = {...getRandomUser(), status: 'active' as UserStatus};
    const user2 = {...getRandomUser(), status: 'inactive' as UserStatus};

    const result = await zqlDB.transaction(async tx => {
      await tx.mutate.user.insert(user1);
      await tx.mutate.user.insert(user2);

      const all = await tx.run(
        addContextToQuery(customQueries.users.all(), {}),
      );
      const active = await tx.run(
        addContextToQuery(customQueries.users.active(), {}),
      );
      const inactive = await tx.run(
        addContextToQuery(customQueries.users.inactive(), {}),
      );
      const byName = await tx.run(
        addContextToQuery(customQueries.users.byName({name: user1.name}), {}),
      );

      return {all, active, inactive, byName};
    }, mockTransactionInput);

    expect(result.all.length).toBeGreaterThanOrEqual(2);
    expect(result.active.length).toBeGreaterThan(0);
    expect(result.inactive.length).toBeGreaterThan(0);
    expect(result.byName.some(u => u.id === user1.id)).toBe(true);
  });

  test('tx.run() works with all adapters (node-pg, postgresjs, drizzle)', async ({
    expect,
  }) => {
    const customQueries = defineQueries({
      userById: defineQuery(({args}: {args: {id: string}}) =>
        zql.user.where('id', args.id).one(),
      ),
    });

    const testUser = getRandomUser();

    // Insert test data
    await postgresJsClient.unsafe(
      `INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)`,
      [testUser.id, testUser.name, testUser.status],
    );

    // Test with postgres.js
    const zqlPostgresJS = zeroPostgresJS(schema, postgresJsClient);
    const resultPostgresJS = await zqlPostgresJS.transaction(
      async tx =>
        await tx.run(
          addContextToQuery(customQueries.userById({id: testUser.id}), {}),
        ),
      mockTransactionInput,
    );
    expect(resultPostgresJS).toEqual(
      expect.objectContaining({id: testUser.id}),
    );

    // Test with node-pg Pool
    const zqlNodePgPool = zeroNodePg(schema, nodePgPool);
    const resultNodePgPool = await zqlNodePgPool.transaction(
      async tx =>
        await tx.run(
          addContextToQuery(customQueries.userById({id: testUser.id}), {}),
        ),
      mockTransactionInput,
    );
    expect(resultNodePgPool).toEqual(
      expect.objectContaining({id: testUser.id}),
    );

    // Test with drizzle
    const drizzleClient = drizzlePostgresJs(postgresJsClient, {
      schema: drizzleSchema,
    });
    const zqlDrizzle = zeroDrizzle(schema, drizzleClient);
    const resultDrizzle = await zqlDrizzle.transaction(
      async tx =>
        await tx.run(
          addContextToQuery(customQueries.userById({id: testUser.id}), {}),
        ),
      mockTransactionInput,
    );
    expect(resultDrizzle).toEqual(expect.objectContaining({id: testUser.id}));

    // Cleanup
    await postgresJsClient.unsafe(`DELETE FROM "user" WHERE id = $1`, [
      testUser.id,
    ]);
  });
});
