import {eq} from 'drizzle-orm';
import {drizzle as drizzleNodePg} from 'drizzle-orm/node-postgres';
import {pgTable, text} from 'drizzle-orm/pg-core';
import {drizzle as drizzlePostgresJs} from 'drizzle-orm/postgres-js';
import {Client, Pool, type PoolClient} from 'pg';
import {afterEach, beforeEach, describe, expectTypeOf, test} from 'vitest';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import {nanoid} from '../../../zero-client/src/util/nanoid.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  zeroDrizzleNodePg,
  type NodePgDrizzleTransaction,
} from './drizzle-pg.ts';
import {
  zeroDrizzlePostgresJS,
  type PostgresJsDrizzleTransaction,
} from './drizzle-postgresjs.ts';
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
  await postgresJsClient.end();

  await nodePgClient.end();

  await nodePgPoolClient.release();
  await nodePgPool.end();
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

      const zqlQuery = await zql.transaction(async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);

        return result;
      }, mockTransactionInput);

      const resultZQL = await zqlQuery.run();

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

    const zqlQuery = await zql.transaction(async tx => {
      const result = await tx.query.user.where('id', '=', newUser.id);
      return result;
    }, mockTransactionInput);

    const resultZQL = await zqlQuery.run();

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
});

describe('drizzle-node-postgres', () => {
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
    const poolTx = null as unknown as NodePgDrizzleTransaction<typeof pool>;
    const clientTx = null as unknown as NodePgDrizzleTransaction<typeof client>;
    const poolClientTx = null as unknown as NodePgDrizzleTransaction<
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
    const s = null as unknown as NodePgDrizzleTransaction<typeof drizzleSchema>;

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

      const zql = zeroDrizzleNodePg(schema, client);

      const zqlQuery = await zql.transaction(async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);
        return result;
      }, mockTransactionInput);
      const resultZQL = await zqlQuery.run();

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
});

describe('drizzle-postgres-js', () => {
  let client: ReturnType<typeof drizzlePostgresJs<typeof drizzleSchema>>;

  beforeEach(() => {
    client = drizzlePostgresJs(postgresJsClient, {
      schema: drizzleSchema,
    });
  });

  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await client.insert(drizzleSchema.user).values(newUser);

    const zql = zeroDrizzlePostgresJS(schema, client);

    const tx = await zql.transaction(async tx => {
      const result = await tx.query.user.where('id', '=', newUser.id);
      return result;
    }, mockTransactionInput);

    const result = await tx.run();

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('types - implicit schema generic', () => {
    const s = null as unknown as PostgresJsDrizzleTransaction<typeof client>;

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
    const s = null as unknown as PostgresJsDrizzleTransaction<
      typeof drizzleSchema
    >;

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

    const zql = zeroDrizzlePostgresJS(schema, client);

    const zqlQuery = await zql.transaction(async tx => {
      const result = await tx.query.user.where('id', '=', newUser.id);
      return result;
    }, mockTransactionInput);
    const resultZQL = await zqlQuery.run();

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
});
