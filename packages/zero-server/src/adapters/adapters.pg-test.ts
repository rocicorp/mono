import {eq} from 'drizzle-orm';
import {drizzle as drizzleNodePg} from 'drizzle-orm/node-postgres';
import {pgTable, text} from 'drizzle-orm/pg-core';
import {drizzle as drizzlePostgresJs} from 'drizzle-orm/postgres-js';
import {Pool} from 'pg';
import {afterEach, beforeEach, describe, expectTypeOf, test} from 'vitest';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../../zero-cache/src/types/pg.ts';
import {nanoid} from '../../../zero-client/src/util/nanoid.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  NodePgDrizzleConnection,
  zeroDrizzleNodePg,
  type NodePgDrizzleTransaction,
} from './drizzle-pg.ts';
import {
  PostgresJsDrizzleConnection,
  zeroDrizzlePostgresJS,
  type PostgresJsDrizzleTransaction,
} from './drizzle-postgresjs.ts';
import {NodePgConnection, zeroNodePg} from './pg.ts';
import {PostgresJSConnection, zeroPostgresJS} from './postgresjs.ts';

let postgresJsClient: PostgresDB;
let nodePgClient: Pool;

beforeEach(async () => {
  postgresJsClient = await testDBs.create('adapters-pg-test');
  nodePgClient = new Pool({
    connectionString: getConnectionURI(postgresJsClient),
  });
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

describe('node-postgres', () => {
  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await nodePgClient.query(
      `
      INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)
    `,
      [newUser.id, newUser.name, newUser.status],
    );

    const zql = zeroNodePg(schema, nodePgClient);

    const result = await zql.transaction(
      async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);
        return result;
      },
      {
        upstreamSchema: '',
        clientGroupID: '',
        clientID: '',
        mutationID: 0,
      },
    );

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('can query from the database in a transaction', async ({expect}) => {
    const newUser = getRandomUser();

    await nodePgClient.query(
      `
      INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)
    `,
      [newUser.id, newUser.name, newUser.status],
    );

    const nodePgConnection = new NodePgConnection(nodePgClient);
    const result = await nodePgConnection.transaction(async tx => {
      const result = await tx.query('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]);
      return result;
    });

    for await (const row of result) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }
  });

  test('can use the underlying wrappedTransaction', async ({expect}) => {
    const newUser = getRandomUser();

    await nodePgClient.query(
      `
      INSERT INTO "user" (id, name, status) VALUES ($1, $2, $3)
    `,
      [newUser.id, newUser.name, newUser.status],
    );

    const nodePgConnection = new NodePgConnection(nodePgClient);
    const result = await nodePgConnection.transaction(tx =>
      tx.wrappedTransaction.query('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]),
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]?.name).toBe(newUser.name);
    expect(result.rows[0]?.id).toBe(newUser.id);
  });
});

describe('postgres-js', () => {
  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await postgresJsClient`
      INSERT INTO "user" (id, name, status) VALUES (${newUser.id}, ${newUser.name}, ${newUser.status})
    `;

    const zql = zeroPostgresJS(schema, postgresJsClient);

    const result = await zql.transaction(
      async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);
        return result;
      },
      {
        upstreamSchema: '',
        clientGroupID: '',
        clientID: '',
        mutationID: 0,
      },
    );

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('can query from the database in a transaction', async ({expect}) => {
    const newUser = getRandomUser();

    await postgresJsClient`
      INSERT INTO "user" (id, name, status) VALUES (${newUser.id}, ${newUser.name}, ${newUser.status})
    `;

    const postgresJsConnection = new PostgresJSConnection(postgresJsClient);
    const result = await postgresJsConnection.transaction(async tx => {
      const result = await tx.query('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]);
      return result;
    });

    for await (const row of result) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }
  });

  test('can use the underlying wrappedTransaction', async ({expect}) => {
    const newUser = getRandomUser();

    await postgresJsClient`
      INSERT INTO "user" (id, name, status) VALUES (${newUser.id}, ${newUser.name}, ${newUser.status})
    `;

    const postgresJsConnection = new PostgresJSConnection(postgresJsClient);
    const result = await postgresJsConnection.transaction(tx =>
      tx.wrappedTransaction.unsafe('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]),
    );

    expect(result).toHaveLength(1);
    expect(result[0]?.name).toBe(newUser.name);
    expect(result[0]?.id).toBe(newUser.id);
  });
});

describe('drizzle-node-postgres', () => {
  let drizzleNodePgClient: ReturnType<
    typeof drizzleNodePg<typeof drizzleSchema>
  >;

  beforeEach(() => {
    drizzleNodePgClient = drizzleNodePg(nodePgClient, {
      schema: drizzleSchema,
    });
  });

  test('types - implicit schema generic', () => {
    const s = null as unknown as NodePgDrizzleTransaction<
      typeof drizzleNodePgClient
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

  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzleNodePgClient.insert(drizzleSchema.user).values(newUser);

    const zql = zeroDrizzleNodePg(schema, drizzleNodePgClient);

    const result = await zql.transaction(
      async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);
        return result;
      },
      {
        upstreamSchema: '',
        clientGroupID: '',
        clientID: '',
        mutationID: 0,
      },
    );

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('can query from the database', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzleNodePgClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new NodePgDrizzleConnection(drizzleNodePgClient);
    const result = await drizzleConnection.query(
      'SELECT * FROM "user" WHERE id = $1',
      [newUser.id],
    );
    expect(result[0]?.name).toBe(newUser.name);
    expect(result[0]?.id).toBe(newUser.id);
  });

  test('can query from the database in a transaction', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzleNodePgClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new NodePgDrizzleConnection(drizzleNodePgClient);
    const result = await drizzleConnection.transaction(async tx => {
      const result = await tx.query('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]);
      return result;
    });

    for await (const row of result) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }
  });

  test('can use the underlying wrappedTransaction', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzleNodePgClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new NodePgDrizzleConnection(drizzleNodePgClient);
    const result = await drizzleConnection.transaction(tx =>
      tx.wrappedTransaction.query.user.findFirst({
        where: eq(drizzleSchema.user.id, newUser.id),
      }),
    );

    expect(result?.name).toBe(newUser.name);
    expect(result?.id).toBe(newUser.id);
  });
});

describe('drizzle-postgres-js', () => {
  let drizzlePostgresJsClient: ReturnType<
    typeof drizzlePostgresJs<typeof drizzleSchema>
  >;

  beforeEach(() => {
    drizzlePostgresJsClient = drizzlePostgresJs(postgresJsClient, {
      schema: drizzleSchema,
    });
  });

  test('zql', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzlePostgresJsClient.insert(drizzleSchema.user).values(newUser);

    const zql = zeroDrizzlePostgresJS(schema, drizzlePostgresJsClient);

    const result = await zql.transaction(
      async tx => {
        const result = await tx.query.user.where('id', '=', newUser.id);
        return result;
      },
      {
        upstreamSchema: '',
        clientGroupID: '',
        clientID: '',
        mutationID: 0,
      },
    );

    expect(result[0]?.name).toEqual(newUser.name);
    expect(result[0]?.id).toEqual(newUser.id);
  });

  test('types - implicit schema generic', () => {
    const s = null as unknown as PostgresJsDrizzleTransaction<
      typeof drizzlePostgresJsClient
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

  test('can query from the database', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzlePostgresJsClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new PostgresJsDrizzleConnection(
      drizzlePostgresJsClient,
    );
    const result = await drizzleConnection.query(
      'SELECT * FROM "user" WHERE id = $1',
      [newUser.id],
    );
    expect(result[0]?.name).toBe(newUser.name);
    expect(result[0]?.id).toBe(newUser.id);
  });

  test('can query from the database in a transaction', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzlePostgresJsClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new PostgresJsDrizzleConnection(
      drizzlePostgresJsClient,
    );
    const result = await drizzleConnection.transaction(async tx => {
      const result = await tx.query('SELECT * FROM "user" WHERE id = $1', [
        newUser.id,
      ]);
      return result;
    });

    for await (const row of result) {
      expect(row.name).toBe(newUser.name);
      expect(row.id).toBe(newUser.id);
    }
  });

  test('can use the underlying wrappedTransaction', async ({expect}) => {
    const newUser = getRandomUser();

    await drizzlePostgresJsClient.insert(drizzleSchema.user).values(newUser);

    const drizzleConnection = new PostgresJsDrizzleConnection(
      drizzlePostgresJsClient,
    );
    const result = await drizzleConnection.transaction(tx =>
      tx.wrappedTransaction.query.user.findFirst({
        where: eq(drizzleSchema.user.id, newUser.id),
      }),
    );

    expect(result?.name).toBe(newUser.name);
    expect(result?.id).toBe(newUser.id);
  });
});
