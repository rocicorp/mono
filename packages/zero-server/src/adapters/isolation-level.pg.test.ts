import {PrismaPg} from '@prisma/adapter-pg';
import {PrismaClient} from '@prisma/client';
import {drizzle} from 'drizzle-orm/node-postgres';
import {Kysely, PostgresDialect} from 'kysely';
import pg from 'pg';
import postgres from 'postgres';
import {describe, expect} from 'vitest';
import {
  getConnectionURI,
  test,
  type PgTest,
} from '../../../zero-cache/src/test/db.ts';
import type {DBConnection, Row} from '../../../zql/src/mutate/custom.ts';
import type {IsolationLevel} from '../transaction-options.ts';
import {DrizzleConnection} from './drizzle.ts';
import {KyselyConnection} from './kysely.ts';
import {NodePgConnection} from './pg.ts';
import {PostgresJSConnection} from './postgresjs.ts';
import {PrismaConnection} from './prisma.ts';

// Every connection below is the only one its pool will open, so the statement
// after the transaction runs on the same backend as the transaction did. That
// is the property a session-level `SET SESSION CHARACTERISTICS` lacks: through
// a transaction-mode pooler the backend is shared with other clients, and the
// level it sets stays on it after the mutation.

const LEVEL_AND_PID = `SELECT current_setting('transaction_isolation') AS level,
  pg_backend_pid() AS pid`;

// Every adapter here implements `query`, the statement outside a transaction.
type Connection = Pick<DBConnection<unknown>, 'transaction'> & {
  query: NonNullable<DBConnection<unknown>['query']>;
};

type Opened = {
  connection: Connection;
  close: () => Promise<void>;
};

type Adapter = {
  name: string;
  open: (uri: string, isolationLevel: IsolationLevel | undefined) => Opened;
};

const options = (isolationLevel: IsolationLevel | undefined) =>
  isolationLevel === undefined ? {} : {isolationLevel};

const adapters: Adapter[] = [
  {
    name: 'kysely',
    open: (uri, isolationLevel) => {
      const db = new Kysely<unknown>({
        dialect: new PostgresDialect({
          pool: new pg.Pool({connectionString: uri, max: 1}),
        }),
      });
      return {
        connection: new KyselyConnection(db, options(isolationLevel)),
        close: () => db.destroy(),
      };
    },
  },
  {
    name: 'node-postgres',
    open: (uri, isolationLevel) => {
      const pool = new pg.Pool({connectionString: uri, max: 1});
      return {
        connection: new NodePgConnection(pool, options(isolationLevel)),
        close: () => pool.end(),
      };
    },
  },
  {
    name: 'postgres.js',
    open: (uri, isolationLevel) => {
      const sql = postgres(uri, {max: 1});
      return {
        connection: new PostgresJSConnection(sql, options(isolationLevel)),
        close: () => sql.end(),
      };
    },
  },
  {
    name: 'drizzle',
    open: (uri, isolationLevel) => {
      const pool = new pg.Pool({connectionString: uri, max: 1});
      return {
        connection: new DrizzleConnection(
          drizzle(pool),
          options(isolationLevel),
        ) as Connection,
        close: () => pool.end(),
      };
    },
  },
  {
    name: 'prisma',
    open: (uri, isolationLevel) => {
      const client = new PrismaClient({
        adapter: new PrismaPg({connectionString: uri, max: 1}),
      });
      return {
        connection: new PrismaConnection(client, options(isolationLevel)),
        close: () => client.$disconnect(),
      };
    },
  },
];

async function levelAndPID(
  run: (sql: string, params: unknown[]) => Promise<Iterable<Row>>,
) {
  const [row] = [...(await run(LEVEL_AND_PID, []))];
  return {level: row.level as string, pid: Number(row.pid)};
}

describe.each(adapters)('$name isolation level', ({open}) => {
  async function withConnection<T>(
    testDBs: PgTest['testDBs'],
    isolationLevel: IsolationLevel | undefined,
    fn: (connection: Connection) => Promise<T>,
  ): Promise<T> {
    await using upstream = await testDBs.create('zero_server_isolation_level');
    const {connection, close} = open(
      getConnectionURI(upstream),
      isolationLevel,
    );
    try {
      return await fn(connection);
    } finally {
      await close();
    }
  }

  test('runs the transaction at the database default when unset', async ({
    testDBs,
  }) => {
    const inside = await withConnection(testDBs, undefined, connection =>
      connection.transaction(tx =>
        levelAndPID((sql, params) => tx.query(sql, params)),
      ),
    );
    expect(inside.level).toBe('read committed');
  });

  test.for(['repeatable read', 'serializable'] as const)(
    'runs the transaction at %s and leaves the session at the default',
    async (isolationLevel, {testDBs}) => {
      const [inside, after] = await withConnection(
        testDBs,
        isolationLevel,
        async connection => {
          const inside = await connection.transaction(tx =>
            levelAndPID((sql, params) => tx.query(sql, params)),
          );
          // Same backend, next statement: nothing was left on the session.
          const after = await levelAndPID((sql, params) =>
            connection.query(sql, params),
          );
          return [inside, after] as const;
        },
      );
      expect(inside.level).toBe(isolationLevel);
      expect(after.pid).toBe(inside.pid);
      expect(after.level).toBe('read committed');
    },
  );
});
