/* oxlint-disable no-console */
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {
  afterAll,
  test as baseTest,
  expect,
  inject,
  type ProvidedContext,
} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {
  type PostgresDB,
  postgresTypeConfig,
  type TypeOptions,
} from '../types/pg.ts';

declare module 'vitest' {
  export interface ProvidedContext {
    pgConnectionString: string;
    pgImage: string;
    pgTimezone: string;
  }
}

function mustInject<K extends keyof ProvidedContext>(key: K) {
  return must(
    inject(key),
    'test file must have suffix ".pg.test.ts" to setup postgres container',
  );
}

// Set by ./test/pg-container-setup.ts
const CONNECTION_URI = mustInject('pgConnectionString');

export type OnNoticeFn = (n: postgres.Notice) => void;

const IGNORE_LEVELS = new Set(['DEBUG', 'INFO', 'NOTICE']);

const defaultOnNotice: OnNoticeFn = n => {
  if (!IGNORE_LEVELS.has(n.severity)) {
    console.log(n);
  }
};

export class TestDBs {
  readonly sql: PostgresDB;
  readonly #dbs: Record<string, postgres.Sql> = {};

  constructor(connectionURI = CONNECTION_URI) {
    this.sql = postgres(connectionURI, {
      onnotice: n => n.severity !== 'NOTICE' && console.log(n),
      ...postgresTypeConfig(),
    });
  }

  async create(
    database: string,
    onNotice?: OnNoticeFn,
    typeOpts: TypeOptions | false = {},
  ): Promise<PostgresDB> {
    const exists = this.#dbs[database];
    if (exists !== undefined) {
      console.warn('dropping database', database);
      await this.#drop(exists);
    }

    const {sql} = this;
    await sql`DROP DATABASE IF EXISTS ${sql(database)}`;
    await sql`CREATE DATABASE ${sql(database)}`;

    const {host, port, user: username, pass} = sql.options;
    const db = postgres({
      host: host[0],
      port: port[0],
      username,
      password: pass ?? undefined,
      database,
      onnotice: n => {
        onNotice?.(n);
        defaultOnNotice(n);
      },
      ...(typeOpts ? postgresTypeConfig(typeOpts) : {}),
    });
    this.#dbs[database] = db;
    return db;
  }

  async drop(...dbs: postgres.Sql[]) {
    await Promise.all(dbs.map(db => this.#drop(db)));
  }

  async #drop(db: postgres.Sql) {
    const {database} = db.options;
    await db.end();

    for (let i = 0; i < 10; i++) {
      const {sql} = this;
      await dropReplicationSlotsFor(sql, database);
      try {
        await sql`DROP DATABASE IF EXISTS ${sql(database)} WITH (FORCE)`;
        break;
      } catch (e) {
        // Sometimes the replication slot isn't immediately dropped, which
        // causes the DROP DATABASE command to fail as well. Log a warning
        // but continue to allow subsequent tests to proceed (provided that
        // they are using unique database and replication slot names).
        console.warn(`Unable to drop database ${database}`, e);
      }
      await sleep(50);
    }

    delete this.#dbs[database];
  }

  /**
   * This automatically is called on the exported `testDBs` instance
   * in the `afterAll()` hook in this file, so there is no need to call
   * it manually.
   */
  async end() {
    const undropped = Object.keys(this.#dbs);
    if (undropped.length) {
      console.warn('undropped databases', undropped);
      await this.drop(...Object.values(this.#dbs));
    }
    await this.sql.end();
  }
}

export const testDBs = new TestDBs();

afterAll(async () => {
  await testDBs.end();
});

/**
 * Constructs a `postgres://` uri for connecting to the specified `db`.
 */
export function getConnectionURI(db: postgres.Sql) {
  const {user, pass, host, port, database} = db.options;
  return `postgres://${user}:${pass}@${host}:${port}/${database}`;
}

export async function initDB(
  db: postgres.Sql,
  statements?: string,
  tables?: Record<string, object[]>,
) {
  await db.begin(async tx => {
    if (statements) {
      await db.unsafe(statements);
    }
    await Promise.all(
      Object.entries(tables ?? {}).map(
        ([table, existing]) => tx`INSERT INTO ${tx(table)} ${tx(existing)}`,
      ),
    );
  });
}

export async function expectTables(
  db: postgres.Sql,
  tables?: Record<string, unknown[]>,
) {
  for (const [table, expected] of Object.entries(tables ?? {})) {
    const actual = await db`SELECT * FROM ${db(table)}`;
    expect(actual).toEqual(expect.arrayContaining(expected));
    expect(expected).toEqual(expect.arrayContaining(actual));
  }
}

export async function expectTablesToMatch(
  db: postgres.Sql,
  tables?: Record<string, unknown[]>,
) {
  for (const [table, expected] of Object.entries(tables ?? {})) {
    const actual = await db`SELECT * FROM ${db(table)}`;
    expect(actual).toMatchObject(expected);
  }
}

export async function dropReplicationSlots(db: postgres.Sql): Promise<void> {
  const {database} = db.options;
  await dropReplicationSlotsFor(db, database);
}

async function dropReplicationSlotsFor(db: postgres.Sql, database: string) {
  for (let i = 0; i < 100; i++) {
    const results = await db<
      {slotName: string; active: boolean; pid: number | null}[]
    >`
    SELECT slot_name as "slotName", active, active_pid as pid
      FROM pg_replication_slots WHERE database = ${database}`;

    if (results.count === 0) {
      break;
    }
    for (const {slotName, active /*, pid */} of results) {
      if (active) {
        // A replication slot can't be dropped when it is still marked "active" on the upstream
        // database. The slot becomes inactive when the replication stream  is closed,
        // but because this is a non-transactional process that happens in the internals of Postgres,
        // sometimes it isn't immediate. Send a pg_terminate_backend() to move it along.
        // console.warn(`terminating backend ${pid} to release replication slot`);
        await db<{slotName: string; active: boolean}[]>`
          SELECT pg_terminate_backend(active_pid)
            FROM pg_replication_slots WHERE database = ${database} and active = true`;
        await sleep(50);
      } else {
        await db`SELECT pg_drop_replication_slot(${slotName})`;
      }
    }
  }
}

export const pgContainerTest = baseTest.extend<{pgConnectionString: string}>({
  pgConnectionString: [
    // vitest requires that the first argument inside a fixture use
    // object destructuring.
    // oxlint-disable-next-line no-empty-pattern
    async ({}, use) => {
      const container = await new PostgreSqlContainer(mustInject('pgImage'))
        .withCommand([
          'postgres',
          '-c',
          'wal_level=logical',
          '-c',
          `timezone=${mustInject('pgTimezone')}`,
        ])
        .start();
      const pgConnectionString = container.getConnectionUri();
      await use(pgConnectionString);
      await container.stop();
    },
    {scope: 'worker'},
  ],
});

export type PgTest = {testDBs: TestDBs};

/**
 * The test currently uses the global (i.e. per-project) pg container
 * that's run by `pg-container-setup.ts`. To switch to running a pg
 * container per worker, extend the `pgContainerTest` instead and use
 * its `pgConnectionString` fixture.
 */
export const test = baseTest.extend<PgTest>({
  testDBs: [
    // vitest requires that the first argument inside a fixture use
    // object destructuring.
    // oxlint-disable-next-line no-empty-pattern
    async ({}, use) => {
      const testDBs = new TestDBs(mustInject('pgConnectionString'));
      try {
        await use(testDBs);
      } finally {
        await testDBs.end();
      }
    },
    {scope: 'worker'},
  ],
});
