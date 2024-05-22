import postgres from 'postgres';
import {assert, assertString} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {afterAll, expect} from 'vitest';
import {PostgresDB, postgresTypeConfig} from '../types/pg.js';

// Set by ./test/pg-container-setup.ts
assertString(process.env.PG_CONTAINER_CONNECTION_URI);
const CONNECTION_URI: string = process.env.PG_CONTAINER_CONNECTION_URI;
assert(
  CONNECTION_URI.length > 0,
  'pg-container-setup.ts must be executed to setup the PG_CONTAINER_CONNECTION_URI',
);

class TestDBs {
  readonly #sql = postgres(CONNECTION_URI, {
    onnotice: () => {},
    ...postgresTypeConfig(),
  });
  readonly #dbs: Record<string, postgres.Sql> = {};

  async create(database: string): Promise<PostgresDB> {
    assert(!(database in this.#dbs), `${database} has already been created`);

    const sql = this.#sql;
    await sql`DROP DATABASE IF EXISTS ${sql(database)} WITH (FORCE)`;
    await sql`CREATE DATABASE ${sql(database)}`;

    const {host, port, user: username, pass} = sql.options;
    const db = postgres({
      host: host[0],
      port: port[0],
      username,
      password: pass ?? undefined,
      database,
      onnotice: () => {},
      ...postgresTypeConfig(),
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
    const sql = this.#sql;
    if (sql) {
      await sql`DROP DATABASE IF EXISTS ${sql(database)} WITH (FORCE)`;
    }

    delete this.#dbs[database];
  }

  /**
   * This automatically is called on the exported `testDBs` instance
   * in the `afterAll()` hook in this file, so there is no need to call
   * it manually.
   */
  async end() {
    await this.#sql.end();
  }
}

export const testDBs = new TestDBs();

afterAll(async () => {
  await testDBs.end();
});

/**
 * Constructs a `postgres://` uri for connecting to the specified `db`.
 * @param scope `external` for a connection uri from outside of the Testcontainer,
 *               and `internal` for a connection uri between databases
 *               running within the Testcontainer (e.g. pg-to-pg replication).
 * @returns
 */
export function getConnectionURI(
  db: postgres.Sql,
  scope: 'internal' | 'external' = 'internal',
) {
  const {user, pass, host, port, database} = db.options;
  return scope === 'external'
    ? `postgres://${user}:${pass}@${host}:${port}/${database}`
    : // The internal uri is used for communication between databases
      // running in the testcontainer, and thus omits the exported port.
      // Instead, the default postgres port is used for intra-container connections.
      `postgres://${user}:${pass}@${host}/${database}`;
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

export async function dropReplicationSlot(db: postgres.Sql, slotName: string) {
  // A replication slot can't be dropped when it is still marked "active" on the upstream
  // database. The slot becomes inactive when the downstream connection is closed (e.g. the
  // initial-sync SUBSCRIPTION is disabled, or the incremental-sync connection is closed),
  // but because this is a non-transactional process that happens in the internals of Postgres,
  // we have to poll the status and wait for the slot to be released.
  for (let i = 0; i < 100; i++) {
    const results = await db<{slotName: string; active: boolean}[]>`
    SELECT slot_name as "slotName", active FROM pg_replication_slots WHERE slot_name = ${slotName}`;

    if (results.count === 0) {
      break;
    }
    const result = results[0];
    if (!result.active) {
      await db`SELECT pg_drop_replication_slot(${slotName})`;
      break;
    }
    await sleep(10);
  }
}
