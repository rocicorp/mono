import {expect} from '@jest/globals';
import postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';

export class TestDBs {
  // Connects to the main "postgres" DB of the local Postgres cluster.
  //
  // Note: In order to run all of the tests successfully, the following
  // configuration needs to be set in postgresql.conf:
  //
  // wal_level = logical                    # default is replica
  // max_logical_replication_workers = 20   # default is 4
  readonly #sql = postgres({
    database: 'postgres',
    transform: postgres.camel,
    onnotice: () => {},
  });
  readonly #dbs: Record<string, postgres.Sql> = {};

  async create(database: string) {
    assert(!(database in this.#dbs), `${database} has already been created`);

    await this.#sql`
    DROP DATABASE IF EXISTS ${this.#sql(database)} WITH (FORCE)`;

    await this.#sql`
    CREATE DATABASE ${this.#sql(database)}`;

    const db = postgres({
      database,
      transform: postgres.camel,
      onnotice: () => {},
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
    await this.#sql`
    DROP DATABASE IF EXISTS ${this.#sql(database)} WITH (FORCE)`;

    delete this.#dbs[database];
  }

  async end() {
    await this.drop(...[...Object.values(this.#dbs)]);
    return this.#sql.end();
  }
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
  tables?: Record<string, object[]>,
) {
  for (const [table, expected] of Object.entries(tables ?? {})) {
    const actual = await db`SELECT * FROM ${db(table)}`;
    expect(actual).toEqual(expected);
  }
}
