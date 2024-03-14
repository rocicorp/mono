import postgres from 'postgres';
import {assert} from 'shared/src/asserts.js';

export class TestDBs {
  // Connects to the main "postgres" DB of the local Postgres cluster.
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

  async drop(db: postgres.Sql) {
    const {database} = db.options;
    await db.end();
    await this.#sql`
    DROP DATABASE IF EXISTS ${this.#sql(database)} WITH (FORCE)`;

    delete this.#dbs[database];
  }

  async end() {
    await Promise.all([...Object.values(this.#dbs)].map(db => this.drop(db)));
    return this.#sql.end();
  }
}
