import type {Database} from 'better-sqlite3';
import type {Statement} from 'better-sqlite3';

export class StatementCache {
  readonly #cache = new Map<
    string,
    {
      statement: Statement;
      inUse: boolean;
    }
  >();
  readonly #db: Database;

  constructor(db: Database) {
    this.#db = db;
  }

  get db(): Database {
    return this.#db;
  }

  get(sql: string): Statement {
    let entry = this.#cache.get(sql);
    if (!entry) {
      const statement = this.#prepare(sql);
      entry = {
        statement,
        inUse: false,
      };
      this.#cache.set(sql, entry);
    }
    if (entry.inUse) {
      throw new Error('Statement in use!');
    }
    entry.inUse = true;
    return entry.statement;
  }

  return(sql: string): void {
    const entry = this.#cache.get(sql);
    if (!entry) {
      throw new Error('Statement not found!');
    }
    if (!entry.inUse) {
      throw new Error('Statement not in use!');
    }
    entry.inUse = false;
  }

  #prepare(sql: string): Statement {
    return this.#db.prepare(sql);
  }
}
