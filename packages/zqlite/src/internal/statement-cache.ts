import type {Database} from 'better-sqlite3';
import type {Statement} from 'better-sqlite3';

export class StatementCache {
  readonly #cache = new Map<string, [statement: Statement, inUse: boolean]>();
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
      const stmt = this.#prepare(sql);
      entry = [stmt, false];
      this.#cache.set(sql, entry);
    }
    if (entry[1]) {
      throw new Error('Statement in use!');
    }
    entry[1] = true;
    return entry[0];
  }

  return(sql: string): void {
    const entry = this.#cache.get(sql);
    if (!entry) {
      throw new Error('Statement not found!');
    }
    if (!entry[1]) {
      throw new Error('Statement not in use!');
    }
    entry[1] = false;
  }

  clear() {
    this.#cache.clear();
  }

  #prepare(sql: string): Statement {
    return this.#db.prepare(sql);
  }
}
