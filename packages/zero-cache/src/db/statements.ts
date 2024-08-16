import {Database, RunResult, Statement} from 'better-sqlite3';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Stmt = Statement<any[]>;

export interface StatementPreparer {
  // Note: Also implemented by the `Database` interface itself.
  prepare(source: string): Stmt;
}

export class StatementCachingDatabase implements StatementPreparer {
  readonly db: Database;
  readonly #cache: Map<string, Stmt> = new Map();

  constructor(db: Database) {
    this.db = db;
  }

  prepare(source: string): Stmt {
    let stmt = this.#cache.get(source);
    if (!stmt) {
      stmt = this.db.prepare(source);
      this.#cache.set(source, stmt);
    }
    return stmt;
  }

  /**
   * Convenience method for preparing a statement (or retrieving it
   * from the cache) and running it.
   */
  run(staticSource: string, ...args: unknown[]): RunResult {
    return this.prepare(staticSource).run(...args);
  }

  // Syntactic sugar methods
  beginConcurrent(): RunResult {
    return this.run('BEGIN CONCURRENT');
  }

  commit(): RunResult {
    return this.run('COMMIT');
  }

  rollback(): RunResult {
    return this.run('ROLLBACK');
  }
}
