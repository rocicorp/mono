import {Database, RunResult, Statement} from 'better-sqlite3';

type Stmt = Statement<unknown[]>;

/**
 * A Statement-caching wrapper for statements that are only executed
 * synchronously and thus do not have re-entrancy issues.
 *
 * Specifically, this class does not provide an API for iteration of
 * prepared statements, since that would be subject to re-entrancy.
 */
export class StatementRunner {
  readonly db: Database;
  readonly #cache: Map<string, Stmt> = new Map();

  constructor(db: Database) {
    this.db = db;
  }

  get size() {
    return this.#cache.size;
  }

  #prepare(source: string): Stmt {
    let stmt = this.#cache.get(source);
    if (!stmt) {
      stmt = this.db.prepare(source);
      this.#cache.set(source, stmt);
    }
    return stmt;
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and runs it
   * with the given args.
   */
  run(staticSource: string, ...args: unknown[]): RunResult {
    return this.#prepare(staticSource).run(...args);
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and returns
   * the first result.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  get(staticSource: string, ...args: unknown[]): any {
    return this.#prepare(staticSource).get(...args);
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and returns
   * all of its results.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  all(staticSource: string, ...args: unknown[]): any[] {
    return this.#prepare(staticSource).all(...args);
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
