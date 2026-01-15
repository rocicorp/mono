import type {RunResult} from '@rocicorp/zero-sqlite3';
import type {Database} from '../../../zqlite/src/db.ts';
import {StatementCache} from '../../../zqlite/src/internal/statement-cache.ts';

/**
 * A stateless wrapper around a {@link StatementCache} that facilitates single-line
 * `printf()` style invocations of cached prepared statement operations.
 */
export class StatementRunner {
  readonly db: Database;
  readonly statementCache: StatementCache;

  constructor(db: Database) {
    this.db = db;
    this.statementCache = new StatementCache(db);
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and runs it
   * with the given args.
   */
  run(sql: string, ...args: unknown[]): RunResult {
    return this.statementCache.use(sql, cached =>
      cached.statement.run(...args),
    );
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and returns
   * the first result.
   */
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  get(sql: string, ...args: unknown[]): any {
    return this.statementCache.use(sql, cached =>
      cached.statement.get(...args),
    );
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and returns
   * all of its results.
   */
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  all(sql: string, ...args: unknown[]): any[] {
    return this.statementCache.use(sql, cached =>
      cached.statement.all(...args),
    );
  }

  // Syntactic sugar methods
  begin(): RunResult {
    return this.run('BEGIN');
  }

  beginConcurrent(): RunResult {
    return this.run('BEGIN CONCURRENT');
  }

  beginImmediate(): RunResult {
    return this.run('BEGIN IMMEDIATE');
  }

  commit(): RunResult {
    return this.run('COMMIT');
  }

  rollback(): RunResult {
    return this.run('ROLLBACK');
  }
}
