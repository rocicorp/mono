/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {RunResult} from '@rocicorp/zero-sqlite3';
import {Database} from '../../../zqlite/src/db.ts';
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  get(sql: string, ...args: unknown[]): any {
    return this.statementCache.use(sql, cached =>
      cached.statement.get(...args),
    );
  }

  /**
   * Prepares a statement (or retrieves it from the cache) and returns
   * all of its results.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
