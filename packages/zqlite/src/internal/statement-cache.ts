import {assert} from '../../../shared/src/asserts.ts';
import type {Database, Statement} from '../db.ts';

export type CachedStatementMap = Map<string, Statement[]>;
export type CachedStatement = {
  sql: string;
  statement: Statement;
};
/**
 * SQLite statement preparation isn't cheap as it involves evaluating possible
 * query plans and picking the best one (in addition to parsing the SQL).
 *
 * This statement cache prevents the need to re-prepare the same statement
 * multiple times.
 *
 * One extra wrinkle is that a single statement cannot be used by multiple
 * callers at the same time. As in, we can't `iterate` the same statement
 * many times concurrently.
 *
 * Given that, statements are removed from the cache while in use.
 * - `get` removes the statement from the cache
 * - `return` adds it back.
 *
 * If a request for the same sql is made while a
 * statement is gotten, a new statement will be prepared.
 * Both statements can be returned to the cache even though they both
 * serve the same SQL. Having both copies returned to the cache allows
 * the cache to serve multiple callers concurrently in the future.
 *
 * It is not an error to fail to call `return` on a statement.
 * Failing to call return will only prevent the statement from being reused
 * by other callers. It will not cause a resource leak.
 */
export class StatementCache {
  #cache: CachedStatementMap = new Map<string, Statement[]>();
  readonly #db: Database;
  #size: number = 0;

  /**
   * The db connection used to prepare the statement.
   * It is an error to use a statement prepared on one connection with another connection.
   * @param db
   */
  constructor(db: Database) {
    this.#db = db;
  }

  // the number of statements in the cache
  get size() {
    return this.#size;
  }

  // the database connection used to prepare statements
  get db() {
    return this.#db;
  }

  drop(n: number) {
    assert(n >= 0, 'Cannot drop a negative number of items');
    assert(n <= this.#size, 'Cannot drop more items than are in the cache');

    let remaining = n;
    for (const [sql, statements] of this.#cache.entries()) {
      if (remaining >= statements.length) {
        this.#cache.delete(sql);
        remaining -= statements.length;
        this.#size -= statements.length;
      } else {
        statements.splice(0, remaining);
        this.#size -= remaining;
        break;
      }
    }
  }

  /**
   * Prepares a statement for the given sql unless one is already cached.
   * If one is cached, it is removed from the cache and returned.
   *
   * Since `get` removes the item from the cache it is not an error to fail to call
   * `return`. The gotten statement will be correctly garbage collected.
   *
   * When a gotten statement is not returned, future calls to
   * `get` with the same `sql` will prepare a new statement.
   *
   * @param sql
   * @returns
   */
  get(sql: string): CachedStatement {
    sql = normalizeWhitespace(sql);
    const statements = this.#cache.get(sql);
    if (statements && statements.length > 0) {
      const statement = statements.pop()!;
      this.#size--;
      if (statements.length === 0) {
        this.#cache.delete(sql);
      }
      return {sql, statement};
    }
    const statement = this.#db.prepare(sql);
    return {sql, statement};
  }

  /**
   * Handles `get` and `return` for the caller by invoking them before
   * and after the callback.
   */
  use<T>(sql: string, cb: (statement: CachedStatement) => T) {
    const statement = this.get(sql);
    try {
      return cb(statement);
    } finally {
      this.return(statement);
    }
  }

  /**
   * Add a statement back to the cache so someone else can use it later.
   * @param statement
   */
  return(statement: CachedStatement) {
    const {sql} = statement;
    if (!this.#cache.has(sql)) {
      this.#cache.set(sql, []);
    }
    const statements = this.#cache.get(sql);
    if (statements) {
      statements.push(statement.statement);
      this.#size++;
    }
  }
}

function normalizeWhitespace(sql: string) {
  return sql.replaceAll(/\s+/g, ' ');
}
