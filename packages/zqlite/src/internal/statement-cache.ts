import {assert} from '../../../shared/src/asserts.ts';
import type {Database, Statement} from '../db.ts';

export type CachedStatementMap = Map<string, Statement[]>;
export type CachedStatement = {
  sql: string;
  statement: Statement;
};

/**
 * The default maximum number of statements retained by a StatementCache.
 *
 * Each cached statement holds a native `sqlite3_stmt` handle (prepared
 * bytecode + SQLite-side memory), so the cache must be bounded. The bound
 * needs enough headroom for callers with a large working set of distinct
 * SQL — e.g. the replication change-processor prepares several statements
 * per table (with additional variants for partial-column updates) through
 * a single cache — while still capping growth from unbounded key spaces
 * such as client-influenced query shapes.
 */
export const DEFAULT_MAX_CACHED_STATEMENTS = 1_000;

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
 *
 * The cache is bounded: it retains at most `maxSize` statements, evicting
 * the least recently used statements when the bound is exceeded. Only idle
 * (returned) statements are ever evicted — statements checked out via `get`
 * are not in the cache and thus never touched by eviction. Evicting a
 * statement drops the cache's reference to it; the native `sqlite3_stmt`
 * handle is finalized when the Statement object is garbage collected.
 */
export class StatementCache {
  // Map insertion order doubles as recency order: entries are re-inserted
  // on every `get` hit and `return`, so the first entry in the map is the
  // least recently used. Within an entry's array, the first statement is
  // the least recently returned.
  #cache: CachedStatementMap = new Map<string, Statement[]>();
  readonly #db: Database;
  readonly #maxSize: number;
  #size: number = 0;

  /**
   * The db connection used to prepare the statement.
   * It is an error to use a statement prepared on one connection with another connection.
   * @param db
   * @param maxSize the maximum number of statements to retain in the cache.
   */
  constructor(db: Database, maxSize: number = DEFAULT_MAX_CACHED_STATEMENTS) {
    assert(maxSize >= 0, 'maxSize must not be negative');
    this.#db = db;
    this.#maxSize = maxSize;
  }

  // the number of statements in the cache
  get size() {
    return this.#size;
  }

  // the maximum number of statements the cache will retain
  get maxSize() {
    return this.#maxSize;
  }

  drop(n: number) {
    assert(n >= 0, 'Cannot drop a negative number of items');
    assert(n <= this.#size, 'Cannot drop more items than are in the cache');

    for (let i = 0; i < n; i++) {
      this.#evictLeastRecentlyUsed();
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
      this.#cache.delete(sql);
      if (statements.length > 0) {
        // Re-insert so the remaining statements for this sql are marked
        // as most recently used.
        this.#cache.set(sql, statements);
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
   *
   * If this pushes the cache over its maximum size, the least recently
   * used statements are evicted.
   * @param statement
   */
  return(statement: CachedStatement) {
    const {sql} = statement;
    let statements = this.#cache.get(sql);
    if (statements === undefined) {
      statements = [];
    } else {
      // Delete so the re-insert below moves this sql to the most recently
      // used position.
      this.#cache.delete(sql);
    }
    statements.push(statement.statement);
    this.#cache.set(sql, statements);
    this.#size++;

    while (this.#size > this.#maxSize) {
      this.#evictLeastRecentlyUsed();
    }
  }

  #evictLeastRecentlyUsed() {
    const lru = this.#cache.entries().next();
    assert(!lru.done, 'Cannot evict from an empty cache');
    const [sql, statements] = lru.value;
    statements.shift();
    this.#size--;
    if (statements.length === 0) {
      this.#cache.delete(sql);
    }
  }
}

function normalizeWhitespace(sql: string) {
  return sql.replaceAll(/\s+/g, ' ');
}
