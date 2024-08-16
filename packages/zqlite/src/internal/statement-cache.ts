import type {Database} from 'better-sqlite3';
import type {Statement} from 'better-sqlite3';
import {assert} from 'shared/src/asserts.js';

export type CachedStatement = {
  sql: string;
  statement: Statement;
};

type Entry = CachedStatement & {
  prev?: Entry | undefined;
  next?: Entry | undefined;
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
  #head?: Entry | undefined;
  #tail?: Entry | undefined;
  readonly #db: Database;
  #size = 0;

  /**
   * The db connection used to prepare the statement.
   * It is an error to use a statement prepared on one connection with another connection.
   * @param db
   */
  constructor(db: Database) {
    this.#db = db;
  }

  get size() {
    return this.#size;
  }

  drop(n: number) {
    assert(n >= 0, 'Cannot drop a negative number of items');
    assert(n <= this.#size, 'Cannot drop more items than are in the cache');
    if (n === this.#size) {
      this.#head = undefined;
      this.#tail = undefined;
      this.#size = 0;
      return;
    }

    let entry = this.#tail;
    const originalN = n;
    while (entry && n > 0) {
      if (!entry.next) {
        break;
      }
      entry = entry.next;
      --n;
    }
    assert(entry, 'Malformed list');

    entry.prev!.next = undefined;
    entry.prev = undefined;
    this.#size -= originalN;
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
    let entry = this.#head;
    while (entry) {
      if (entry.sql === sql) {
        const {sql, statement} = entry;
        if (entry === this.#head) {
          this.#head = entry.prev;
        }
        if (entry === this.#tail) {
          this.#tail = entry.next;
        }
        unlink(entry);
        --this.#size;
        return {sql, statement};
      }
      entry = entry.prev;
    }
    const statement = this.#db.prepare(sql);
    return {sql, statement};
  }

  /**
   * Handles `get` and `return` for the caller by invoking them before
   * and after the callback.
   */
  use(sql: string, cb: (statement: CachedStatement) => void) {
    const statement = this.get(sql);
    try {
      cb(statement);
    } finally {
      this.return(statement);
    }
  }

  /**
   * Add a statement back to the cache so someone else can use it later.
   * @param statement
   */
  return(statement: CachedStatement) {
    const entry: Entry = {
      sql: statement.sql,
      statement: statement.statement,
      prev: this.#head,
    };
    if (this.#head) {
      this.#head.next = entry;
    }
    if (!this.#tail) {
      this.#tail = entry;
    }
    this.#head = entry;
    ++this.#size;
  }
}

function unlink(entry: Entry) {
  if (entry.prev) {
    entry.prev.next = entry.next;
  }
  if (entry.next) {
    entry.next.prev = entry.prev;
  }
  entry.prev = undefined;
  entry.next = undefined;
}

function normalizeWhitespace(sql: string) {
  return sql.replaceAll(/\s+/g, ' ');
}
