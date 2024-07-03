import type {Database, Statement} from 'better-sqlite3';
import {StatementCache} from './statement-cache.js';

export class DB {
  readonly #stmtCache: StatementCache;
  readonly transaction: Database['transaction'];

  constructor(db: Database) {
    this.#stmtCache = new StatementCache(db);
    this.transaction = db.transaction.bind(db);
  }

  getStmt(sql: string): Statement {
    return this.#stmtCache.get(sql);
  }

  returnStmt(sql: string) {
    this.#stmtCache.return(sql);
  }
}
