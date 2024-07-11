import Database from 'better-sqlite3';

export class DB {
  readonly transaction: Database.Database['transaction'];
  readonly prepare: Database.Database['prepare'];
  readonly #db: Database.Database;
  readonly #beginStmt: Database.Statement;
  readonly #commitStmt: Database.Statement;
  readonly #rollbackStmt: Database.Statement;

  constructor(sqliteDbPath: string) {
    this.#db = new Database(sqliteDbPath);
    this.#db.pragma('journal_mode = WAL');
    this.#db.pragma('synchronous = NORMAL');
    this.transaction = this.#db.transaction.bind(this.#db);
    this.prepare = this.#db.prepare.bind(this.#db);
    DB.ensureSchema(this.#db);

    this.#beginStmt = this.#db.prepare('BEGIN');
    this.#commitStmt = this.#db.prepare('COMMIT');
    this.#rollbackStmt = this.#db.prepare('ROLLBACK');
  }

  static ensureSchema(db: Database.Database) {
    db.exec(/*sql*/ `CREATE TABLE IF NOT EXISTS _zero_metadata (
        key TEXT PRIMARY KEY,
        value TEXT
      );`);
  }

  beginImperativeTransaction() {
    this.#beginStmt.run();
  }

  commitImperativeTransaction() {
    this.#commitStmt.run();
  }

  rollbackImperativeTransaction() {
    this.#rollbackStmt.run();
  }
}

export const queries = {
  setCommittedLsn: /*sql*/ `INSERT OR REPLACE INTO _zero_metadata (key, value) VALUES ('committed-lsn', ?);`,
  setIvmLsn: /*sql*/ `INSERT OR REPLACE INTO _zero_metadata (key, value) VALUES ('ivm-lsn', ?);`,
  getCommittedLsn: /*sql*/ `SELECT value FROM _zero_metadata WHERE key = 'committed-lsn';`,
  getIvmLsn: /*sql*/ `SELECT value FROM _zero_metadata WHERE key = 'ivm-lsn';`,
};
