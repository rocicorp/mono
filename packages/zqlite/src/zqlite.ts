import {Materialite} from 'zql/src/zql/ivm/materialite.js';
import type {Database} from 'better-sqlite3';

export class ZQLite extends Materialite {
  readonly #beginStmt;
  readonly #commitStmt;
  readonly #rollbackStmt;

  constructor(db: Database) {
    super();
    this.#beginStmt = db.prepare('BEGIN');
    this.#commitStmt = db.prepare('COMMIT');
    this.#rollbackStmt = db.prepare('ROLLBACK');
  }

  protected _txBegin(): void {
    this.#beginStmt.run();
  }

  protected _txCommit(): void {
    this.#commitStmt.run();
  }

  protected _txRollback(): void {
    this.#rollbackStmt.run();
  }
}
