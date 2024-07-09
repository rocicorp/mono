import type {Database} from 'better-sqlite3';

export class DB {
  readonly transaction: Database['transaction'];

  constructor(db: Database) {
    this.transaction = db.transaction.bind(db);
  }
}
