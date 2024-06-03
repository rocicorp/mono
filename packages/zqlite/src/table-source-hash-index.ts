import type {Primitive, Selector} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {HashIndex} from '@rocicorp/zql/src/zql/ivm/source/source-hash-index.js';
import type {PipelineEntity} from '@rocicorp/zql/src/zql/ivm/types.js';
import type {DB} from './internal/DB.js';

export class TableSourceHashIndex<K extends Primitive, T extends PipelineEntity>
  implements HashIndex<K, T>
{
  readonly #sql;
  readonly #db;

  constructor(db: DB, table: string, column: Selector) {
    this.#db = db;

    this.#sql = `SELECT * FROM "${table}" WHERE "${column[1]}" = ?`;
  }

  get(key: K): Iterable<T> | undefined {
    const stmt = this.#db.getStmt(this.#sql);
    try {
      const rows = stmt.all(key);
      return rows;
    } finally {
      this.#db.returnStmt(this.#sql);
    }
  }
}
