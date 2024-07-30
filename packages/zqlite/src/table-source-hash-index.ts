import type Database from 'better-sqlite3';
import type {Primitive, Selector} from 'zql/src/zql/ast/ast.js';
import type {HashIndex} from 'zql/src/zql/ivm/source/source-hash-index.js';
import type {PipelineEntity} from 'zql/src/zql/ivm/types.js';

export class TableSourceHashIndex<K extends Primitive, T extends PipelineEntity>
  implements HashIndex<K, T>
{
  readonly #statement: Database.Statement;

  constructor(db: Database.Database, table: string, column: Selector) {
    this.#statement = db.prepare(
      `SELECT * FROM "${table}" WHERE "${column[1]}" = ?`,
    );
  }

  get(key: K): Iterable<T> | undefined {
    return this.#statement.iterate(key);
  }
}
