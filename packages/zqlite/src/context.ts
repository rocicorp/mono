import type {Context} from 'zql/src/zql/context/context.js';
import type {Materialite} from 'zql/src/zql/ivm/materialite.js';
import type {Database} from 'better-sqlite3';
import {TableSource} from './table-source.js';
import {DB} from './internal/db.js';
import type {PipelineEntity} from 'zql/src/zql/ivm/types.js';
import type {Source} from 'zql/src/zql/ivm/source/source.js';
import type {Ordering} from 'zql/src/zql/ast/ast.js';

const emptyFunction = () => {};
export function createContext(
  materialite: Materialite,
  sqliteDb: Database,
): Context {
  const db = new DB(sqliteDb);
  const sources = new Map<string, Source<PipelineEntity>>();

  return {
    materialite,
    getSource: <T extends PipelineEntity>(
      name: string,
      _ordering: Ordering | undefined,
    ): Source<T> => {
      let existing = sources.get(name);
      if (existing) {
        return existing as Source<T>;
      }
      const sql = `SELECT name FROM pragma_table_info(?)`;
      const columns = db.getStmt(sql).pluck().all(name);
      db.returnStmt(sql);
      existing = materialite.constructSource(
        internal => new TableSource(db, internal, name, columns),
      );
      sources.set(name, existing);
      return existing as Source<T>;
    },
    subscriptionAdded: () => emptyFunction,
  };
}
