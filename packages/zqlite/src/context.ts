import type {Context} from 'zql/src/zql/context/context.js';
import type {Materialite} from 'zql/src/zql/ivm/materialite.js';
import type {Database} from 'better-sqlite3';
import {TableSource} from './table-source.js';
import type {PipelineEntity} from 'zql/src/zql/ivm/types.js';
import type {Source} from 'zql/src/zql/ivm/source/source.js';

const emptyFunction = () => {};

export function createContext(materialite: Materialite, db: Database): Context {
  const sources = new Map<string, Source<PipelineEntity>>();
  const sql = `SELECT name FROM pragma_table_info(?)`;
  const columnsStatement = db.prepare(sql).pluck();

  return {
    materialite,
    getSource: <T extends PipelineEntity>(name: string): Source<T> => {
      let existing = sources.get(name);
      if (existing) {
        return existing as Source<T>;
      }
      const columns = columnsStatement.all(name);
      existing = materialite.constructSource(
        internal => new TableSource(db, internal, name, columns),
      );
      sources.set(name, existing);
      return existing as Source<T>;
    },
    subscriptionAdded: () => emptyFunction,
  };
}
