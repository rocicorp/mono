// create a zql query

import {assert} from '../../shared/src/asserts.js';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.js';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.js';
import type {Source} from '../../zql/src/ivm/source.js';
import {newQuery, type QueryDelegate} from '../../zql/src/query/query-impl.js';
import {Database} from '../../zqlite/src/db.js';
import {TableSource} from '../../zqlite/src/table-source.js';
import {listTables} from '../src/db/lite-tables.js';
import {mapLiteDataTypeToZqlSchemaValue} from '../src/types/lite.js';
import {schema} from './schema.js';

type Options = {
  dbFile: string;
};

// load up some data!
export function bench(opts: Options) {
  const {dbFile} = opts;
  const db = new Database(createSilentLogContext(), dbFile);
  const sources = new Map<string, Source>();
  const tableSpecs = new Map(listTables(db).map(spec => [spec.name, spec]));
  const host: QueryDelegate = {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }
      const tableSpec = tableSpecs.get(name);
      assert(tableSpec, `Missing tableSpec for ${name}`);
      const {columns, primaryKey} = tableSpec;

      source = new TableSource(
        db,
        name,
        Object.fromEntries(
          Object.entries(columns).map(([name, {dataType}]) => [
            name,
            mapLiteDataTypeToZqlSchemaValue(dataType),
          ]),
        ),
        [primaryKey[0], ...primaryKey.slice(1)],
      );

      sources.set(name, source);
      return source;
    },

    createStorage() {
      // TODO: table storage!!
      return new MemoryStorage();
    },
    addServerQuery() {
      return () => {};
    },
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
  };

  const issueQuery = newQuery(host, schema.issue);
  const q = issueQuery
    .related('labels')
    .orderBy('modified', 'desc')
    .limit(10_000);

  const start = performance.now();
  q.materialize();

  const end = performance.now();
  console.log(`materialize\ttook ${end - start}ms`);
}
