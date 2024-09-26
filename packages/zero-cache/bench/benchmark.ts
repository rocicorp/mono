// create a zql query

import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {must} from 'shared/src/must.js';
import {MemoryStorage} from 'zql/src/zql/ivm/memory-storage.js';
import type {Source} from 'zql/src/zql/ivm/source.js';
import {newQuery, type QueryDelegate} from 'zql/src/zql/query/query-impl.js';
import {Database} from 'zqlite/src/db.js';
import {TableSource} from 'zqlite/src/table-source.js';
import {listTables} from '../src/db/lite-tables.js';
import {mapLiteDataTypeToZqlSchemaValue} from '../src/types/lite.js';
import {schema} from './schema.js';

// load up some data!
export function bench() {
  const db = new Database(createSilentLogContext(), '/tmp/sync-replica.db');
  const sources = new Map<string, Source>();
  const tableSpecs = new Map(listTables(db).map(spec => [spec.name, spec]));
  const host: QueryDelegate = {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }
      const tableSpec = must(tableSpecs.get(name));
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
  };

  const issueQuery = newQuery(host, schema.issue);
  const view = issueQuery
    .related('labels')
    .orderBy('modified', 'desc')
    .limit(10_000)
    .materialize();

  const start = performance.now();
  view.hydrate();
  const end = performance.now();
  console.log(`hydrate took ${end - start}ms`);
}
