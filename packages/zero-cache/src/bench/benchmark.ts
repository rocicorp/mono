// create a zql query

import Database from 'better-sqlite3';
import {Host} from 'zql/src/zql/builder/builder.js';
import {Source} from 'zql/src/zql/ivm2/source.js';
import {listTables} from '../services/replicator/tables/list.js';
import {must} from 'shared/src/must.js';
import {TableSource} from '@rocicorp/zqlite/src/v2/table-source.js';
import {mapLiteDataTypeToZqlValueType} from '../types/lite.js';
import {MemoryStorage} from 'zql/src/zql/ivm2/memory-storage.js';
import {newQuery} from 'zql/src/zql/query2/query-impl.js';
import {SubscriptionDelegate} from 'zql/src/zql/context/context.js';
import {schema} from './schema.js';

// load up some data!
function bench() {
  const db = new Database('/tmp/sync-replica.db');
  const sources = new Map<string, Source>();
  const tableSpecs = new Map(listTables(db).map(spec => [spec.name, spec]));
  const host: Host & SubscriptionDelegate = {
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
            mapLiteDataTypeToZqlValueType(dataType),
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

    subscriptionAdded() {
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

bench();
