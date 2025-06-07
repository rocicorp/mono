// create a zql query

import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {assert} from '../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {FilterInput} from '../../zql/src/ivm/filter-operators.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../zql/src/ivm/operator.ts';
import type {Source} from '../../zql/src/ivm/source.ts';
import {newQuery, type QueryDelegate} from '../../zql/src/query/query-impl.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
import {computeZqlSpecs} from '../src/db/lite-tables.ts';
import {mapLiteDataTypeToZqlSchemaValue} from '../src/types/lite.ts';
import {schema} from './schema.ts';

type Options = {
  dbFile: string;
};

// load up some data!
export function bench(opts: Options) {
  const {dbFile} = opts;
  const lc = createSilentLogContext();
  const db = new Database(lc, dbFile);
  const sources = new Map<string, Source>();
  const tableSpecs = computeZqlSpecs(lc, db);
  const host: QueryDelegate = {
    getSource: (name: string) => {
      let source = sources.get(name);
      if (source) {
        return source;
      }
      const spec = tableSpecs.get(name);
      assert(spec?.tableSpec, `Missing tableSpec for ${name}`);
      const {columns, primaryKey} = spec.tableSpec;

      source = new TableSource(
        lc,
        testLogConfig,
        'benchmark',
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
    decorateInput(input: Input): Input {
      return input;
    },
    decorateFilterInput(input: FilterInput): FilterInput {
      return input;
    },
    addServerQuery() {
      return () => {};
    },
    addCustomQuery() {
      return () => {};
    },
    updateServerQuery() {},
    updateCustomQuery() {},
    onQueryMaterialized() {},
    onTransactionCommit() {
      return () => {};
    },
    batchViewUpdates<T>(applyViewUpdates: () => T): T {
      return applyViewUpdates();
    },
    assertValidRunOptions() {},
    defaultQueryComplete: true,
  };

  const issueQuery = newQuery(host, schema, 'issue');
  const q = issueQuery
    .related('labels')
    .orderBy('modified', 'desc')
    .limit(10_000);

  const start = performance.now();
  q.materialize();

  const end = performance.now();
  // eslint-disable-next-line no-console
  console.log(`materialize\ttook ${end - start}ms`);
}
