// create a zql query

import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {assert} from '../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {Source} from '../../zql/src/ivm/source.ts';
import {QueryDelegateBase} from '../../zql/src/query/query-delegate-base.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
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
  const tableSpecs = computeZqlSpecs(lc, db, {
    includeBackfillingColumns: true,
  });

  class BenchmarkQueryDelegate extends QueryDelegateBase {
    readonly defaultQueryComplete = true;

    getSource(name: string): Source | undefined {
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
    }
  }

  const delegate = new BenchmarkQueryDelegate();

  const issueQuery = newQuery(schema, 'issue');
  const q = issueQuery
    .related('labels')
    .orderBy('modified', 'desc')
    .limit(10_000);

  const start = performance.now();
  delegate.materialize(q);

  const end = performance.now();
  // oxlint-disable-next-line no-console
  console.log(`materialize\ttook ${end - start}ms`);
}
