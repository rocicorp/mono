/* eslint-disable no-console */
import 'dotenv/config';

import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {type QueryDelegate} from '../../../zql/src/query/query-impl.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {
  runtimeDebugFlags,
  runtimeDebugStats,
} from '../../../zqlite/src/runtime-debug.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import {getSchema} from '../auth/load-schema.ts';
import {getDebugConfig} from '../config/zero-config.ts';

const config = getDebugConfig();
const schemaAndPermissions = await getSchema(config);
runtimeDebugFlags.trackRowsVended = true;

const ast = JSON.parse(must(config.debug.ast)) as AST;

const db = new Database(createSilentLogContext(), config.replicaFile);
const sources = new Map<string, TableSource>();
const host: QueryDelegate = {
  getSource: (name: string) => {
    let source = sources.get(name);
    if (source) {
      return source;
    }
    source = new TableSource(
      '',
      db,
      name,
      schemaAndPermissions.schema.tables[name].columns,
      schemaAndPermissions.schema.tables[name].primaryKey,
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

const pipeline = buildPipeline(ast, host);
const output = new Catch(pipeline);

const start = performance.now();
output.fetch();
const end = performance.now();

let totalRowsConsidered = 0;
for (const source of sources.values()) {
  const entires = [
    ...(runtimeDebugStats.getRowsVended('')?.get(source.table)?.entries() ??
      []),
  ];
  totalRowsConsidered += entires.reduce((acc, entry) => acc + entry[1], 0);
  console.log(source.table + ' VENDED: ', entires);
}

// console.log(JSON.stringify(view, null, 2));
console.log('ROWS CONSIDERED:', totalRowsConsidered);
console.log('TIME:', (end - start).toFixed(2), 'ms');
