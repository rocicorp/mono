import 'dotenv/config';

import {getDebugConfig} from '../config/zero-config.js';
import {getSchema} from '../auth/load-schema.js';
import {must} from '../../../shared/src/must.js';
import {Database} from '../../../zqlite/src/db.js';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.js';
import {type QueryDelegate} from '../../../zql/src/query/query-impl.js';
import {TableSource} from '../../../zqlite/src/table-source.js';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.js';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {buildPipeline} from '../../../zql/src/builder/builder.js';
import {Catch} from '../../../zql/src/ivm/catch.js';
import {
  runtimeDebugFlags,
  runtimeDebugStats,
} from '../../../zqlite/src/runtime-debug.js';

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
