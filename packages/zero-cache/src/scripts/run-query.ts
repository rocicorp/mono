/* eslint-disable no-console */
import 'dotenv/config';

import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import {
  newQuery,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
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

let start: number;
let end: number;
const suppressError: Record<string, unknown> = {};
if (config.debug.ast) {
  [start, end] = runAst(JSON.parse(config.debug.ast) as AST);
} else if (config.debug.query) {
  [start, end] = runQuery(config.debug.query);
} else {
  throw new Error('No query or AST provided');
}

function runAst(ast: AST): [number, number] {
  const pipeline = buildPipeline(ast, host);
  const output = new Catch(pipeline);

  const start = performance.now();
  output.fetch();
  const end = performance.now();
  return [start, end];
}

function runQuery(queryString: string): [number, number] {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let q: any;
  const z = {
    query: Object.fromEntries(
      Object.entries(schemaAndPermissions.schema.tables).map(([name]) => [
        name,
        newQuery(host, schemaAndPermissions.schema, name),
      ]),
    ),
  };
  suppressError.q = q;
  suppressError.z = z;

  eval(`q = ${queryString};`);
  const start = performance.now();
  q.run();
  const end = performance.now();
  return [start, end];
}

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
