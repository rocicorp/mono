/* eslint-disable no-console */
import chalk from 'chalk';
import 'dotenv/config';

import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {parseOptions} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  mapAST,
  type AST,
  type CompoundKey,
} from '../../../zero-protocol/src/ast.ts';
import {buildPipeline} from '../../../zql/src/builder/builder.ts';
import {Catch} from '../../../zql/src/ivm/catch.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../../zql/src/ivm/operator.ts';
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
import {ZERO_ENV_VAR_PREFIX, zeroOptions} from '../config/zero-config.ts';
import {loadSchemaAndPermissions} from './permissions.ts';
import {
  clientToServer,
  serverToClient,
} from '../../../zero-schema/src/name-mapper.ts';

const options = {
  replicaFile: zeroOptions.replica.file,
  ast: {
    type: v.string().optional(),
    desc: ['AST for the query to be transformed or timed.'],
  },
  query: {
    type: v.string().optional(),
    desc: [
      `Query to be timed in the form of: z.query.table.where(...).related(...).etc`,
    ],
  },
  schema: {
    type: v.string().default('./schema.ts'),
    desc: ['Path to the schema file.'],
  },
};

const config = parseOptions(
  options,
  process.argv.slice(2),
  ZERO_ENV_VAR_PREFIX,
);

runtimeDebugFlags.trackRowsVended = true;

const lc = createSilentLogContext();

const db = new Database(lc, config.replicaFile);
const {schema} = await loadSchemaAndPermissions(lc, config.schema);
const sources = new Map<string, TableSource>();
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);
const host: QueryDelegate = {
  mapAst(ast: AST): AST {
    return mapAST(ast, clientToServerMapper);
  },
  getSource: (serverTableName: string) => {
    const clientTableName = serverToClientMapper.tableName(serverTableName);
    let source = sources.get(serverTableName);
    if (source) {
      return source;
    }
    source = new TableSource(
      lc,
      testLogConfig,
      '',
      db,
      serverTableName,
      Object.fromEntries(
        Object.entries(schema.tables[clientTableName].columns).map(
          ([colName, column]) => [
            clientToServerMapper.columnName(clientTableName, colName),
            column,
          ],
        ),
      ),
      schema.tables[clientTableName].primaryKey.map(col =>
        clientToServerMapper.columnName(clientTableName, col),
      ) as unknown as CompoundKey,
    );

    sources.set(serverTableName, source);
    return source;
  },

  createStorage() {
    // TODO: table storage!!
    return new MemoryStorage();
  },
  decorateInput(input: Input): Input {
    return input;
  },
  addServerQuery() {
    return () => {};
  },
  updateServerQuery() {},
  onQueryMaterialized() {},
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
if (config.ast) {
  [start, end] = runAst(JSON.parse(config.ast) as AST);
} else if (config.query) {
  [start, end] = runQuery(config.query);
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
      Object.entries(schema.tables).map(([name]) => [
        name,
        newQuery(host, schema, name),
      ]),
    ),
  };
  suppressError.q = q;
  suppressError.z = z;

  console.log('schema', schema);
  eval(`q = ${queryString};`);
  console.log('evaled to q:', q);
  console.log('qstring:', queryString);

  const start = performance.now();
  q.run();
  const end = performance.now();
  return [start, end];
}

console.log(chalk.blue.bold('=== Query Stats: ===\n'));
showStats();
console.log(chalk.blue.bold('\n\n=== Query Plans: ===\n'));
explainQueries();

function showStats() {
  let totalRowsConsidered = 0;
  for (const source of sources.values()) {
    const entires = [
      ...(runtimeDebugStats.getRowsVended('')?.get(source.table)?.entries() ??
        []),
    ];
    totalRowsConsidered += entires.reduce((acc, entry) => acc + entry[1], 0);
    console.log(chalk.bold(source.table + ' vended:'), entires);
  }

  console.log(
    chalk.bold('total rows considered:'),
    colorRowsConsidered(totalRowsConsidered),
  );
  console.log(chalk.bold('time:'), colorTime(end - start), 'ms');
}

function explainQueries() {
  for (const source of sources.values()) {
    const queries =
      runtimeDebugStats.getRowsVended('')?.get(source.table)?.keys() ?? [];
    for (const query of queries) {
      console.log(chalk.bold('query'), query);
      console.log(
        db
          // we should be more intelligent about value replacement.
          // Different values result in different plans. E.g., picking a value at the start
          // of an index will result in `scan` vs `search`. The scan is fine in that case.
          .prepare(`EXPLAIN QUERY PLAN ${query.replaceAll('?', "'sdfse'")}`)
          .all<{detail: string}>()
          .map((row, i) => colorPlanRow(row.detail, i))
          .join('\n'),
      );
      console.log('\n');
    }
  }
}

function colorTime(duration: number) {
  if (duration < 100) {
    return chalk.green(duration.toFixed(2) + 'ms');
  } else if (duration < 1000) {
    return chalk.yellow(duration.toFixed(2) + 'ms');
  }
  return chalk.red(duration.toFixed(2) + 'ms');
}

function colorRowsConsidered(n: number) {
  if (n < 1000) {
    return chalk.green(n.toString());
  } else if (n < 10000) {
    return chalk.yellow(n.toString());
  }
  return chalk.red(n.toString());
}

function colorPlanRow(row: string, i: number) {
  if (row.includes('SCAN')) {
    if (i === 0) {
      return chalk.yellow(row);
    }
    return chalk.red(row);
  }
  return chalk.green(row);
}
