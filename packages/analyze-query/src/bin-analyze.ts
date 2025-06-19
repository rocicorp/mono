/* eslint-disable no-console */
import '../../shared/src/dotenv.ts';
import chalk from 'chalk';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {must} from '../../shared/src/must.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {transformAndHashQuery} from '../../zero-cache/src/auth/read-authorizer.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.ts';
import {loadSchemaAndPermissions} from '../../zero-cache/src/scripts/permissions.ts';
import {pgClient} from '../../zero-cache/src/types/pg.ts';
import {hydrate} from '../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {getShardID, upstreamSchema} from '../../zero-cache/src/types/shards.ts';
import {
  mapAST,
  type AST,
  type CompoundKey,
} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import {buildPipeline} from '../../zql/src/builder/builder.ts';
import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {Input} from '../../zql/src/ivm/operator.ts';
import {completedAST, newQuery} from '../../zql/src/query/query-impl.ts';
import {type PullRow, type Query} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {
  runtimeDebugFlags,
  runtimeDebugStats,
} from '../../zqlite/src/runtime-debug.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
import type {FilterInput} from '../../zql/src/ivm/filter-operators.ts';
import {hashOfAST} from '../../zero-protocol/src/query-hash.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {assert} from '../../shared/src/asserts.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';

const options = {
  replica: zeroOptions.replica,
  ast: {
    type: v.string().optional(),
    desc: [
      'AST for the query to be analyzed.  Only one of ast/query/hash should be provided.',
    ],
  },
  query: {
    type: v.string().optional(),
    desc: [
      `Query to be analyzed in the form of: table.where(...).related(...).etc. `,
      `Only one of ast/query/hash should be provided.`,
    ],
  },
  hash: {
    type: v.string().optional(),
    desc: [
      `Hash of the query to be analyzed. This is used to look up the query in the database. `,
      `Only one of ast/query/hash should be provided.`,
      `You should run this script from the directory containing your .env file to reduce the amount of`,
      `configuration required. The .env file should contain the connection URL to the CVR database.`,
    ],
  },
  schemaPath: {
    type: v.string().default('./schema.ts'),
    desc: ['Path to the schema file.'],
  },
  applyPermissions: {
    type: v.boolean().default(false),
    desc: [
      'Whether to apply permissions (from your schema file) to the provided query.',
    ],
  },
  authData: {
    type: v.string().optional(),
    desc: [
      'JSON encoded payload of the auth data.',
      'This will be used to fill permission variables if the "applyPermissions" option is set',
    ],
  },
  cvr: {
    db: {
      type: v.string().optional(),
      desc: [
        'Connection URL to the CVR database. Required if using a query hash. ',
        'Will attempt to be set to your upstream db if this option is not specified.',
        'If your upstream db does not have a schema for the cvr, you must provide this in ',
        'order to use the hash option.',
      ],
    },
  },
  app: appOptions,
  shard: shardOptions,
  outputVendedRows: {
    type: v.boolean().default(false),
    desc: [
      'Whether to output the rows which were read from the replica in order to execute the analyzed query. ',
      'If the same row is read more than once it will be logged once for each time it was read.',
    ],
  },
  outputSyncedRows: {
    type: v.boolean().default(false),
    desc: [
      'Whether to output the rows which would be synced to the client for the analyzed query.',
    ],
  },

  // This args is hidden as it is only present to:
  // 1. Parse it out of the env if it exists
  // 2. Use it to default the `cvr` to `upstream` if `cvr` is not provided
  upstream: Object.fromEntries(
    Object.entries(zeroOptions.upstream).map(([key, value]) => [
      key,
      {
        ...value,
        type: value.type.optional(),
        hidden: true,
      },
    ]),
  ) as unknown as typeof zeroOptions.upstream,
};

const cfg = parseOptions(
  options,
  // the command line parses drops all text after the first newline
  // so we need to replace newlines with spaces
  // before parsing
  process.argv.slice(2).map(s => s.replaceAll('\n', ' ')),
  ZERO_ENV_VAR_PREFIX,
);
const config = {
  ...cfg,
  cvr: {
    ...cfg.cvr,
    db: cfg.cvr.db ?? cfg.upstream.db,
  },
};

runtimeDebugFlags.trackRowCountsVended = true;
runtimeDebugFlags.trackRowsVended = config.outputVendedRows;

const clientGroupID = 'clientGroupIDForAnalyze';
const lc = createSilentLogContext();

const db = new Database(lc, config.replica.file);
const {schema, permissions} = await loadSchemaAndPermissions(
  lc,
  config.schemaPath,
);
const sources = new Map<string, TableSource>();
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);
const host: QueryDelegate = {
  getSource: (serverTableName: string) => {
    const clientTableName = serverToClientMapper.tableName(serverTableName);
    let source = sources.get(serverTableName);
    if (source) {
      return source;
    }
    source = new TableSource(
      lc,
      testLogConfig,
      clientGroupID,
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

let start: number;
let end: number;

if (config.ast) {
  // the user likely has a transformed AST since the wire and storage formats are the transformed AST
  [start, end] = await runAst(JSON.parse(config.ast), true);
} else if (config.query) {
  [start, end] = await runQuery(config.query);
} else if (config.hash) {
  [start, end] = await runHash(config.hash);
} else {
  throw new Error('No query or AST or hash provided');
}

async function runAst(
  ast: AST,
  isTransformed: boolean,
): Promise<[number, number]> {
  if (!isTransformed) {
    // map the AST to server names if not already transformed
    ast = mapAST(ast, clientToServerMapper);
  }
  if (config.applyPermissions) {
    const authData = config.authData ? JSON.parse(config.authData) : {};
    if (!config.authData) {
      console.warn(
        chalk.yellow(
          'No auth data provided. Permission rules will compare to `NULL` wherever an auth data field is referenced.',
        ),
      );
    }
    ast = transformAndHashQuery(
      lc,
      clientGroupID,
      ast,
      permissions,
      authData,
      false,
    ).transformedAst;
    console.log(chalk.blue.bold('\n\n=== Query After Permissions: ===\n'));
    console.log(await formatOutput(ast.table + astToZQL(ast)));
  }

  const tableSpecs = computeZqlSpecs(lc, db);
  const pipeline = buildPipeline(ast, host);

  const start = performance.now();

  if (config.outputSyncedRows) {
    console.log(chalk.blue.bold('\n\n=== Synced rows: ===\n'));
  }
  let syncedRowCount = 0;
  const rowsByTable: Record<string, Row[]> = {};
  for (const rowChange of hydrate(pipeline, hashOfAST(ast), tableSpecs)) {
    assert(rowChange.type === 'add');
    syncedRowCount++;
    if (config.outputSyncedRows) {
      let rows: Row[] = rowsByTable[rowChange.table];
      if (!rows) {
        rows = [];
        rowsByTable[rowChange.table] = rows;
      }
      rows.push(rowChange.row);
    }
  }
  if (config.outputSyncedRows) {
    for (const [source, rows] of Object.entries(rowsByTable)) {
      console.log(chalk.bold(`${source}:`), rows);
    }
    console.log(chalk.bold('total synced rows:'), syncedRowCount);
  }

  const end = performance.now();
  return [start, end];
}

function runQuery(queryString: string): Promise<[number, number]> {
  const z = {
    query: Object.fromEntries(
      Object.entries(schema.tables).map(([name]) => [
        name,
        newQuery(host, schema, name),
      ]),
    ),
  };

  const f = new Function('z', `return z.query.${queryString};`);
  const q: Query<Schema, string, PullRow<string, Schema>> = f(z);

  const ast = completedAST(q);
  return runAst(ast, false);
}

async function runHash(hash: string) {
  const cvrDB = pgClient(
    lc,
    must(config.cvr.db, 'CVR DB must be provided when using the hash option'),
  );

  const rows =
    await cvrDB`select "clientAST", "internal" from ${cvrDB(upstreamSchema(getShardID(config)) + '/cvr')}."queries" where "queryHash" = ${must(
      hash,
    )} limit 1;`;
  await cvrDB.end();

  console.log('ZQL from Hash:');
  const ast = rows[0].clientAST as AST;
  console.log(await formatOutput(ast.table + astToZQL(ast)));

  return runAst(ast, true);
}

console.log(chalk.blue.bold('=== Query Stats: ===\n'));
showStats();
if (config.outputVendedRows) {
  console.log(chalk.blue.bold('=== Vended Rows: ===\n'));
  for (const source of sources.values()) {
    const entries = [
      ...(runtimeDebugStats
        .getVendedRows()
        .get(clientGroupID)
        ?.get(source.table)
        ?.entries() ?? []),
    ];
    console.log(chalk.bold(`${source.table}:`), Object.fromEntries(entries));
  }
}
console.log(chalk.blue.bold('\n\n=== Query Plans: ===\n'));
explainQueries();

function showStats() {
  let totalRowsConsidered = 0;
  for (const source of sources.values()) {
    const entries = [
      ...(runtimeDebugStats
        .getVendedRowCounts()
        .get(clientGroupID)
        ?.get(source.table)
        ?.entries() ?? []),
    ];
    totalRowsConsidered += entries.reduce((acc, entry) => acc + entry[1], 0);
    console.log(
      chalk.bold(source.table + ' vended:'),
      Object.fromEntries(entries),
    );
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
      runtimeDebugStats
        .getVendedRowCounts()
        .get(clientGroupID)
        ?.get(source.table)
        ?.keys() ?? [];
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
