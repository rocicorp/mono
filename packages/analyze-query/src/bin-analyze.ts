/* eslint-disable no-console */
import chalk from 'chalk';
import fs from 'node:fs';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import {logLevel, logOptions} from '../../otel/src/log-options.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import '../../shared/src/dotenv.ts';
import {colorConsole, createLogContext} from '../../shared/src/logging.ts';
import {must} from '../../shared/src/must.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.ts';
import {
  deployPermissionsOptions,
  loadSchemaAndPermissions,
} from '../../zero-cache/src/scripts/permissions.ts';
import {pgClient} from '../../zero-cache/src/types/pg.ts';
import {getShardID, upstreamSchema} from '../../zero-cache/src/types/shards.ts';
import {type AST, type CompoundKey} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';

import {MemoryStorage} from '../../zql/src/ivm/memory-storage.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {completedAST, newQuery} from '../../zql/src/query/query-impl.ts';
import {type PullRow, type Query} from '../../zql/src/query/query.ts';
import {Database} from '../../zqlite/src/db.ts';
import {runtimeDebugFlags} from '../../zql/src/builder/debug-delegate.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';
import {runAst, type RunResult} from './run-ast.ts';

const options = {
  schema: deployPermissionsOptions.schema,
  replicaFile: {
    ...zeroOptions.replica.file,
    desc: [`File path to the SQLite replica to test queries against.`],
  },
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
  cvr: {
    db: {
      type: v.string().optional(),
      desc: [
        'Connection URL to the CVR database. If using --hash, either this or --upstream-db',
        'must be specified.',
      ],
    },
  },
  upstream: {
    db: {
      desc: [
        `Connection URL to the "upstream" authoritative postgres database. If using --hash, `,
        'either this or --cvr-db must be specified.',
      ],
      type: v.string().optional(),
    },
    type: zeroOptions.upstream.type,
  },
  app: appOptions,
  shard: shardOptions,
  log: {
    ...logOptions,
    level: logLevel.default('error'),
  },
};

const cfg = parseOptions(options, {
  // the command line parses drops all text after the first newline
  // so we need to replace newlines with spaces
  // before parsing
  argv: process.argv.slice(2).map(s => s.replaceAll('\n', ' ')),
  envNamePrefix: ZERO_ENV_VAR_PREFIX,
  description: [
    {
      header: 'analyze-query',
      content: `Analyze a ZQL query and show information about how it runs against a SQLite replica.

  analyze-query uses the same environment variables and flags as zero-cache-dev. If run from your development environment, it will pick up your ZERO_REPLICA_FILE, ZERO_SCHEMA_PATH, and other env vars automatically.

  If run in another environment (e.g., production) you will have to specify these flags. In particular, you must have a copy of the appropriate Zero schema file to give to the --schema-path flag.`,
    },
    {
      header: 'Examples',
      content: `# In development
  npx analyze-query --query='issue.related("comments").limit(10)'
  npx analyze-query --ast='\\{"table": "artist","limit": 10\\}'
  npx analyze-query --hash=1234567890

  # In production
  # First copy schema.ts to your production environment, then run:
  npx analyze-query \\
    --schema-path='./schema.ts' \\
    --replica-file='/path/to/replica.db' \\
    --query='issue.related("comments").limit(10)'

  npx analyze-query \\
    --schema-path='./schema.ts' \\
    --replica-file='/path/to/replica.db' \\
    --ast='\\{"table": "artist","limit": 10\\}'

  # cvr-db is required when using the hash option.
  # It is typically the same as your upstream db.
  npx analyze-query \\
    --schema-path='./schema.ts' \\
    --replica-file='/path/to/replica.db' \\
    --cvr-db='postgres://user:pass@host:port/db' \\
    --hash=1234567890
  `,
    },
  ],
});
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
const lc = createLogContext({
  log: config.log,
});

if (!fs.existsSync(config.replicaFile)) {
  colorConsole.error(`Replica file ${config.replicaFile} does not exist`);
  process.exit(1);
}
const db = new Database(lc, config.replicaFile);

const {schema, permissions} = await loadSchemaAndPermissions(
  config.schema.path,
);

const sources = new Map<string, TableSource>();
const clientToServerMapper = clientToServer(schema.tables);
const serverToClientMapper = serverToClient(schema.tables);
const debug = new Debug();
const host: QueryDelegate = {
  debug,
  getSource: (serverTableName: string) => {
    const clientTableName = serverToClientMapper.tableName(serverTableName);
    let source = sources.get(serverTableName);
    if (source) {
      return source;
    }
    source = new TableSource(
      lc,
      testLogConfig,
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
  decorateInput: input => input,
  decorateSourceInput: input => input,
  decorateFilterInput: input => input,
  addEdge() {},
  addServerQuery() {
    return () => {};
  },
  addCustomQuery() {
    return () => {};
  },
  updateServerQuery() {},
  updateCustomQuery() {},
  onTransactionCommit() {
    return () => {};
  },
  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  },
  flushQueryChanges() {},
  assertValidRunOptions() {},
  defaultQueryComplete: true,
  addMetric() {},
};

let result: RunResult;

if (config.ast) {
  // the user likely has a transformed AST since the wire and storage formats are the transformed AST
  result = await runAst(lc, JSON.parse(config.ast), true, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    outputSyncedRows: config.outputSyncedRows,
    db,
    host,
  });
} else if (config.query) {
  result = await runQuery(config.query);
} else if (config.hash) {
  result = await runHash(config.hash);
} else {
  colorConsole.error('No query or AST or hash provided');
  process.exit(1);
}

function runQuery(queryString: string): Promise<RunResult> {
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
  return runAst(lc, ast, false, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    outputSyncedRows: config.outputSyncedRows,
    db,
    host,
  });
}

async function runHash(hash: string) {
  const cvrDB = pgClient(
    lc,
    must(config.cvr.db, 'CVR DB must be provided when using the hash option'),
  );

  const rows = await cvrDB`select "clientAST", "internal" from ${cvrDB(
    upstreamSchema(getShardID(config)) + '/cvr',
  )}."queries" where "queryHash" = ${must(hash)} limit 1;`;
  await cvrDB.end();

  colorConsole.log('ZQL from Hash:');
  const ast = rows[0].clientAST as AST;
  colorConsole.log(await formatOutput(ast.table + astToZQL(ast)));

  return runAst(lc, ast, true, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    outputSyncedRows: config.outputSyncedRows,
    db,
    host,
  });
}

if (config.outputSyncedRows) {
  colorConsole.log(chalk.blue.bold('=== Synced Rows: ===\n'));
  for (const [table, rows] of Object.entries(result.syncedRows)) {
    colorConsole.log(chalk.bold(table + ':'), rows);
  }
}

colorConsole.log(chalk.blue.bold('=== Query Stats: ===\n'));
colorConsole.log(chalk.bold('total synced rows:'), result.syncedRowCount);
showStats();
if (config.outputVendedRows) {
  colorConsole.log(chalk.blue.bold('=== Vended Rows: ===\n'));
  for (const source of sources.values()) {
    const entries = [
      ...(debug
        .getVendedRows()
        .get(clientGroupID)
        ?.get(source.table)
        ?.entries() ?? []),
    ];
    colorConsole.log(
      chalk.bold(`${source.table}:`),
      Object.fromEntries(entries),
    );
  }
}
colorConsole.log(chalk.blue.bold('\n\n=== Query Plans: ===\n'));
explainQueries();

function showStats() {
  let totalRowsConsidered = 0;
  for (const source of sources.values()) {
    const entries = [
      ...(debug.getVendedRowCounts()?.get(source.table)?.entries() ?? []),
    ];
    totalRowsConsidered += entries.reduce((acc, entry) => acc + entry[1], 0);
    colorConsole.log(
      chalk.bold(source.table + ' vended:'),
      Object.fromEntries(entries),
    );
  }

  colorConsole.log(
    chalk.bold('total rows considered:'),
    colorRowsConsidered(totalRowsConsidered),
  );
  colorConsole.log(
    chalk.bold('time:'),
    colorTime(result.end - result.start),
    'ms',
  );
}

function explainQueries() {
  for (const source of sources.values()) {
    const queries = debug.getVendedRowCounts()?.get(source.table)?.keys() ?? [];
    for (const query of queries) {
      colorConsole.log(chalk.bold('query'), query);
      colorConsole.log(
        db
          // we should be more intelligent about value replacement.
          // Different values result in different plans. E.g., picking a value at the start
          // of an index will result in `scan` vs `search`. The scan is fine in that case.
          .prepare(`EXPLAIN QUERY PLAN ${query.replaceAll('?', "'sdfse'")}`)
          .all<{detail: string}>()
          .map((row, i) => colorPlanRow(row.detail, i))
          .join('\n'),
      );
      colorConsole.log('\n');
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
