import '../../shared/src/dotenv.ts';

import chalk from 'chalk';
import fs from 'node:fs';
import {astToZQL} from '../../ast-to-zql/src/ast-to-zql.ts';
import {formatOutput} from '../../ast-to-zql/src/format.ts';
import {logLevel, logOptions} from '../../otel/src/log-options.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
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
  computeZqlSpecs,
  mustGetTableSpec,
} from '../../zero-cache/src/db/lite-tables.ts';
import {
  deployPermissionsOptions,
  loadSchemaAndPermissions,
} from '../../zero-cache/src/scripts/permissions.ts';
import {pgClient} from '../../zero-cache/src/types/pg.ts';
import {getShardID, upstreamSchema} from '../../zero-cache/src/types/shards.ts';
import type {AnalyzeQueryResult} from '../../zero-protocol/src/analyze-query-result.ts';
import {type AST} from '../../zero-protocol/src/ast.ts';
import {clientSchemaFrom} from '../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {
  Debug,
  runtimeDebugFlags,
} from '../../zql/src/builder/debug-delegate.ts';
import type {Source} from '../../zql/src/ivm/source.ts';
import type {PullRow, QueryBuilder} from '../../zql/src/query/query-builder.ts';
import {QueryDelegateBase} from '../../zql/src/query/query-delegate-base.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {Database} from '../../zqlite/src/db.ts';
import {TableSource} from '../../zqlite/src/table-source.ts';
import {explainQueries} from './explain-queries.ts';
import {runAst} from './run-ast.ts';

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
const clientSchema = clientSchemaFrom(schema).clientSchema;

const sources = new Map<string, TableSource>();
const clientToServerMapper = clientToServer(schema.tables);
const debug = new Debug();
const tableSpecs = computeZqlSpecs(lc, db);

class AnalyzeQueryDelegate extends QueryDelegateBase {
  readonly debug = debug;
  readonly defaultQueryComplete = true;

  getSource(serverTableName: string): Source | undefined {
    let source = sources.get(serverTableName);
    if (source) {
      return source;
    }
    const tableSpec = mustGetTableSpec(tableSpecs, serverTableName);
    const {primaryKey} = tableSpec.tableSpec;

    source = new TableSource(
      lc,
      testLogConfig,
      db,
      serverTableName,
      tableSpec.zqlSpec,
      primaryKey,
    );

    sources.set(serverTableName, source);
    return source;
  }
}

const host = new AnalyzeQueryDelegate();

let result: AnalyzeQueryResult;

if (config.ast) {
  // the user likely has a transformed AST since the wire and storage formats are the transformed AST
  result = await runAst(lc, clientSchema, JSON.parse(config.ast), true, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    syncedRows: config.outputSyncedRows,
    db,
    tableSpecs,
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

function runQuery(queryString: string): Promise<AnalyzeQueryResult> {
  const z = {
    query: Object.fromEntries(
      Object.entries(schema.tables).map(([name]) => [
        name,
        newQuery(schema, name),
      ]),
    ),
  };

  const f = new Function('z', `return z.query.${queryString};`);
  const q: QueryBuilder<string, Schema, PullRow<string, Schema>> = f(z);

  const ast = asQueryInternals(q).ast;
  return runAst(lc, clientSchema, ast, false, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    syncedRows: config.outputSyncedRows,
    db,
    tableSpecs,
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

  return runAst(lc, clientSchema, ast, true, {
    applyPermissions: config.applyPermissions,
    authData: config.authData,
    clientToServerMapper,
    permissions,
    syncedRows: config.outputSyncedRows,
    db,
    tableSpecs,
    host,
  });
}

if (config.outputSyncedRows) {
  colorConsole.log(chalk.blue.bold('=== Synced Rows: ===\n'));
  for (const [table, rows] of Object.entries(result.syncedRows ?? {})) {
    colorConsole.log(chalk.bold(table + ':'), rows);
  }
}

colorConsole.log(chalk.blue.bold('=== Query Stats: ===\n'));
colorConsole.log(chalk.bold('total synced rows:'), result.syncedRowCount);
showStats();
if (config.outputVendedRows) {
  colorConsole.log(chalk.blue.bold('=== JS Row Scan Values: ===\n'));
  for (const source of sources.values()) {
    colorConsole.log(
      chalk.bold(`${source.tableSchema.name}:`),
      debug.getVendedRows()?.[source.tableSchema.name] ?? {},
    );
  }
}

colorConsole.log(chalk.blue.bold('\n=== Rows Scanned (by SQLite): ===\n'));
const nvisitCounts = debug.getNVisitCounts();
let totalNVisit = 0;
for (const [table, queries] of Object.entries(nvisitCounts)) {
  colorConsole.log(chalk.bold(`${table}:`), queries);
  for (const count of Object.values(queries)) {
    totalNVisit += count;
  }
}
colorConsole.log(
  chalk.bold('total rows scanned:'),
  colorRowsConsidered(totalNVisit),
);

colorConsole.log(chalk.blue.bold('\n\n=== Query Plans: ===\n'));
const plans = explainQueries(debug.getVendedRowCounts() ?? {}, db);
for (const [query, plan] of Object.entries(plans)) {
  colorConsole.log(chalk.bold('query'), query);
  colorConsole.log(plan.map((row, i) => colorPlanRow(row, i)).join('\n'));
  colorConsole.log('\n');
}

function showStats() {
  let totalRowsConsidered = 0;
  for (const source of sources.values()) {
    const values = Object.values(
      debug.getVendedRowCounts()?.[source.tableSchema.name] ?? {},
    );
    for (const v of values) {
      totalRowsConsidered += v;
    }
    colorConsole.log(
      chalk.bold(source.tableSchema.name + ' vended:'),
      debug.getVendedRowCounts()?.[source.tableSchema.name] ?? {},
    );
  }

  colorConsole.log(
    chalk.bold('Rows Read (into JS):'),
    colorRowsConsidered(totalRowsConsidered),
  );
  colorConsole.log(
    chalk.bold('time:'),
    colorTime(result.end - result.start),
    'ms',
  );
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
