import '../../shared/src/dotenv.ts';

import {styleText} from 'node:util';
import {logLevel, logOptions} from '../../otel/src/log-options.ts';
import {colorConsole, createLogContext} from '../../shared/src/logging.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {
  appOptions,
  shardOptions,
  ZERO_ENV_VAR_PREFIX,
  zeroOptions,
} from '../../zero-cache/src/config/zero-config.ts';
import type {AnalyzeQueryResult} from '../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {clientSchemaFrom} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {PullRow, Query} from '../../zql/src/query/query.ts';
import {analyzeRemote, type RemoteQuery} from './remote-analyze.ts';

export type AnalyzeCliOptions = {
  schema: Schema;
  /** Defaults to `process.argv.slice(2)`. */
  argv?: readonly string[];
};

const options = {
  zeroCacheUrl: {
    type: v.string().optional(),
    desc: [
      'URL of the remote zero-cache to analyze against.',
      'Accepts http(s):// or ws(s):// — normalized internally to ws(s)://.',
    ],
  },
  adminPassword: zeroOptions.adminPassword,
  authToken: {
    type: v.string().optional(),
    desc: [
      'Raw JWT forwarded to zero-cache via the WebSocket handshake.',
      'Used server-side to fill permission variables for the query.',
    ],
  },
  ast: {
    type: v.string().optional(),
    desc: [
      'JSON-encoded AST. Exactly one of --ast / --query / --query-name is required.',
    ],
  },
  query: {
    type: v.string().optional(),
    desc: [
      'ZQL query in chain form, e.g. `issue.related("comments").limit(10)`.',
      'Parsed locally against the schema passed to runAnalyzeCli.',
    ],
  },
  queryName: {
    type: v.string().optional(),
    desc: [
      'Name of a server-registered custom (named) query.',
      'The server resolves it via its own custom-query handler.',
    ],
  },
  queryArgs: {
    type: v.string().optional(),
    desc: [
      'JSON-encoded array of arguments for --query-name. Defaults to `[]`.',
    ],
  },
  outputVendedRows: {
    type: v.boolean().default(false),
    desc: [
      'Include the rows read from the replica to execute the query.',
      'Each row appears once per read.',
    ],
  },
  outputSyncedRows: {
    type: v.boolean().default(false),
    desc: ['Include the rows that would be synced to the client.'],
  },
  app: appOptions,
  shard: shardOptions,
  log: {
    ...logOptions,
    level: logLevel.default('error'),
  },
};

/**
 * Entry point for a user's `cli.ts`. Parses argv, connects to a remote
 * zero-cache over WebSocket, runs `analyze-query` via the inspector protocol,
 * and renders the result. Intended to be called as:
 *
 * ```ts
 * import {schema} from './schema.ts';
 * import {runAnalyzeCli} from '@rocicorp/zero/analyze';
 * await runAnalyzeCli({schema});
 * ```
 *
 * Exits the process with code 1 on error.
 */
export async function runAnalyzeCli(opts: AnalyzeCliOptions): Promise<void> {
  const argv = (opts.argv ?? process.argv.slice(2)).map(s =>
    s.replaceAll('\n', ' '),
  );

  const config = parseOptions(options, {
    argv,
    envNamePrefix: ZERO_ENV_VAR_PREFIX,
    description: [
      {
        header: 'analyze-query (remote)',
        content: `Analyze a ZQL query against a remote zero-cache.

  Connects over WebSocket using the inspector protocol and reports the
  server-observed row scans, SQLite query plans, and timings.`,
      },
      {
        header: 'Examples',
        content: `  tsx cli.ts --zero-cache-url=https://zero.example.com \\
    --admin-password="$ZERO_ADMIN_PASSWORD" \\
    --query='issue.related("comments").limit(10)'

  tsx cli.ts --zero-cache-url=http://localhost:4848 \\
    --ast='\\{"table": "issue", "limit": 5\\}'

  tsx cli.ts --zero-cache-url=http://localhost:4848 \\
    --query-name=issueList --query-args='[]'`,
      },
    ],
  });

  if (!config.zeroCacheUrl) {
    colorConsole.error('--zero-cache-url is required. See --help for usage.');
    process.exit(1);
  }

  const remoteQuery = buildRemoteQuery(config, opts.schema);

  const lc = createLogContext({log: config.log});
  const {clientSchema} = clientSchemaFrom(opts.schema);

  let result: AnalyzeQueryResult;
  try {
    result = await analyzeRemote(
      lc,
      config.zeroCacheUrl,
      config.adminPassword,
      config.authToken,
      clientSchema,
      remoteQuery,
      {
        vendedRows: config.outputVendedRows,
        syncedRows: config.outputSyncedRows,
      },
    );
  } catch (e) {
    colorConsole.error(e instanceof Error ? e.message : String(e));
    process.exit(1);
  }

  renderResult(result, {
    outputSyncedRows: config.outputSyncedRows,
    outputVendedRows: config.outputVendedRows,
  });
}

function buildRemoteQuery(
  config: {
    ast?: string | undefined;
    query?: string | undefined;
    queryName?: string | undefined;
    queryArgs?: string | undefined;
  },
  schema: Schema,
): RemoteQuery {
  const selectors = [
    config.ast !== undefined && 'ast',
    config.query !== undefined && 'query',
    config.queryName !== undefined && 'queryName',
  ].filter(Boolean) as string[];

  if (selectors.length === 0) {
    colorConsole.error(
      'Exactly one of --ast / --query / --query-name is required.',
    );
    process.exit(1);
  }
  if (selectors.length > 1) {
    colorConsole.error(
      `Only one of --ast / --query / --query-name may be provided; got: ${selectors.join(', ')}`,
    );
    process.exit(1);
  }

  if (config.ast !== undefined) {
    return {kind: 'ast', ast: JSON.parse(config.ast) as AST};
  }
  if (config.query !== undefined) {
    return {kind: 'ast', ast: parseQueryString(config.query, schema)};
  }
  const args = config.queryArgs
    ? (JSON.parse(config.queryArgs) as ReadonlyArray<unknown>)
    : [];
  return {kind: 'named', name: config.queryName as string, args};
}

function parseQueryString(queryString: string, schema: Schema): AST {
  const z = {
    query: Object.fromEntries(
      Object.entries(schema.tables).map(([name]) => [
        name,
        newQuery(schema, name),
      ]),
    ),
  };
  const f = new Function('z', `return z.query.${queryString};`);
  const q: Query<string, Schema, PullRow<string, Schema>> = f(z);
  return asQueryInternals(q).ast;
}

function renderResult(
  result: AnalyzeQueryResult,
  opts: {outputSyncedRows: boolean; outputVendedRows: boolean},
) {
  if (opts.outputSyncedRows) {
    colorConsole.log(styleText(['blue', 'bold'], '=== Synced Rows: ===\n'));
    for (const [table, rows] of Object.entries(result.syncedRows ?? {})) {
      colorConsole.log(styleText('bold', table + ':'), rows);
    }
  }

  colorConsole.log(styleText(['blue', 'bold'], '=== Query Stats: ===\n'));
  colorConsole.log(
    styleText('bold', 'total synced rows:'),
    result.syncedRowCount,
  );

  const readRowCountsByQuery = result.readRowCountsByQuery ?? {};
  let totalRowsRead = 0;
  for (const table of Object.keys(readRowCountsByQuery).sort()) {
    const counts = readRowCountsByQuery[table];
    for (const n of Object.values(counts)) {
      totalRowsRead += n;
    }
    colorConsole.log(styleText('bold', `${table} vended:`), counts);
  }
  colorConsole.log(
    styleText('bold', 'Rows Read (into JS):'),
    colorRowsConsidered(totalRowsRead),
  );
  const duration = result.elapsed ?? result.end - result.start;
  colorConsole.log(styleText('bold', 'time:'), colorTime(duration), 'ms');

  if (opts.outputVendedRows) {
    colorConsole.log(
      styleText(['blue', 'bold'], '=== JS Row Scan Values: ===\n'),
    );
    for (const [table, rows] of Object.entries(result.readRows ?? {})) {
      colorConsole.log(styleText('bold', `${table}:`), rows);
    }
  }

  colorConsole.log(
    styleText(['blue', 'bold'], '\n=== Rows Scanned (by SQLite): ===\n'),
  );
  const dbScansByQuery = result.dbScansByQuery ?? {};
  let totalNVisit = 0;
  for (const [table, queries] of Object.entries(dbScansByQuery)) {
    colorConsole.log(styleText('bold', `${table}:`), queries);
    for (const count of Object.values(queries)) {
      totalNVisit += count;
    }
  }
  colorConsole.log(
    styleText('bold', 'total rows scanned:'),
    colorRowsConsidered(totalNVisit),
  );

  colorConsole.log(styleText(['blue', 'bold'], '\n\n=== Query Plans: ===\n'));
  const plans = result.sqlitePlans ?? {};
  for (const [query, plan] of Object.entries(plans)) {
    colorConsole.log(styleText('bold', 'query'), query);
    colorConsole.log(plan.map((row, i) => colorPlanRow(row, i)).join('\n'));
    colorConsole.log('\n');
  }

  if (result.warnings.length > 0) {
    colorConsole.log(styleText(['yellow', 'bold'], '=== Warnings: ===\n'));
    for (const w of result.warnings) {
      colorConsole.log(styleText('yellow', w));
    }
  }
}

function colorTime(duration: number) {
  if (duration < 100) {
    return styleText('green', duration.toFixed(2) + 'ms');
  } else if (duration < 1000) {
    return styleText('yellow', duration.toFixed(2) + 'ms');
  }
  return styleText('red', duration.toFixed(2) + 'ms');
}

function colorRowsConsidered(n: number) {
  if (n < 1000) {
    return styleText('green', n.toString());
  } else if (n < 10000) {
    return styleText('yellow', n.toString());
  }
  return styleText('red', n.toString());
}

function colorPlanRow(row: string, i: number) {
  if (row.includes('SCAN')) {
    if (i === 0) {
      return styleText('yellow', row);
    }
    return styleText('red', row);
  }
  return styleText('green', row);
}
