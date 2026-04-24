import '../../shared/src/dotenv.ts';

import {Console} from 'node:console';
import {styleText} from 'node:util';
import type {LogSink} from '@rocicorp/logger';
import {WebSocket as NodeWebSocket} from 'ws';
import {logLevel, logOptions} from '../../otel/src/log-options.ts';
import {colorConsole} from '../../shared/src/logging.ts';
import {parseOptions} from '../../shared/src/options.ts';
import * as v from '../../shared/src/valita.ts';
import {ZERO_ENV_VAR_PREFIX} from '../../zero-cache/src/config/zero-config.ts';
import {Zero} from '../../zero-client/src/client/zero.ts';
import type {AnalyzeQueryResult} from '../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import type {SchemaQuery} from '../../zql/src/query/schema-query.ts';

export type AnalyzeCliOptions = {
  schema: Schema;
  /** Defaults to `process.argv.slice(2)`. */
  argv?: readonly string[] | undefined;
};

const options = {
  zeroCacheUrl: {
    type: v.string().optional(),
    desc: [
      'URL of the remote zero-cache to analyze against.',
      'Accepts http(s):// or ws(s):// (ws(s) is the transport actually used).',
    ],
  },
  adminPassword: {
    type: v.string().optional(),
    desc: [
      'Admin password for zero-cache.',
      'Required when the server is configured with one; ignored in dev mode.',
    ],
  },
  authToken: {
    type: v.string().optional(),
    desc: [
      'Raw JWT forwarded to zero-cache.',
      'Used server-side to fill permission variables for the query.',
    ],
  },
  cookie: {
    type: v.string().optional(),
    desc: [
      'Cookie header value sent on the WebSocket upgrade request,',
      'e.g. `session=abc; foo=bar`. Use this when zero-cache is behind',
      'a proxy that resolves auth via cookies. Merged with --headers-json',
      '(--cookie wins on conflict).',
    ],
  },
  headersJson: {
    type: v.string().optional(),
    desc: [
      'JSON object of arbitrary headers to send on the WebSocket upgrade',
      'request, e.g. `{"x-api-key":"..."}`. Escape hatch for exotic auth',
      'schemes; prefer --auth-token or --cookie when possible.',
    ],
  },
  userId: {
    type: v.string().optional(),
    desc: [
      'Optional userID to report to zero-cache.',
      'Has no functional effect on analysis; defaults to "analyze-cli".',
    ],
  },
  ast: {
    type: v.string().optional(),
    desc: [
      'JSON-encoded AST. Exactly one of --ast / --query / --query-name is required.',
      'The AST is sent to the server verbatim — provide it in server (post-mapping) form.',
    ],
  },
  query: {
    type: v.string().optional(),
    desc: [
      'ZQL query in chain form, e.g. `issue.related("comments").limit(10)`.',
      'Evaluated against the schema you pass to runAnalyzeCli.',
    ],
  },
  queryName: {
    type: v.string().optional(),
    desc: [
      'Name of a server-registered custom (named) query.',
      'The server resolves the name + args via its registered query handler.',
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
  log: {
    ...logOptions,
    level: logLevel.default('error'),
  },
};

type QueryPlan =
  | {kind: 'ast'; ast: AST}
  | {kind: 'zql'; text: string}
  | {kind: 'named'; name: string; args: ReadonlyArray<unknown>};

// Route all Zero client log output to stderr so stdout contains only the
// analyze result. Shell redirection (`2>/dev/null`) can then cleanly silence
// logs without affecting output.
const stderrConsole = new Console({
  stdout: process.stderr,
  stderr: process.stderr,
});
const stderrLogSink: LogSink = {
  log(level, context, ...args) {
    const ctx = context
      ? Object.entries(context).map(([k, v]) =>
          v === undefined ? k : `${k}=${v}`,
        )
      : [];
    stderrConsole[level](...ctx, ...args);
  },
};

/**
 * Entry point for a user's `cli.ts`. Parses argv, connects to a remote
 * zero-cache by standing up an in-process Zero client (in-memory storage,
 * no subscriptions), calls the inspector's `analyze-query` RPC, and
 * renders the result. Intended to be called as:
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

  Connects to zero-cache's inspector protocol and reports the server-observed
  row scans, SQLite query plans, and timings.`,
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

  const plan = buildQueryPlan(config);

  const handshakeHeaders = resolveHandshakeHeaders(config);
  if (Object.keys(handshakeHeaders).length > 0) {
    installWebSocketHeaderShim(handshakeHeaders);
  }

  // zero-client and replicache reference a build-time `TESTING` global that
  // bundlers replace with a boolean literal; under tsx there's no replacement,
  // so provide a runtime default.
  (globalThis as {TESTING?: boolean}).TESTING ??= false;

  const z = new Zero({
    schema: opts.schema,
    server: config.zeroCacheUrl,
    auth: config.authToken,
    userID: config.userId ?? 'analyze-cli',
    kvStore: 'mem',
    logLevel: config.log.level,
    logSink: stderrLogSink,
  });

  let result: AnalyzeQueryResult;
  try {
    const authOk = await z.inspector.authenticate(config.adminPassword ?? '');
    if (!authOk) {
      throw new Error(
        'admin password rejected (or --admin-password is required)',
      );
    }

    const rpcOptions = {
      vendedRows: config.outputVendedRows,
      syncedRows: config.outputSyncedRows,
    };

    if (plan.kind === 'ast') {
      result = await z.inspector.analyzeServerAST(plan.ast, rpcOptions);
    } else if (plan.kind === 'named') {
      result = await z.inspector.analyzeNamedQuery(
        plan.name,
        plan.args as ReadonlyArray<never>,
        rpcOptions,
      );
    } else {
      const built = buildZqlQuery(plan.text, createBuilder(opts.schema));
      result = await z.inspector.analyzeQuery(built, rpcOptions);
    }
  } catch (e) {
    colorConsole.error(e instanceof Error ? e.message : String(e));
    await z.close().catch(() => {});
    process.exit(1);
  }

  renderResult(result, {
    outputSyncedRows: config.outputSyncedRows,
    outputVendedRows: config.outputVendedRows,
  });

  await z.close();
}

function buildQueryPlan(config: {
  ast?: string | undefined;
  query?: string | undefined;
  queryName?: string | undefined;
  queryArgs?: string | undefined;
}): QueryPlan {
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
    return {kind: 'zql', text: config.query};
  }
  const args = config.queryArgs
    ? (JSON.parse(config.queryArgs) as ReadonlyArray<unknown>)
    : [];
  return {kind: 'named', name: config.queryName as string, args};
}

function buildZqlQuery(
  queryString: string,
  builder: SchemaQuery<Schema>,
): AnyQuery {
  const f = new Function('builder', `return builder.${queryString};`);
  return f(builder) as AnyQuery;
}

function resolveHandshakeHeaders(config: {
  cookie?: string | undefined;
  headersJson?: string | undefined;
}): Record<string, string> {
  const headers: Record<string, string> = {};
  if (config.headersJson !== undefined) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(config.headersJson);
    } catch (e) {
      colorConsole.error(
        `--headers-json is not valid JSON: ${e instanceof Error ? e.message : String(e)}`,
      );
      process.exit(1);
    }
    if (
      parsed === null ||
      typeof parsed !== 'object' ||
      Array.isArray(parsed)
    ) {
      colorConsole.error('--headers-json must be a JSON object.');
      process.exit(1);
    }
    for (const [k, val] of Object.entries(parsed)) {
      if (typeof val !== 'string') {
        colorConsole.error(
          `--headers-json values must be strings; got ${typeof val} for "${k}".`,
        );
        process.exit(1);
      }
      headers[k] = val;
    }
  }
  if (config.cookie !== undefined) {
    headers.cookie = config.cookie;
  }
  return headers;
}

function installWebSocketHeaderShim(headers: Record<string, string>): void {
  class HeaderInjectingWebSocket extends NodeWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols, {headers});
    }
  }
  (globalThis as {WebSocket?: unknown}).WebSocket = HeaderInjectingWebSocket;
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
