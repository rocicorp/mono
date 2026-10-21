/**
 * Runs the server side of the Swift end-to-end tests: a fresh Postgres
 * database loaded from `fixture.sql`, an API server for the app's named
 * queries and mutators, and zero-cache from this repository's source.
 *
 *   node packages/zero-conformance/src/e2e/server.ts
 *
 * Needs Node 24 (zero-cache's source uses `using`) and `psql` on the PATH.
 * Prints `ready <socket origin>` once zero-cache serves, and stops everything
 * on SIGINT or SIGTERM.
 *
 * Environment:
 *   ZERO_E2E_PG        admin connection URL
 *                      (default postgresql://postgres@127.0.0.1:5432/postgres)
 *   ZERO_E2E_DB        database to (re)create (default zero_e2e)
 *   ZERO_E2E_PORT      zero-cache port (default 4848)
 *   ZERO_E2E_API_PORT  API server port (default 4850)
 *   ZERO_E2E_DIR       working directory for the replica (default a temp dir)
 */
import {execFileSync, spawn} from 'node:child_process';
import {mkdtempSync} from 'node:fs';
import {createServer, type IncomingMessage} from 'node:http';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {zeroPostgresJS} from '../../../zero-server/src/adapters/postgresjs.ts';
import {handleMutateRequest} from '../../../zero-server/src/process-mutations.ts';
import {handleQueryRequest} from '../../../zero-server/src/queries/process-queries.ts';
import {mustGetMutator} from '../../../zql/src/mutate/mutator-registry.ts';
import {mustGetQuery} from '../../../zql/src/query/query-registry.ts';
import {mutators, queries, schema} from './app.ts';

const env = process.env;
const adminURL =
  env['ZERO_E2E_PG'] ?? 'postgresql://postgres@127.0.0.1:5432/postgres';
const dbName = env['ZERO_E2E_DB'] ?? 'zero_e2e';
const port = Number(env['ZERO_E2E_PORT'] ?? 4848);
const apiPort = Number(env['ZERO_E2E_API_PORT'] ?? 4850);
const workDir = env['ZERO_E2E_DIR'] ?? mkdtempSync(join(tmpdir(), 'zero-e2e-'));

const here = (path: string) => fileURLToPath(new URL(path, import.meta.url));
const upstreamURL = (() => {
  const url = new URL(adminURL);
  url.pathname = `/${dbName}`;
  return url.toString();
})();

function psql(url: string, ...args: string[]) {
  execFileSync('psql', [url, '-v', 'ON_ERROR_STOP=1', '-q', ...args], {
    stdio: ['ignore', 'ignore', 'inherit'],
  });
}

psql(adminURL, '-c', `DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`);
psql(adminURL, '-c', `CREATE DATABASE ${dbName}`);
psql(upstreamURL, '-f', here('./fixture.sql'));

const dbProvider = zeroPostgresJS(schema, upstreamURL);

async function readJSON(req: IncomingMessage): Promise<ReadonlyJSONValue> {
  let body = '';
  for await (const chunk of req) {
    body += chunk;
  }
  return JSON.parse(body) as ReadonlyJSONValue;
}

const api = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', `http://${req.headers.host}`);
  try {
    let response: unknown;
    if (req.method === 'POST' && url.pathname === '/query') {
      response = await handleQueryRequest({
        handler: (name, args) =>
          mustGetQuery(queries, name).fn({args, ctx: undefined}),
        schema,
        query: Object.fromEntries(url.searchParams),
        body: await readJSON(req),
        userID: null,
        logLevel: 'info',
      });
    } else if (req.method === 'POST' && url.pathname === '/mutate') {
      response = await handleMutateRequest({
        dbProvider,
        handler: transact =>
          transact((tx, name, args) =>
            // The registry is typed over the default schema, not this one.
            mustGetMutator(mutators, name).fn({
              tx: tx as never,
              args,
              ctx: undefined,
            }),
          ),
        query: Object.fromEntries(url.searchParams),
        body: await readJSON(req),
        userID: null,
        logLevel: 'info',
      });
    } else {
      res.writeHead(404).end();
      return;
    }
    res.writeHead(200, {'content-type': 'application/json'});
    res.end(JSON.stringify(response));
  } catch (e) {
    res.writeHead(500).end(String(e));
  }
});
await new Promise<void>(resolve => api.listen(apiPort, '127.0.0.1', resolve));

const zeroCache = spawn(
  process.execPath,
  [here('../../../zero-cache/src/server/runner/main.ts')],
  {
    stdio: ['ignore', 'inherit', 'inherit'],
    env: {
      ...env,
      ['NODE_ENV']: 'development',
      ['ZERO_UPSTREAM_DB']: upstreamURL,
      ['ZERO_REPLICA_FILE']: join(workDir, 'replica.db'),
      ['ZERO_PORT']: String(port),
      ['ZERO_QUERY_URL']: `http://127.0.0.1:${apiPort}/query`,
      ['ZERO_MUTATE_URL']: `http://127.0.0.1:${apiPort}/mutate`,
      ['ZERO_NUM_SYNC_WORKERS']: '1',
      ['ZERO_LOG_LEVEL']: env['ZERO_LOG_LEVEL'] ?? 'info',
    },
  },
);

function stop(code: number) {
  zeroCache.kill('SIGTERM');
  api.close();
  void dbProvider;
  process.exit(code);
}
zeroCache.on('exit', code => {
  process.stderr.write(`zero-cache exited with ${code}\n`);
  stop(1);
});
process.on('SIGINT', () => stop(0));
process.on('SIGTERM', () => stop(0));

for (let i = 0; ; i++) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/`);
    if (res.ok) {
      break;
    }
  } catch {
    // Not listening yet.
  }
  if (i > 600) {
    process.stderr.write('zero-cache did not start\n');
    stop(1);
  }
  await new Promise(resolve => setTimeout(resolve, 100));
}
process.stdout.write(`ready ws://127.0.0.1:${port}\n`);
