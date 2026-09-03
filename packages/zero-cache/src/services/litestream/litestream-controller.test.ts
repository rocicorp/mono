import {createServer, type IncomingMessage, type Server} from 'node:http';
import {tmpdir} from 'node:os';
import {json as readJSON} from 'node:stream/consumers';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../../shared/src/rand.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {LitestreamController} from './litestream-controller.ts';

const lc = createSilentLogContext();

// A fake control server standing in for litestream's own: real litestream
// endpoints (writeJSON / writeJSONError in server.go) always respond with
// JSON, on both success and error, which is the contract #post() relies on.
type Handler = (
  req: IncomingMessage,
) => Promise<{status: number; body: unknown}>;

async function startFakeServer(handler: Handler): Promise<Server> {
  const socketPath = `${tmpdir()}/litestream-controller-test-${randInt(1_000_000, 9_999_999)}.sock`;
  const server = createServer((req, res) => {
    void handler(req).then(({status, body}) => {
      res.writeHead(status, {'content-type': 'application/json'});
      res.end(JSON.stringify(body));
    });
  });
  await new Promise<void>(resolve => server.listen(socketPath, resolve));
  return server;
}

function socketPathOf(server: Server): string {
  const addr = server.address();
  if (typeof addr !== 'string') {
    throw new Error('expected a Unix socket server');
  }
  return addr;
}

describe('litestream/litestream-controller', () => {
  let server: Server | undefined;

  afterEach(async () => {
    if (server) {
      await new Promise<void>(resolve => server?.close(() => resolve()));
      server = undefined;
    }
  });

  test('sync() posts {path, wait} and resolves with the parsed response', async () => {
    const requests: unknown[] = [];
    server = await startFakeServer(async req => {
      requests.push(await readJSON(req));
      return {
        status: 200,
        body: {
          status: 'synced_local',
          path: '/data/replica.db',
          txid: 5,
          replicated_txid: 3,
        },
      };
    });
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    const result = await client.sync({wait: true});

    expect(result).toEqual({
      status: 'synced_local',
      path: '/data/replica.db',
      txid: 5,
      replicated_txid: 3,
    });
    expect(requests).toEqual([{path: '/data/replica.db', wait: true}]);
    client.close();
  });

  test('defaults wait to false', async () => {
    const requests: unknown[] = [];
    server = await startFakeServer(async req => {
      requests.push(await readJSON(req));
      return {
        status: 200,
        body: {
          status: 'no_change',
          path: '/data/replica.db',
          txid: 1,
          replicated_txid: 1,
        },
      };
    });
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    await client.sync();

    expect(requests).toEqual([{path: '/data/replica.db', wait: false}]);
    client.close();
  });

  test('rejects with the parsed error body on a non-2xx response', async () => {
    server = await startFakeServer(() =>
      Promise.resolve({
        status: 409,
        body: {error: 'db not open', details: null},
      }),
    );
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    await expect(client.sync()).rejects.toThrow(
      /litestream \/sync failed: 409.*db not open/,
    );
    client.close();
  });

  test('rejects distinctly on a malformed (non-JSON) response body', async () => {
    server = createServer((_req, res) => {
      res.writeHead(200, {'content-type': 'text/plain'});
      res.end('not json');
    });
    await new Promise<void>(resolve =>
      server?.listen(
        `${tmpdir()}/litestream-controller-test-${randInt(1_000_000, 9_999_999)}.sock`,
        resolve,
      ),
    );
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    await expect(client.sync()).rejects.toThrow(/invalid JSON/);
    client.close();
  });

  test('rejects on timeout', async () => {
    server = createServer((_req, res) => {
      // Never responds within the client's timeout.
      setTimeout(() => res.end('{}'), 10_000);
    });
    await new Promise<void>(resolve =>
      server?.listen(
        `${tmpdir()}/litestream-controller-test-${randInt(1_000_000, 9_999_999)}.sock`,
        resolve,
      ),
    );
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    await expect(client.sync({timeoutMs: 50})).rejects.toThrow(/timed out/);
    client.close();
  });

  test('an AbortSignal cancels an in-flight request', async () => {
    server = await startFakeServer(async () => {
      await sleep(10_000);
      return {status: 200, body: {}};
    });
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    const ac = new AbortController();
    const pending = client.sync({}, ac.signal);
    ac.abort();

    await expect(pending).rejects.toThrow();
    client.close();
  });

  test('reuses a single connection across calls (keepAlive)', async () => {
    let connections = 0;
    server = await startFakeServer(async req => {
      await readJSON(req);
      return {
        status: 200,
        body: {status: 'no_change', path: 'x', txid: 0, replicated_txid: 0},
      };
    });
    server.on('connection', () => {
      connections++;
    });
    const client = new LitestreamController(
      lc,
      '/data/replica.db',
      socketPathOf(server),
    );

    await client.sync();
    await client.sync();
    await client.sync();

    expect(connections).toBe(1);
    client.close();
  });
});
