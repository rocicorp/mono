import {resolver} from '@rocicorp/resolver';
import {Server} from 'node:http';
import {afterAll, afterEach, beforeAll, describe, expect, test} from 'vitest';
import {WebSocket, WebSocketServer, type RawData} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {inProcChannel} from './processes.ts';
import {
  installWebSocketHandoff,
  installWebSocketReceiver,
} from './websocket-handoff.ts';

describe('types/websocket-handoff', () => {
  let port: number;
  let server: Server;
  let wss: WebSocketServer;
  const lc = createSilentLogContext();

  beforeAll(() => {
    port = randInt(10000, 20000);
    server = new Server();
    server.listen(port);
    wss = new WebSocketServer({
      noServer: true,
      perMessageDeflate: true,
    });
  });

  afterEach(() => {
    server.removeAllListeners('upgrade');
  });

  afterAll(() => {
    server.close();
    wss.close();
  });

  test('handoff', async () => {
    const [parent, child] = inProcChannel();

    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'boo'},
        sender: child,
      }),
      server,
    );

    installWebSocketReceiver(
      lc,
      wss,
      (ws, payload, m) => {
        ws.on('message', msg => {
          ws.send(
            `Received "${msg}" and payload ${JSON.stringify(payload)} at ${m.url}`,
          );
          ws.close();
        });
      },
      parent,
    );

    const {promise: reply, resolve} = resolver<RawData>();
    const ws = new WebSocket(`ws://localhost:${port}/foobar`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(String(await reply)).toBe(
      'Received "hello" and payload {"foo":"boo"} at /foobar',
    );
  });

  test('handoff callback', async () => {
    const [parent, child] = inProcChannel();

    installWebSocketHandoff(
      lc,
      (_, callback) =>
        callback({
          payload: {foo: 'boo'},
          sender: child,
        }),
      server,
    );

    installWebSocketReceiver(
      lc,
      wss,
      (ws, payload) => {
        ws.on('message', msg => {
          ws.send(`Received "${msg}" and payload ${JSON.stringify(payload)}`);
          ws.close();
        });
      },
      parent,
    );

    const {promise: reply, resolve} = resolver<RawData>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(String(await reply)).toBe(
      'Received "hello" and payload {"foo":"boo"}',
    );
  });

  test('double handoff', async () => {
    const [grandParent, parent1] = inProcChannel();
    const [parent2, child] = inProcChannel();

    // server(grandParent) to parent
    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'boo'},
        sender: grandParent,
      }),
      server,
    );

    // parent to child
    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'boo'},
        sender: parent2,
      }),
      parent1,
    );

    // child receives socket
    installWebSocketReceiver(
      lc,
      wss,
      (ws, payload) => {
        ws.on('message', msg => {
          ws.send(`Received "${msg}" and payload ${JSON.stringify(payload)}`);
          ws.close();
        });
      },
      child,
    );

    const {promise: reply, resolve} = resolver<RawData>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => ws.send('hello'));
    ws.on('message', msg => resolve(msg));

    expect(String(await reply)).toBe(
      'Received "hello" and payload {"foo":"boo"}',
    );
  });

  test('handoff error', async () => {
    installWebSocketHandoff(
      lc,
      () => {
        throw new Error('こんにちは' + 'あ'.repeat(150));
      },
      server,
    );

    const ws = new WebSocket(`ws://localhost:${port}/`);
    const {promise, resolve} = resolver<{code: number; reason: string}>();
    ws.on('close', (code, reason) =>
      resolve({code, reason: reason.toString('utf-8')}),
    );

    const error = await promise;
    expect(error).toMatchInlineSnapshot(`
      {
        "code": 1002,
        "reason": "Error: こんにちはああああああああああああああああああああああああああああああああ...",
      }
    `);
    // close messages must be less than or equal to 123 bytes:
    // https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
    expect(new TextEncoder().encode(error.reason).length).toBeLessThanOrEqual(
      123,
    );
  });

  test('compression enabled', async () => {
    const [parent, child] = inProcChannel();

    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'bar'},
        sender: child,
      }),
      server,
      {noServer: true, perMessageDeflate: true},
    );

    installWebSocketReceiver(
      lc,
      wss,
      ws => {
        ws.close();
      },
      parent,
    );

    const {promise, resolve} = resolver<string | undefined>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => {
      // Check that permessage-deflate extension was negotiated
      const extensions = ws.extensions;
      resolve(extensions);
    });

    const extensions = await promise;
    expect(extensions).toContain('permessage-deflate');
  });

  test('compression disabled', async () => {
    const [parent, child] = inProcChannel();

    // Create a separate WSS without compression for this test
    const wssNoCompression = new WebSocketServer({
      noServer: true,
    });

    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'bar'},
        sender: child,
      }),
      server,
      {noServer: true},
    );

    installWebSocketReceiver(
      lc,
      wssNoCompression,
      ws => {
        ws.close();
      },
      parent,
    );

    const {promise, resolve} = resolver<string | undefined>();
    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('open', () => {
      // Check that permessage-deflate extension was NOT negotiated
      const extensions = ws.extensions;
      resolve(extensions);
    });

    const extensions = await promise;
    expect(extensions).not.toContain('permessage-deflate');

    wssNoCompression.close();
  });

  test('websocket closed during upgrade is handled gracefully', async () => {
    const [parent, child] = inProcChannel();

    installWebSocketHandoff(
      lc,
      () => ({
        payload: {foo: 'boo'},
        sender: child,
      }),
      server,
    );

    let receiveCalled = false;
    const {promise: testComplete, resolve: completeTest} = resolver<void>();

    // Create a custom WSS that simulates a closed websocket after upgrade
    const wssWithClosedWs = new WebSocketServer({noServer: true});

    // Override handleUpgrade to close the websocket before calling the callback
    const originalHandleUpgrade =
      wssWithClosedWs.handleUpgrade.bind(wssWithClosedWs);
    wssWithClosedWs.handleUpgrade = (request, socket, head, callback) => {
      originalHandleUpgrade(request, socket, head, (ws, req) => {
        // Simulate the race condition: close the websocket before the callback runs
        ws.close();
        // Wait a tick for the close to take effect
        setTimeout(() => callback(ws, req), 10);
      });
    };

    installWebSocketReceiver(
      lc,
      wssWithClosedWs,
      () => {
        receiveCalled = true;
      },
      parent,
    );

    const ws = new WebSocket(`ws://localhost:${port}/`);
    ws.on('close', () => {
      // Give time for receive to potentially be called
      setTimeout(() => {
        completeTest();
      }, 50);
    });
    ws.on('error', () => {
      // Errors are expected when connection closes abruptly
    });

    await testComplete;

    // The receive callback should NOT have been called because
    // the websocket was closed during the handoff
    expect(receiveCalled).toBe(false);

    wssWithClosedWs.close();
  });
});
