/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
    wss = new WebSocketServer({noServer: true});
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
});
