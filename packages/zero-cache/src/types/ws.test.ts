import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import WebSocket, {WebSocketServer} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {closeWithProtocolError} from './ws.ts';

describe('types/ws', () => {
  let port: number;
  let wss: WebSocketServer;

  beforeAll(() => {
    port = randInt(10000, 20000);
    wss = new WebSocketServer({port});
  });

  afterAll(() => {
    wss.close();
  });

  test('close with protocol error', async () => {
    wss.on('connection', ws =>
      closeWithProtocolError(
        createSilentLogContext(),
        ws,
        'こんにちは' + 'あ'.repeat(150),
      ),
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
        "reason": "こんにちはあああああああああああああああああああああああああああああああああああ...",
      }
    `);
    // close messages must be less than or equal to 123 bytes:
    // https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
    expect(new TextEncoder().encode(error.reason).length).toBeLessThanOrEqual(
      123,
    );
  });
});
