/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {resolver} from '@rocicorp/resolver';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import WebSocket, {WebSocketServer} from 'ws';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {closeWithError, PROTOCOL_ERROR} from './ws.ts';

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
      closeWithError(
        createSilentLogContext(),
        ws,
        'こんにちは' + 'あ'.repeat(150),
        PROTOCOL_ERROR,
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
