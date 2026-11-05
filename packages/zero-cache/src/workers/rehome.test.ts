import {once} from 'node:events';
import {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import {WebSocket, WebSocketServer} from 'ws';
import {rehome} from './rehome.ts';

test('a draining connection receives a short retry directive before closing', async () => {
  const server = new WebSocketServer({port: 0});
  await once(server, 'listening');
  const address = server.address();
  if (typeof address === 'string' || address === null) {
    throw new Error('Expected a TCP listener');
  }
  server.once('connection', ws => rehome(new LogContext('error'), ws));
  const client = new WebSocket(`ws://127.0.0.1:${address.port}`);
  try {
    const closed = once(client, 'close');
    const [message] = await once(client, 'message');
    expect(JSON.parse(message.toString())).toEqual([
      'error',
      {
        kind: 'Rehome',
        message: 'Server is draining',
        origin: 'zeroCache',
        maxBackoffMs: 250,
      },
    ]);
    const [code, reason] = await closed;
    expect(code).toBe(1001);
    expect(reason.toString()).toBe('Server is draining');
  } finally {
    client.terminate();
    for (const ws of server.clients) ws.terminate();
    await new Promise<void>((resolve, reject) =>
      server.close(error => (error ? reject(error) : resolve())),
    );
  }
});
