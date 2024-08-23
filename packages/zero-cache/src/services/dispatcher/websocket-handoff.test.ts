import {Server} from 'node:http';
import {Queue} from 'shared/src/queue.js';
import {randInt} from 'shared/src/rand.js';
import {afterAll, afterEach, beforeAll, describe, expect, test} from 'vitest';
import WebSocket from 'ws';
import {installWebSocketHandoff} from './websocket-handoff.js';

describe('dispatcher/websocket-handoff', () => {
  let port: number;
  let server: Server;
  let wss: WebSocket.Server;

  beforeAll(() => {
    port = randInt(10000, 20000);
    server = new Server();
    server.listen(port);
    wss = new WebSocket.Server({noServer: true});
  });

  afterEach(() => {
    server.removeAllListeners('upgrade');
  });

  afterAll(() => {
    server.close();
    wss.close();
  });

  test('handoff', async () => {
    const {port1, port2} = new MessageChannel();
    installWebSocketHandoff(server, () => ({
      payload: {foo: 'bar'},
      receiver: port1,
    }));

    const receiver = new Queue<unknown>();
    port2.on('message', msg => receiver.enqueue(msg));

    new WebSocket(`ws://localhost:${port}/`);

    expect(await receiver.dequeue()).toMatchObject({
      fd: expect.any(Number),
      head: expect.any(Uint8Array),
      message: {
        headers: expect.any(Object),
        method: 'GET',
      },
      payload: {
        foo: 'bar',
      },
    });

    // Note: Unfortunately, testing the receiving end in the same thread
    // (i.e. without running it in a Worker) results in an "Error: open EEXIST"
    // error when attempting to create the Socket object with the FileHandle.
    // The handoff only works when creating the Socket in a Worker thread,
    // which isn't easily testable in vitest.
  });
});
