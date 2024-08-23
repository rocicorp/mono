import {FileHandle} from 'node:fs/promises';
import {IncomingMessage, Server} from 'node:http';
import {Socket} from 'node:net';
import {MessagePort, Worker} from 'node:worker_threads';
import WebSocket from 'ws';

export type WebSocketHandoff<P> = (message: IncomingMessage) => {
  payload: P;
  receiver: Worker | MessagePort;
};

export type WebSocketReceiver<P> = (ws: WebSocket, payload: P) => void;

export function installWebSocketHandoff<P>(
  server: Server,
  handoff: WebSocketHandoff<P>,
) {
  server.on('upgrade', (req, socket, head) => {
    try {
      const {payload, receiver} = handoff(req);
      const {headers, method = 'GET'} = req;
      const {
        _handle: {fd},
      } = socket as unknown as SocketWithFileHandle;

      const data = {
        message: {headers, method},
        fd,
        head,
        payload,
      } satisfies UpgradeRequest<P>;

      receiver.postMessage(data);
    } catch (error) {
      socket.write(`HTTP/1.1 400 Bad Request\r\n${String(error)}`);
      return;
    }
  });
}

export function installWebSocketReceiver<P>(
  server: WebSocket.Server,
  receiver: MessagePort,
  receive: WebSocketReceiver<P>,
) {
  receiver.on('message', msg => {
    const {message, fd, head, payload} = msg as UpgradeRequest<P>;
    const socket = new Socket({
      fd,
      readable: true,
      writable: true,
      allowHalfOpen: true,
    });

    server.handleUpgrade(
      message as IncomingMessage,
      socket,
      Buffer.from(head),
      ws => receive(ws, payload),
    );
  });
}

// https://github.com/nodejs/help/issues/1312#issuecomment-394138355
type SocketWithFileHandle = {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _handle: FileHandle;
};

// Contains the subset of http.IncomingRequest passed from the main thread
// to the syncer thread to hand off the upgrade of the request to a WebSocket.
// This is specific to the handoff receiver implementation
// WebSocket.Server.handleUpgrade(), which takes the http.IncomingMessage type but only
// inspects the "headers" and "method" fields. It is the solution recommended
// by the author of the 'ws' library:
// https://github.com/websockets/ws/issues/154#issuecomment-304511349
type IncomingMessageSubset = {
  headers: Record<string, string | string[] | undefined>;
  method: string;
};

type UpgradeRequest<P> = {
  message: IncomingMessageSubset;
  fd: number;
  head: ArrayBuffer;
  payload: P;
};
