import {IncomingMessage, Server} from 'node:http';
import {Socket} from 'node:net';
import WebSocket from 'ws';
import {MESSAGE_TYPES, Receiver, Sender} from '../../types/processes.js';

export type WebSocketHandoff<P> = (message: IncomingMessage) => {
  payload: P;
  receiver: Receiver;
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

      const data = [
        'handoff',
        {
          message: {headers, method},
          head,
          payload,
        },
      ] satisfies Handoff<P>;

      // "This event is guaranteed to be passed an instance of the <net.Socket> class"
      // https://nodejs.org/api/http.html#event-upgrade
      receiver.send(data, socket as Socket);
    } catch (error) {
      socket.write(`HTTP/1.1 400 Bad Request\r\n${String(error)}`);
      return;
    }
  });
}

export function installWebSocketReceiver<P>(
  server: WebSocket.Server,
  receive: WebSocketReceiver<P>,
  sender: Sender,
) {
  sender.onMessageType<Handoff<P>>('handoff', (msg, socket) => {
    const {message, head, payload} = msg;
    server.handleUpgrade(
      message as IncomingMessage,
      socket as Socket,
      Buffer.from(head),
      ws => receive(ws, payload),
    );
  });
}

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

type Handoff<P> = [
  typeof MESSAGE_TYPES.handoff,
  {
    message: IncomingMessageSubset;
    head: ArrayBuffer;
    payload: P;
  },
];
