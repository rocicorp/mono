import type {LogContext} from '@rocicorp/logger';
import {IncomingMessage, Server} from 'node:http';
import {Socket} from 'node:net';
import type {WebSocket, WebSocketServer} from 'ws';
import {
  serializableSubset,
  type IncomingMessageSubset,
} from '../../types/http.js';
import {
  MESSAGE_TYPES,
  type Receiver,
  type Sender,
  type Worker,
} from '../../types/processes.js';

export type WebSocketHandoff<P> = (message: IncomingMessageSubset) => {
  payload: P;
  receiver: Receiver;
};

export type WebSocketReceiver<P> = (ws: WebSocket, payload: P) => void;

/**
 * Installs websocket handoff logic from either an http.Server
 * receiving requests, or a parent Worker process
 * that is handing off requests to this process.
 */
export function installWebSocketHandoff<P>(
  lc: LogContext,
  handoff: WebSocketHandoff<P>,
  source: Server | Worker,
) {
  const handle = (
    message: IncomingMessageSubset,
    socket: Socket,
    head: Buffer,
  ) => {
    try {
      const {payload, receiver} = handoff(message);
      const data = [
        'handoff',
        {
          message: serializableSubset(message),
          head,
          payload,
        },
      ] satisfies Handoff<P>;

      // "This event is guaranteed to be passed an instance of the <net.Socket> class"
      // https://nodejs.org/api/http.html#event-upgrade
      receiver.send(data, socket as Socket);
    } catch (error) {
      lc.warn?.(`dispatch error: ${String(error)}`, error);
      socket.write(`HTTP/1.1 400 Bad Request\r\n${String(error)}`);
      return;
    }
  };

  if (source instanceof Server) {
    // handoff messages from an HTTP server
    source.on('upgrade', handle);
  } else {
    // handoff messages from this worker's parent.
    source.onMessageType<Handoff<P>>('handoff', (msg, socket) => {
      const {message, head} = msg;
      handle(message, socket as Socket, Buffer.from(head));
    });
  }
}

export function installWebSocketReceiver<P>(
  server: WebSocketServer,
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

type Handoff<P> = [
  typeof MESSAGE_TYPES.handoff,
  {
    message: IncomingMessageSubset;
    head: ArrayBuffer;
    payload: P;
  },
];
