import type {LogContext} from '@rocicorp/logger';
import {IncomingMessage, Server} from 'node:http';
import {Socket} from 'node:net';
import {WebSocketServer, type WebSocket} from 'ws';
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
  const wss = new WebSocketServer({noServer: true});
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
      const errMsg = String(error);
      lc.warn?.(`dispatch error: ${errMsg}`, error);
      // Returning an error on the HTTP handshake looks like a hanging connection
      // (at least from Chrome) and doesn't report any meaningful error in the browser.
      // Instead, finish the upgrade to a websocket and then close it with an error.
      wss.handleUpgrade(message as IncomingMessage, socket, head, ws =>
        ws.close(1002 /* "protocol error" */, truncate(errMsg)),
      );
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

// close messages must be less than or equal to 123 bytes:
// https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
function truncate(val: string, maxBytes = 123) {
  const encoder = new TextEncoder();
  if (encoder.encode(val).length <= maxBytes) {
    return val;
  }
  val = val.substring(0, maxBytes - 3);
  while (encoder.encode(val + '...').length > maxBytes) {
    val = val.substring(0, val.length - 1);
  }
  return val + '...';
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
