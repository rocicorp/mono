import type {LogContext} from '@rocicorp/logger';
import {IncomingMessage, Server} from 'node:http';
import {Socket} from 'node:net';
import {WebSocketServer, type WebSocket} from 'ws';
import {assert} from '../../../../shared/src/asserts.ts';
import {
  serializableSubset,
  type IncomingMessageSubset,
} from '../../types/http.ts';
import {
  MESSAGE_TYPES,
  type Receiver,
  type Sender,
  type Worker,
} from '../../types/processes.ts';
import {closeWithError, PROTOCOL_ERROR} from '../../types/ws.ts';

export type HandoffSpec<P> = {
  payload: P;
  receiver: Receiver;
};

/**
 * The WebSocketHandoff is a function that either returns the payload and
 * receiver, or invokes the specified `callback` with the payload and
 * receiver. It must not do both.
 *
 * Similarly, an error can be handled by throwing synchronously from the
 * function, or invoking the `onerror` callback.
 */
export type WebSocketHandoff<P> = (
  message: IncomingMessageSubset,
  callback: (h: HandoffSpec<P>) => void,
  onerror: (reason: unknown) => void,
) => HandoffSpec<P> | void;

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
    let sent = false;

    function send({payload, receiver}: HandoffSpec<P>) {
      assert(!sent, 'Handoff callback already invoked');
      sent = true;

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
      receiver.send(data, socket);
    }

    function onError(error: unknown) {
      // Returning an error on the HTTP handshake looks like a hanging connection
      // (at least from Chrome) and doesn't report any meaningful error in the browser.
      // Instead, finish the upgrade to a websocket and then close it with an error.
      wss.handleUpgrade(message as IncomingMessage, socket, head, ws =>
        closeWithError(lc, ws, error, PROTOCOL_ERROR),
      );
    }

    try {
      const spec = handoff(message, send, onError);
      if (spec) {
        send(spec);
      }
    } catch (error) {
      onError(error);
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
  lc: LogContext,
  server: WebSocketServer,
  receive: WebSocketReceiver<P>,
  sender: Sender,
) {
  sender.onMessageType<Handoff<P>>('handoff', (msg, socket) => {
    // Per https://nodejs.org/api/child_process.html#subprocesssendmessage-sendhandle-options-callback
    //
    // > Any 'message' handlers in the subprocess should verify that socket
    // > exists, as the connection may have been closed during the time it
    // > takes to send the connection to the child.
    if (!socket) {
      lc.warn?.('websocket closed during handoff');
      return;
    }
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
