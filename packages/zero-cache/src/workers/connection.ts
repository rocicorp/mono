import type {LogContext} from '@rocicorp/logger';
import {pipeline, Readable, Writable} from 'node:stream';
import type {CloseEvent, Data, ErrorEvent} from 'ws';
import WebSocket, {createWebSocketStream} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import * as valita from '../../../shared/src/valita.ts';
import type {ConnectedMessage} from '../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import {
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../../zero-protocol/src/protocol-version.ts';
import {upstreamSchema, type Upstream} from '../../../zero-protocol/src/up.ts';
import {
  ErrorWithLevel,
  findErrorForClient,
  getLogLevel,
} from '../types/error-for-client.ts';
import type {Source} from '../types/streams.ts';
import type {ConnectParams} from './connect-params.ts';

export type HandlerResult =
  | {
      type: 'ok';
    }
  | {
      type: 'fatal';
      error: ErrorBody;
    }
  | {
      type: 'transient';
      errors: ErrorBody[];
    }
  | StreamResult;

export type StreamResult = {
  type: 'stream';
  source: 'viewSyncer' | 'pusher';
  stream: Source<Downstream>;
};

export interface MessageHandler {
  handleMessage(msg: Upstream): Promise<HandlerResult[]>;
}

// Ensures that a downstream message is sent at least every interval, sending a
// 'pong' if necessary. This is set to be slightly longer than the client-side
// PING_INTERVAL of 5 seconds, so that in the common case, 'pong's are sent in
// response to client-initiated 'ping's. However, if the inbound stream is
// backed up because a command is taking a long time to process, the pings
// will be stuck in the queue (i.e. back-pressured), in which case pongs will
// be manually sent to notify the client of server liveness.
//
// This is equivalent to what is done for Postgres keepalives on the
// replication stream (which can similarly be back-pressured):
// https://github.com/rocicorp/mono/blob/f98cb369a2dbb15650328859c732db358f187ef0/packages/zero-cache/src/services/change-source/pg/logical-replication/stream.ts#L21
const DOWNSTREAM_MSG_INTERVAL_MS = 6_000;

/**
 * Represents a connection between the client and server.
 *
 * Handles incoming messages on the connection and dispatches
 * them to the correct service.
 *
 * Listens to the ViewSyncer and sends messages to the client.
 */
export class Connection {
  readonly #ws: WebSocket;
  readonly #wsID: string;
  readonly #protocolVersion: number;
  readonly #lc: LogContext;
  readonly #onClose: () => void;
  readonly #messageHandler: MessageHandler;
  readonly #downstreamMsgTimer: NodeJS.Timeout | undefined;

  #viewSyncerOutboundStream: Source<Downstream> | undefined;
  #pusherOutboundStream: Source<Downstream> | undefined;
  #closed = false;

  constructor(
    lc: LogContext,
    connectParams: ConnectParams,
    ws: WebSocket,
    messageHandler: MessageHandler,
    onClose: () => void,
  ) {
    const {clientGroupID, clientID, wsID, protocolVersion} = connectParams;
    this.#messageHandler = messageHandler;

    this.#ws = ws;
    this.#wsID = wsID;
    this.#protocolVersion = protocolVersion;

    this.#lc = lc
      .withContext('connection')
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#lc.debug?.('new connection');
    this.#onClose = onClose;

    this.#ws.addEventListener('close', this.#handleClose);
    this.#ws.addEventListener('error', this.#handleError);

    this.#proxyInbound();
    this.#downstreamMsgTimer = setInterval(
      this.#maybeSendPong,
      DOWNSTREAM_MSG_INTERVAL_MS / 2,
    );
  }

  /**
   * Checks the protocol version and errors for unsupported protocols,
   * sending the initial `connected` response on success.
   *
   * This is early in the connection lifecycle because {@link #handleMessage}
   * will only parse messages with schema(s) of supported protocol versions.
   */
  init(): boolean {
    if (
      this.#protocolVersion > PROTOCOL_VERSION ||
      this.#protocolVersion < MIN_SERVER_SUPPORTED_SYNC_PROTOCOL
    ) {
      this.#closeWithError({
        kind: ErrorKind.VersionNotSupported,
        message: `server is at sync protocol v${PROTOCOL_VERSION} and does not support v${
          this.#protocolVersion
        }. The ${
          this.#protocolVersion > PROTOCOL_VERSION ? 'server' : 'client'
        } must be updated to a newer release.`,
      });
    } else {
      const connectedMessage: ConnectedMessage = [
        'connected',
        {wsid: this.#wsID, timestamp: Date.now()},
      ];
      this.send(connectedMessage, 'ignore-backpressure');
      return true;
    }
    return false;
  }

  close(reason: string, ...args: unknown[]) {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#lc.info?.(`closing connection: ${reason}`, ...args);
    this.#ws.removeEventListener('close', this.#handleClose);
    this.#ws.removeEventListener('error', this.#handleError);
    this.#viewSyncerOutboundStream?.cancel();
    this.#viewSyncerOutboundStream = undefined;
    this.#pusherOutboundStream?.cancel();
    this.#pusherOutboundStream = undefined;
    this.#onClose();
    if (this.#ws.readyState !== this.#ws.CLOSED) {
      this.#ws.close();
    }
    clearTimeout(this.#downstreamMsgTimer);

    // spin down services if we have
    // no more client connections for the client group?
  }

  handleInitConnection(initConnectionMsg: string) {
    return this.#handleMessage({data: initConnectionMsg});
  }

  #handleMessage = async (event: {data: Data}) => {
    const data = event.data.toString();
    if (this.#closed) {
      this.#lc.debug?.('Ignoring message received after closed', data);
      return;
    }

    let msg;
    try {
      const value = JSON.parse(data);
      msg = valita.parse(value, upstreamSchema);
    } catch (e) {
      this.#lc.warn?.(`failed to parse message "${data}": ${String(e)}`);
      this.#closeWithError(
        {kind: ErrorKind.InvalidMessage, message: String(e)},
        e,
      );
      return;
    }

    try {
      const msgType = msg[0];
      if (msgType === 'ping') {
        this.send(['pong', {}], 'ignore-backpressure');
        return;
      }

      const result = await this.#messageHandler.handleMessage(msg);
      for (const r of result) {
        this.#handleMessageResult(r);
      }
    } catch (e) {
      this.#closeWithThrown(e);
    }
  };

  #handleMessageResult(result: HandlerResult): void {
    switch (result.type) {
      case 'fatal':
        this.#closeWithError(result.error);
        break;
      case 'ok':
        break;
      case 'stream': {
        switch (result.source) {
          case 'viewSyncer':
            assert(
              this.#viewSyncerOutboundStream === undefined,
              'Outbound stream already set for this connection!',
            );
            this.#viewSyncerOutboundStream = result.stream;
            break;
          case 'pusher':
            assert(
              this.#pusherOutboundStream === undefined,
              'Outbound stream already set for this connection!',
            );
            this.#pusherOutboundStream = result.stream;
            break;
        }
        this.#proxyOutbound(result.stream);
        break;
      }
      case 'transient': {
        for (const error of result.errors) {
          this.sendError(error);
        }
      }
    }
  }

  #handleClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    this.close('WebSocket close event', {code, reason, wasClean});
  };

  #handleError = (e: ErrorEvent) => {
    this.#lc.error?.('WebSocket error event', e.message, e.error);
  };

  #proxyInbound() {
    pipeline(
      createWebSocketStream(this.#ws),
      new Writable({
        write: (data, _encoding, callback) => {
          this.#handleMessage({data}).then(() => callback(), callback);
        },
      }),
      // The done callback is not used, as #handleClose and #handleError,
      // configured on the underlying WebSocket, provide more complete
      // information.
      () => {},
    );
  }

  #proxyOutbound(outboundStream: Source<Downstream>) {
    // Note: createWebSocketStream() is avoided here in order to control
    //       exception handling with #closeWithThrown(). If the Writable
    //       from createWebSocketStream() were instead used, exceptions
    //       from the outboundStream result in the Writable closing the
    //       the websocket before the error message can be sent.
    pipeline(
      Readable.from(outboundStream),
      new Writable({
        objectMode: true,
        write: (downstream: Downstream, _encoding, callback) =>
          this.send(downstream, callback),
      }),
      e =>
        e
          ? this.#closeWithThrown(e)
          : this.close(`downstream closed by ViewSyncer`),
    );
  }

  #closeWithThrown(e: unknown) {
    const errorBody = findErrorForClient(e)?.errorBody ?? {
      kind: ErrorKind.Internal,
      message: String(e),
    };

    this.#closeWithError(errorBody, e);
  }

  #closeWithError(errorBody: ErrorBody, thrown?: unknown) {
    this.sendError(errorBody, thrown);
    this.close(`client error: ${errorBody.kind}`, errorBody);
  }

  #lastDownstreamMsgTime = Date.now();

  #maybeSendPong = () => {
    if (Date.now() - this.#lastDownstreamMsgTime > DOWNSTREAM_MSG_INTERVAL_MS) {
      this.#lc.debug?.('manually sending pong');
      this.send(['pong', {}], 'ignore-backpressure');
    }
  };

  send(
    data: Downstream,
    callback: ((err?: Error | null) => void) | 'ignore-backpressure',
  ) {
    this.#lastDownstreamMsgTime = Date.now();
    return send(this.#lc, this.#ws, data, callback);
  }

  sendError(errorBody: ErrorBody, thrown?: unknown) {
    sendError(this.#lc, this.#ws, errorBody, thrown);
  }
}

// Exported for testing purposes.
export function send(
  lc: LogContext,
  ws: Pick<WebSocket, 'readyState' | 'send'>,
  data: Downstream,
  callback: ((err?: Error | null) => void) | 'ignore-backpressure',
) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(
      JSON.stringify(data),
      callback === 'ignore-backpressure' ? undefined : callback,
    );
  } else {
    lc.debug?.(`Dropping outbound message on ws (state: ${ws.readyState})`, {
      dropped: data,
    });
    if (callback !== 'ignore-backpressure') {
      callback(new ErrorWithLevel('websocket closed', 'info'));
    }
  }
}

export function sendError(
  lc: LogContext,
  ws: WebSocket,
  errorBody: ErrorBody,
  thrown?: unknown,
) {
  lc = lc.withContext('errorKind', errorBody.kind);
  const logLevel = thrown ? getLogLevel(thrown) : 'info';
  lc[logLevel]?.('Sending error on WebSocket', errorBody, thrown ?? '');
  send(lc, ws, ['error', errorBody], 'ignore-backpressure');
}
