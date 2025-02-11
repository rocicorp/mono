import type {LogContext} from '@rocicorp/logger';
import type {CloseEvent, Data, ErrorEvent} from 'ws';
import WebSocket from 'ws';
import * as valita from '../../../shared/src/valita.ts';
import type {ConnectedMessage} from '../../../zero-protocol/src/connect.ts';
import type {Downstream} from '../../../zero-protocol/src/down.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import {type ErrorBody} from '../../../zero-protocol/src/error.ts';
import type {PongMessage} from '../../../zero-protocol/src/pong.ts';
import {
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL,
  PROTOCOL_VERSION,
} from '../../../zero-protocol/src/protocol-version.ts';
import {upstreamSchema, type Upstream} from '../../../zero-protocol/src/up.ts';
import type {ConnectParams} from '../services/dispatcher/connect-params.ts';
import {findErrorForClient, getLogLevel} from '../types/error-for-client.ts';
import type {Source} from '../types/streams.ts';
import {assert} from '../../../shared/src/asserts.ts';

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
  | {
      type: 'stream';
      stream: Source<Downstream>;
    };

export interface MessageHandler {
  handleMessage(msg: Upstream): Promise<HandlerResult>;
}

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

  #outboundStream: Source<Downstream> | undefined;
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
    this.#onClose = onClose;

    this.#ws.addEventListener('message', this.#handleMessage);
    this.#ws.addEventListener('close', this.#handleClose);
    this.#ws.addEventListener('error', this.#handleError);
  }

  /**
   * Checks the protocol version and errors for unsupported protocols,
   * sending the initial `connected` response on success.
   *
   * This is early in the connection lifecycle because {@link #handleMessage}
   * will only parse messages with schema(s) of supported protocol versions.
   */
  init() {
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
      send(this.#ws, connectedMessage);
    }
  }

  close(reason: string, ...args: unknown[]) {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#lc.info?.(`closing connection: ${reason}`, ...args);
    this.#ws.removeEventListener('message', this.#handleMessage);
    this.#ws.removeEventListener('close', this.#handleClose);
    this.#ws.removeEventListener('error', this.#handleError);
    this.#outboundStream?.cancel();
    this.#outboundStream = undefined;
    this.#onClose();
    if (this.#ws.readyState !== this.#ws.CLOSED) {
      this.#ws.close();
    }

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
        this.send(['pong', {}] satisfies PongMessage);
        return;
      }
      const result = await this.#messageHandler.handleMessage(msg);
      switch (result.type) {
        case 'fatal':
          this.#closeWithError(result.error);
          break;
        case 'ok':
          break;
        case 'stream': {
          assert(
            this.#outboundStream === undefined,
            'Outbound stream already set for this connection!',
          );
          this.#outboundStream = result.stream;
          void this.#proxyOutbound(result.stream);
          break;
        }
        case 'transient': {
          for (const error of result.errors) {
            this.sendError(error);
          }
        }
      }
    } catch (e) {
      this.#closeWithThrown(e);
    }
  };

  #handleClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    this.close('WebSocket close event', {code, reason, wasClean});
  };

  #handleError = (e: ErrorEvent) => {
    this.#lc.error?.('WebSocket error event', e.message, e.error);
  };

  async #proxyOutbound(outboundStream: Source<Downstream>) {
    try {
      for await (const outMsg of outboundStream) {
        this.send(outMsg);
      }
      this.close('downstream closed by ViewSyncer');
    } catch (e) {
      this.#closeWithThrown(e);
    }
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

  send(data: Downstream) {
    send(this.#ws, data);
  }

  sendError(errorBody: ErrorBody, thrown?: unknown) {
    sendError(this.#lc, this.#ws, errorBody, thrown);
  }
}

export function send(ws: WebSocket, data: Downstream) {
  ws.send(JSON.stringify(data));
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
  send(ws, ['error', errorBody]);
}
