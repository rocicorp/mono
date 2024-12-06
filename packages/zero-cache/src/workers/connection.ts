import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import type {CloseEvent, Data, ErrorEvent} from 'ws';
import WebSocket from 'ws';
import {unreachable} from '../../../shared/src/asserts.js';
import * as valita from '../../../shared/src/valita.js';
import {type ErrorBody} from '../../../zero-protocol/src/error.js';
import {
  type ConnectedMessage,
  type Downstream,
  ErrorKind,
  type PongMessage,
  upstreamSchema,
} from '../../../zero-protocol/src/mod.js';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.js';
import type {ConnectParams} from '../services/dispatcher/connect-params.js';
import type {Mutagen} from '../services/mutagen/mutagen.js';
import type {
  SyncContext,
  TokenData,
  ViewSyncer,
} from '../services/view-syncer/view-syncer.js';
import {findErrorForClient} from '../types/error-for-client.js';
import type {Source} from '../types/streams.js';
import {trace} from '@opentelemetry/api';
import {version} from '../../../otel/src/version.js';
import {startAsyncSpan} from '../../../otel/src/span.js';

const tracer = trace.getTracer('syncer-ws-server', version);

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
  readonly #clientGroupID: string;
  readonly #syncContext: SyncContext;
  readonly #lc: LogContext;
  readonly #onClose: () => void;

  readonly #viewSyncer: ViewSyncer;
  readonly #mutagen: Mutagen;
  readonly #mutationLock = new Lock();
  readonly #authData: JWTPayload | undefined;

  #outboundStream: Source<Downstream> | undefined;
  #closed = false;

  constructor(
    lc: LogContext,
    tokenData: TokenData | undefined,
    viewSyncer: ViewSyncer,
    mutagen: Mutagen,
    connectParams: ConnectParams,
    ws: WebSocket,
    onClose: () => void,
  ) {
    const {
      clientGroupID,
      clientID,
      wsID,
      baseCookie,
      protocolVersion,
      schemaVersion,
    } = connectParams;

    this.#ws = ws;
    this.#authData = tokenData?.decoded;
    this.#wsID = wsID;
    this.#protocolVersion = protocolVersion;
    this.#clientGroupID = clientGroupID;
    this.#syncContext = {clientID, wsID, baseCookie, schemaVersion, tokenData};
    this.#lc = lc
      .withContext('connection')
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#onClose = onClose;

    this.#viewSyncer = viewSyncer;
    this.#mutagen = mutagen;

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
      this.#protocolVersion !== PROTOCOL_VERSION &&
      this.#protocolVersion !== PROTOCOL_VERSION - 1
    ) {
      this.#closeWithError({
        kind: ErrorKind.VersionNotSupported,
        message: `server supports v${
          PROTOCOL_VERSION - 1
        } and v${PROTOCOL_VERSION} protocols`,
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
    const lc = this.#lc;
    const data = event.data.toString();
    const viewSyncer = this.#viewSyncer;
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
      switch (msgType) {
        case 'ping':
          this.send(['pong', {}] satisfies PongMessage);
          break;
        case 'push': {
          await startAsyncSpan(tracer, 'connection.push', async () => {
            const {clientGroupID, mutations, schemaVersion} = msg[1];
            if (clientGroupID !== this.#clientGroupID) {
              this.#closeWithError({
                kind: ErrorKind.InvalidPush,
                message:
                  `clientGroupID in mutation "${clientGroupID}" does not match ` +
                  `clientGroupID of connection "${this.#clientGroupID}`,
              });
            }
            // Hold a connection-level lock while processing mutations so that:
            // 1. Mutations are processed in the order in which they are received and
            // 2. A single view syncer connection cannot hog multiple upstream connections.
            await this.#mutationLock.withLock(async () => {
              for (const mutation of mutations) {
                const maybeError = await this.#mutagen.processMutation(
                  mutation,
                  this.#authData,
                  schemaVersion,
                );
                if (maybeError !== undefined) {
                  this.sendError({kind: maybeError[0], message: maybeError[1]});
                }
              }
            });
          });
          break;
        }
        case 'pull':
          lc.error?.('TODO: implement pull');
          break;
        case 'changeDesiredQueries':
          await startAsyncSpan(
            tracer,
            'connection.changeDesiredQueries',
            async () => {
              await viewSyncer.changeDesiredQueries(this.#syncContext, msg);
            },
          );
          break;
        case 'deleteClients':
          lc.error?.('TODO: implement deleteClients');
          break;
        case 'initConnection': {
          await startAsyncSpan(
            tracer,
            'connection.initConnection',
            async () => {
              // TODO (mlaw): tell mutagens about the new token too
              this.#outboundStream = await viewSyncer.initConnection(
                this.#syncContext,
                msg,
              );
              if (this.#closed) {
                this.#outboundStream.cancel();
              } else {
                void this.#proxyOutbound(this.#outboundStream);
              }
            },
          );
          break;
        }
        default:
          unreachable(msgType);
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
  const logLevel = thrown ? 'error' : 'info';
  lc[logLevel]?.('Sending error on WebSocket', errorBody, thrown ?? '');
  send(ws, ['error', errorBody]);
}
