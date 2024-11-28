import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import type {CloseEvent, Data, ErrorEvent} from 'ws';
import WebSocket from 'ws';
import {unreachable} from '../../../shared/src/asserts.js';
import {randomCharacters} from '../../../shared/src/random-values.js';
import * as valita from '../../../shared/src/valita.js';
import {
  type ConnectedMessage,
  type Downstream,
  ErrorKind,
  type ErrorMessage,
  type PongMessage,
  upstreamSchema,
} from '../../../zero-protocol/src/mod.js';
import type {ZeroConfig} from '../config/zero-config.js';
import type {ConnectParams} from '../services/dispatcher/connect-params.js';
import type {Mutagen} from '../services/mutagen/mutagen.js';
import type {
  SyncContext,
  TokenData,
  ViewSyncer,
} from '../services/view-syncer/view-syncer.js';
import {findErrorForClient} from '../types/error-for-client.js';
import type {Source} from '../types/streams.js';

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
    config: ZeroConfig,
    tokenData: TokenData | undefined,
    viewSyncer: ViewSyncer,
    mutagen: Mutagen,
    connectParams: ConnectParams,
    ws: WebSocket,
    onClose: () => void,
  ) {
    this.#ws = ws;
    this.#authData = tokenData?.decoded;
    const {clientGroupID, clientID, wsID, baseCookie, schemaVersion} =
      connectParams;
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

    const connectedMessage: ConnectedMessage = [
      'connected',
      {wsid: wsID, timestamp: Date.now()},
    ];
    send(ws, connectedMessage);
    this.#warmConnection(config);
  }

  close() {
    if (this.#closed) {
      return;
    }
    this.#lc.debug?.('close');
    this.#closed = true;
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

  // Landing this to gather some data on time savings, if any.
  #warmConnection(config: ZeroConfig) {
    if (config.warmWebsocket) {
      for (let i = 0; i < config.warmWebsocket; i++) {
        send(this.#ws, [
          'warm',
          {
            payload: randomCharacters(1024),
          },
        ]);
      }
    }
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
      this.#closeWithError(['error', ErrorKind.InvalidMessage, String(e)], e);
      return;
    }
    try {
      const msgType = msg[0];
      switch (msgType) {
        case 'ping':
          this.send(['pong', {}] satisfies PongMessage);
          break;
        case 'push': {
          const {clientGroupID, mutations, schemaVersion} = msg[1];
          if (clientGroupID !== this.#clientGroupID) {
            this.#closeWithError([
              'error',
              ErrorKind.InvalidPush,
              `clientGroupID in mutation "${clientGroupID}" does not match ` +
                `clientGroupID of connection "${this.#clientGroupID}`,
            ]);
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
                this.sendError(['error', maybeError[0], maybeError[1]]);
              }
            }
          });
          break;
        }
        case 'pull':
          lc.error?.('TODO: implement pull');
          break;
        case 'changeDesiredQueries':
          await viewSyncer.changeDesiredQueries(this.#syncContext, msg);
          break;
        case 'deleteClients':
          lc.error?.('TODO: implement deleteClients');
          break;
        case 'initConnection': {
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
    this.#lc.info?.('WebSocket close event', {code, reason, wasClean});
    this.close();
  };

  #handleError = (e: ErrorEvent) => {
    this.#lc.error?.('WebSocket error event', e.message, e.error);
  };

  async #proxyOutbound(outboundStream: Source<Downstream>) {
    try {
      for await (const outMsg of outboundStream) {
        this.send(outMsg);
      }
      this.#lc.info?.('downstream closed by ViewSyncer');
      this.close();
    } catch (e) {
      this.#closeWithThrown(e);
    }
  }

  #closeWithThrown(e: unknown) {
    const errorMessage = findErrorForClient(e)?.errorMessage ?? [
      'error',
      ErrorKind.Internal,
      String(e),
    ];
    this.#closeWithError(errorMessage, e);
  }

  #closeWithError(errorMessage: ErrorMessage, thrown?: unknown) {
    this.sendError(errorMessage, thrown);
    this.close();
  }

  send(data: Downstream) {
    send(this.#ws, data);
  }

  sendError(errorMessage: ErrorMessage, thrown?: unknown) {
    sendError(this.#lc, this.#ws, errorMessage, thrown);
  }
}

export function send(ws: WebSocket, data: Downstream) {
  ws.send(JSON.stringify(data));
}

export function sendError(
  lc: LogContext,
  ws: WebSocket,
  errorMessage: ErrorMessage,
  thrown?: unknown,
) {
  lc = lc.withContext('errorKind', errorMessage[1]);
  const logLevel = thrown ? 'error' : 'info';
  lc[logLevel]?.('Sending error on WebSocket', errorMessage, thrown ?? '');
  send(ws, errorMessage);
}
