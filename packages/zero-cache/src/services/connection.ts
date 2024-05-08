import type {LogContext} from '@rocicorp/logger';
import * as valita from 'shared/src/valita.js';
import {Downstream, upstreamSchema} from 'zero-protocol';
import type {ServiceRunner} from './service-runner.js';
import type {
  SyncContext,
  ViewSyncerService,
} from './view-syncer/view-syncer.js';
// TODO(mlaw): break dependency on reflect-server
import {handlePing} from 'reflect-server/ping';
import {closeWithError} from 'reflect-server/socket';
import type {PostgresDB} from '../types/pg.js';
import type {CancelableAsyncIterable} from '../types/streams.js';
import {processMutation} from './mutagen/mutagen.js';

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
  readonly #syncContext: SyncContext;
  readonly #lc: LogContext;
  readonly #upstreamDB: PostgresDB;

  readonly #viewSyncer: ViewSyncerService;

  #outboundStream: CancelableAsyncIterable<Downstream> | undefined;

  constructor(
    lc: LogContext,
    db: PostgresDB,
    serviceRunner: ServiceRunner,
    clientGroupID: string,
    clientID: string,
    wsID: string,
    baseCookie: string | null,
    ws: WebSocket,
  ) {
    this.#ws = ws;
    this.#syncContext = {clientID, wsID, baseCookie};
    this.#lc = lc
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#upstreamDB = db;

    this.#viewSyncer = serviceRunner.getViewSyncer(clientGroupID);
    this.#ws.addEventListener('message', this.#onMessage);
  }

  close() {
    this.#ws.close();
    this.#outboundStream?.cancel();
    this.#outboundStream = undefined;
  }

  #onMessage = async (event: MessageEvent) => {
    const lc = this.#lc;
    const data = event.data.toString();
    const ws = this.#ws;
    const viewSyncer = this.#viewSyncer;

    lc.debug?.('Received message', data);
    let msg;
    try {
      const value = JSON.parse(data);
      msg = valita.parse(value, upstreamSchema);
    } catch (e) {
      closeWithError(lc, ws, 'InvalidMessage', String(e));
      return;
    }
    try {
      const msgType = msg[0];
      switch (msgType) {
        case 'ping':
          handlePing(lc, ws);
          break;
        case 'push':
          for (const mutation of msg[1].mutations) {
            await processMutation(lc, this.#upstreamDB, mutation);
          }
          break;
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
          this.#outboundStream = await viewSyncer.initConnection(
            this.#syncContext,
            msg,
          );

          void this.proxyOutbound(lc, ws, this.#outboundStream);
          break;
        }
        default:
          msgType satisfies never;
      }
    } catch (e) {
      // TODO: Determine the ErrorKind from a custom Error type for e.
      closeWithError(lc, ws, 'InvalidMessage', String(e));
      this.close();
    }
  };

  async proxyOutbound(
    lc: LogContext,
    ws: WebSocket,
    outboundStream: CancelableAsyncIterable<Downstream>,
  ) {
    try {
      for await (const outMsg of outboundStream) {
        send(ws, outMsg);
      }
      lc.info?.('downstream closed by ViewSyncer');
    } catch (e) {
      // TODO: Determine the ErrorKind from a custom Error type for e.
      closeWithError(lc, ws, 'InvalidMessage', String(e));
    } finally {
      this.close();
    }
  }
}

export function send(ws: WebSocket, data: Downstream) {
  ws.send(JSON.stringify(data));
}
