import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {CONNECT_URL_PATTERN} from './paths.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';
// TODO(mlaw): break dependency on reflect-server
import {Router, BaseContext} from 'reflect-server/router';
// TODO(mlaw): break dependency on reflect-server
import {
  requireUpgradeHeader,
  upgradeWebsocketResponse,
} from 'reflect-server/http-util';
import type {ErrorKind} from 'zero-protocol/src/error.js';
// TODO(mlaw): break dependency on reflect-server
import {closeWithError} from 'reflect-server/socket';
import {getConnectRequest} from '../connect.js';
import {Connection, send} from './connection.js';
import type {ConnectedMessage} from 'zero-protocol';
import {PostgresDB, postgresTypeConfig} from '../types/pg.js';
import postgres from 'postgres';

export class ServiceRunnerDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #router = new Router();
  readonly #clients = new Map<string, Connection>();
  readonly #db: PostgresDB;

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    this.#serviceRunner = new ServiceRunner(logSink, logLevel, state, env);
    this.#db = postgres(env.UPSTREAM_URI, {
      ...postgresTypeConfig(),
    });
    this.#lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );

    this.#initRoutes();
  }

  #initRoutes() {
    this.#router.register(CONNECT_URL_PATTERN, this.#connect);
  }

  #connect = async (_ctx: BaseContext, request: Request): Promise<Response> => {
    const error = requireUpgradeHeader(request, this.#lc);
    if (error) {
      return error;
    }

    const {0: clientWS, 1: serverWS} = new WebSocketPair();
    const url = new URL(request.url);
    serverWS.accept();

    await this.#handleConnection(serverWS, url);

    return upgradeWebsocketResponse(clientWS, request.headers);
  };

  // eslint-disable-next-line require-await
  #handleConnection = async (serverWS: WebSocket, url: URL) => {
    const closeWithErrorLocal = (ek: ErrorKind, msg: string) => {
      closeWithError(this.#lc, serverWS, ek, msg);
    };

    const {result, error} = getConnectRequest(url);
    if (error !== null) {
      closeWithErrorLocal('InvalidConnectionRequest', error);
      return;
    }
    const {clientGroupID, clientID, baseCookie, wsid} = result;

    const existing = this.#clients.get(clientID);
    if (existing) {
      this.#lc.info?.('closing old socket');
      existing.close();
      return;
    }

    const connection = new Connection(
      this.#lc,
      this.#db,
      this.#serviceRunner,
      clientGroupID,
      clientID,
      baseCookie,
      serverWS,
    );
    this.#clients.set(clientID, connection);

    serverWS.addEventListener('close', () => {
      // spin down services if we have
      // no more client connections for the client group?
      this.#lc.info?.('Connection closed', {clientID, wsid});
    });
    serverWS.addEventListener('error', e => {
      this.#lc.error?.('Unhandled error in ws connection', e);
    });

    const connectedMessage: ConnectedMessage = [
      'connected',
      {wsid, timestamp: Date.now()},
    ];
    send(serverWS, connectedMessage);
  };

  async fetch(request: Request): Promise<Response> {
    const lc = this.#lc.withContext('url', request.url);
    lc.info?.('Handling request:', request.url);

    try {
      return await this.#router.dispatch(request, {lc});
    } catch (e) {
      lc.error?.('Unhandled exception in fetch', e);
      return new Response(e instanceof Error ? e.message : String(e), {
        status: 500,
      });
    }
  }
}
