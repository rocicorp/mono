import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {BaseContext, Router} from 'cf-shared/src/router.js';
import type {ConnectedMessage} from 'zero-protocol';
import type {ErrorKind} from 'zero-protocol/src/error.js';
import {getConnectRequest} from '../connect.js';
import {Connection, closeWithError, send} from './connection.js';
import {CONNECT_URL_PATTERN, STATUS_URL_PATTERN} from './paths.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';

export class ServiceRunnerDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #router = new Router();
  readonly #clients = new Map<string, Connection>();

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    this.#serviceRunner = new ServiceRunner(logSink, logLevel, state, env);
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );
    this.#lc = lc;
    void (async () => {
      const traceResponse = await fetch('https://cloudflare.com/cdn-cgi/trace');
      const traceText = await traceResponse.text();
      lc.info?.('Location information:\n', traceText);
    })();

    this.#initRoutes();
  }

  #initRoutes() {
    this.#router.register(CONNECT_URL_PATTERN, this.#connect);
    this.#router.register(STATUS_URL_PATTERN, this.#status);
  }

  #connect = async (_ctx: BaseContext, request: Request): Promise<Response> => {
    if (request.headers.get('Upgrade') !== 'websocket') {
      this.#lc.info?.('missing Upgrade header for', request.url);
      return new Response('expected websocket Upgrade header', {status: 400});
    }

    const {0: clientWS, 1: serverWS} = new WebSocketPair();
    const url = new URL(request.url);
    serverWS.accept();

    await this.#handleConnection(serverWS, url);

    // Sec-WebSocket-Protocol is being used as a mechanism for sending `auth`
    // since custom headers are not supported by the browser WebSocket API, the
    // Sec-WebSocket-Protocol semantics must be followed. Send a
    // Sec-WebSocket-Protocol response header with a value matching the
    // Sec-WebSocket-Protocol request header, to indicate support for the
    // protocol, otherwise the client will close the connection.
    const responseHeaders = new Headers();
    const protocol = request.headers.get('Sec-WebSocket-Protocol');
    if (protocol) {
      responseHeaders.set('Sec-WebSocket-Protocol', protocol);
    }
    return new Response(null, {
      status: 101,
      webSocket: clientWS,
      headers: responseHeaders,
    });
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
    }

    const connection = new Connection(
      this.#lc,
      this.#serviceRunner,
      clientGroupID,
      clientID,
      wsid,
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

  #status = async (_ctx: BaseContext, _request: Request): Promise<Response> => {
    const status = await this.#serviceRunner.status();
    return new Response(JSON.stringify(status));
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
