import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {CONNECT_URL_PATTERN} from './paths.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';
import {Router, BaseContext} from 'reflect-server/router';
import {
  requireUpgradeHeader,
  upgradeWebsocketResponse,
} from 'reflect-server/http-util';
import {getConnectRequest} from 'reflect-server/connect';
import type {ErrorKind} from 'zero-protocol/src/error.js';
import {closeWithError} from 'reflect-server/socket';

export class ServiceRunnerDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #router = new Router();

  constructor(
    registry: InvalidationWatcherRegistry,
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    this.#serviceRunner = new ServiceRunner(
      registry,
      logSink,
      logLevel,
      state,
      env,
    );
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

    await this.#handleConnection(serverWS, url, request.headers);

    return upgradeWebsocketResponse(clientWS, request.headers);
  };

  // eslint-disable-next-line require-await
  #handleConnection = async (
    serverWS: WebSocket,
    url: URL,
    headers: Headers,
  ) => {
    const closeWithErrorLocal = (ek: ErrorKind, msg: string) => {
      closeWithError(this.#lc, serverWS, ek, msg);
    };

    const {result, error} = getConnectRequest(url, headers);
    if (error !== null) {
      closeWithErrorLocal('InvalidConnectionRequest', error);
      return;
    }
    const {clientGroupID} = result;

    // ensure that the viewSyncer is up and running
    this.#serviceRunner.getViewSyncer(clientGroupID);

    serverWS.addEventListener('message', event => {
      dispatchMessage(this.#serviceRunner, event.data.toString(), serverWS);
    });
  };

  // eslint-disable-next-line require-await
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

function dispatchMessage(
  _serviceRunner: ServiceRunner,
  _data: string,
  _ws: WebSocket,
) {
  // decodes the message and dispatches to the appropriate service
}
