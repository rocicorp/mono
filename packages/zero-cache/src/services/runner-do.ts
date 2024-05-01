import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {Router} from '../router.js';
import {requireUpgradeHeader, upgradeWebsocketResponse} from './http-util.js';
import type {InvalidationWatcherRegistry} from './invalidation-watcher/registry.js';
import {CONNECT_URL_PATTERN} from './paths.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';
import type {BaseContext} from '../router.js';

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
    // upgrade to websocket
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
    _url: URL,
    _headers: Headers,
  ) => {
    // const {result, error} = getConnectRequest(url, headers);

    /**
     * initialize from data in `result`
     * e.g., client group, client id, etc.
     */

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
