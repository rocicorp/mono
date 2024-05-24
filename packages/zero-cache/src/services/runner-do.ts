import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {BaseContext, Router} from 'cf-shared/src/router.js';
import {Connection, handleConnection} from './connection.js';
import {CONNECT_URL_PATTERN, STATUS_URL_PATTERN} from './paths.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';

export class ServiceRunnerDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #router = new Router();
  readonly #clientConnections = new Map<string, Connection>();

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );
    this.#serviceRunner = new ServiceRunner(lc, state, env, false);
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

  #connect = (ctx: BaseContext, request: Request): Response =>
    handleConnection(
      ctx.lc,
      this.#serviceRunner,
      this.#clientConnections,
      request,
    );

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
