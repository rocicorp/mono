import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {BaseContext, Router, bodyOnly, post} from 'cf-shared/src/router.js';
import {streamOut} from '../types/streams.js';
import {REGISTER_FILTERS_PATTERN, VERSION_CHANGES_PATTERN} from './paths.js';
import {registerInvalidationFiltersRequest} from './replicator/replicator.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';

export class ReplicatorDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #router = new Router();

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableObjectState,
    env: ServiceRunnerEnv,
  ) {
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ReplicatorDO',
    );
    this.#lc = lc;
    this.#serviceRunner = new ServiceRunner(lc, state, env, true);
    void (async () => {
      const traceResponse = await fetch('https://cloudflare.com/cdn-cgi/trace');
      const traceText = await traceResponse.text();
      lc.info?.('Location information:\n', traceText);
    })();

    this.#initRoutes();
  }

  #initRoutes() {
    this.#router.register(REGISTER_FILTERS_PATTERN, this.#registerFilters);
    this.#router.register(VERSION_CHANGES_PATTERN, this.#versionChanges);
  }

  #registerFilters = post()
    .with(bodyOnly(registerInvalidationFiltersRequest))
    .handleJSON(async ctx => {
      const replicator = await this.#serviceRunner.getReplicator();
      return replicator.registerInvalidationFilters(ctx.body);
    });

  #versionChanges = async (
    _: BaseContext,
    request: Request,
  ): Promise<Response> => {
    if (request.headers.get('Upgrade') !== 'websocket') {
      this.#lc.info?.('Missing Upgrade header for', request.url);
      return new Response('expected WebSocket Upgrade header', {status: 400});
    }

    const {0: clientWS, 1: serverWS} = new WebSocketPair();
    serverWS.accept();

    const replicator = await this.#serviceRunner.getReplicator();
    const subscription = await replicator.versionChanges();

    void streamOut(
      this.#lc.withContext('stream', 'VersionChange'),
      subscription,
      serverWS,
    );

    // Sec-WebSocket-Protocol is used as a mechanism for sending `auth`
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
