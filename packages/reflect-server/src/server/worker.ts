import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {version} from 'reflect-shared';
import type {MaybePromise} from 'replicache';
import {timed} from 'shared/src/timed.js';
import {Series, reportMetricsSchema} from '../types/report-metrics.js';
import {isTrueEnvValue} from '../util/env.js';
import {populateLogContextFromRequest} from '../util/log-context-common.js';
import {
  AUTH_ROUTES_AUTHED_BY_API_KEY,
  AUTH_ROUTES_CUSTOM_AUTH,
  AUTH_ROUTES_UNAUTHED,
} from './auth-do.js';
import {createDatadogMetricsSink} from './datadog-metrics-sink.js';
import {
  CANARY_GET,
  HELLO,
  LOG_LOGS_PATH,
  REPORT_METRICS_PATH,
} from './paths.js';
import type {DatadogMetricsOptions} from './reflect.js';
import {
  BaseContext,
  Handler,
  Router,
  WithLogContext,
  asJSON,
  checkAuthAPIKey,
  get,
  post,
  withBody,
} from './router.js';
import {withUnhandledRejectionHandler} from './unhandled-rejection-handler.js';

export type MetricsSink = (
  allSeries: Series[],
  lc: LogContext,
) => MaybePromise<void>;

export interface WorkerOptions {
  logSink: LogSink;
  logLevel: LogLevel;
  datadogMetricsOptions?: DatadogMetricsOptions | undefined;
}

export interface BaseWorkerEnv {
  authDO: DurableObjectNamespace;
  /**
   * If DISABLE is 'true' or '1' (ignoring case), all request will be returned
   * a 503 response, and scheduled events will not be handled.
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DISABLE?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DATADOG_LOGS_API_KEY?: string;
}

type WithEnv = {
  env: BaseWorkerEnv;
};

type WorkerContext = BaseContext &
  WithEnv & {
    datadogMetricsOptions?: DatadogMetricsOptions | undefined;
  };

type WorkerRouter = Router<WorkerContext>;

/**
 * Registers routes that are not handled by dispatch.
 */
function registerRoutes(router: WorkerRouter) {
  for (const [path, handler] of Object.entries(WORKER_ROUTES)) {
    router.register(path, handler);
  }
  for (const pattern of Object.values(AUTH_ROUTES_AUTHED_BY_API_KEY)) {
    router.register(
      pattern,
      requireAPIKeyMatchesEnv((ctx, req) => sendToAuthDO(ctx, req)),
    );
  }
  for (const pattern of Object.values(AUTH_ROUTES_CUSTOM_AUTH)) {
    router.register(pattern, sendToAuthDO);
  }
  for (const pattern of Object.values(AUTH_ROUTES_UNAUTHED)) {
    router.register(pattern, sendToAuthDO);
  }
}

// The datadog metrics http endpoint does not support CORS, so we
// have to proxy metrics reports through this worker endpoint. This
// is the most basic implementation we can imagine. This endpoint
// should...
// - buffer metrics reports and send them in batches, timeout,
//    and have retry
// - rate limit requests
// - maybe authenticate requests (but not just utilizing the auth
//    handler: we want to be able to report metrics for a logged
//    out user as well)
const reportMetrics = post<WorkerContext, Response>(
  withBody(reportMetricsSchema, async ctx => {
    const {lc, body, datadogMetricsOptions} = ctx;

    if (!datadogMetricsOptions) {
      lc.debug?.('No DatadogMetricsOptions configured, dropping metrics.');
      return new Response('noop');
    }

    if (body.series.length === 0) {
      return new Response('ok');
    }

    const metricsSink = createDatadogMetricsSink(datadogMetricsOptions);
    try {
      await metricsSink(body.series, lc);
      lc.debug?.('Successfully sent metrics to Datadog.');
    } catch (e) {
      lc.error?.(
        'Failed to send metrics to Datadog. Dropping metrics on floor.',
        e,
      );
    }

    return new Response('ok');
  }),
);

const logLogs = post<WorkerContext, Response>(
  async (ctx: WorkerContext, req: Request) => {
    const {lc, env} = ctx;

    if (env.DATADOG_LOGS_API_KEY === undefined) {
      lc.debug?.('No DATADOG_LOGS_API_KEY configured, dropping client logs.');
      return new Response('noop');
    }

    const ip = req.headers.get('CF-Connecting-IP');
    const ddUrl = new URL(req.url);
    ddUrl.protocol = 'https';
    ddUrl.host = 'http-intake.logs.datadoghq.com';
    ddUrl.pathname = 'api/v2/logs';
    ddUrl.searchParams.set('dd-api-key', env.DATADOG_LOGS_API_KEY);
    // Set ddsource to the custom string 'client', instead of 'browser'
    // because 'browser' triggers automatic DataDog pipeline processing
    // behavior that will be incorrect for these requests since
    // they are proxied and not directly from the browser (in particular the
    // automatic behavior of populating the attirbutes http.useragent from the
    // User-Agent header and network.client.ip from the Request's ip).  Instead
    // set network.client.ip and http.useragent attributes explicitly
    // to the values from the request being proxied.
    ddUrl.searchParams.set('ddsource', 'client');
    if (ip) {
      ddUrl.searchParams.set('network.client.ip', ip);
    }
    const userAgent = req.headers.get('User-Agent');
    if (userAgent) {
      ddUrl.searchParams.set('http.useragent', userAgent);
    }

    const ddRequest = new Request(ddUrl.toString(), {
      method: 'POST',
      headers: new Headers({
        'content-type':
          req.headers.get('content-type') ?? 'text/plain;charset=UTF-8',
      }),
      body: req.body,
    });

    lc.info?.('ddRequest', ddRequest.url, [...ddRequest.headers.entries()]);
    try {
      const ddResponse = await fetch(ddRequest);
      if (ddResponse.ok) {
        lc.debug?.('Successfully sent client logs to Datadog.');
        return new Response('ok');
      }
      lc.error?.(
        'Failed to send client logs to DataDog, error response',
        ddResponse.status,
        ddResponse.statusText,
        await ddResponse.text,
      );
      return new Response('Error response.', {status: ddResponse.status});
    } catch (e) {
      lc.error?.('Failed to send client logs to DataDog, error', e);
      return new Response('Error.', {status: 500});
    }
  },
);

const hello = get<WorkerContext, Response>(
  asJSON(() => ({
    reflectServerVersion: version,
  })),
);

const canaryGet = get<WorkerContext, Response>(
  (ctx: WorkerContext, req: Request) => {
    const url = new URL(req.url);
    const checkID = url.searchParams.get('id') ?? 'missing';
    const lc = ctx.lc
      .withContext('connectCheckID', checkID)
      .withContext('checkName', 'cfGet');
    lc.info?.('Handling get connection check.');
    return new Response('hello');
  },
);

function requireAPIKeyMatchesEnv(next: Handler<WorkerContext, Response>) {
  return (ctx: WorkerContext, req: Request) => {
    const resp = checkAuthAPIKey(ctx.env.REFLECT_AUTH_API_KEY, req);
    if (resp) {
      return resp;
    }
    return next(ctx, req);
  };
}

export function createWorker<Env extends BaseWorkerEnv>(
  getOptions: (env: Env) => WorkerOptions,
): ExportedHandler<Env> {
  const router: WorkerRouter = new Router();
  registerRoutes(router);
  return {
    fetch: (request: Request, env: Env, ctx: ExecutionContext) => {
      const {logSink, logLevel, datadogMetricsOptions} = getOptions(env);
      return withLogContext(
        ctx,
        logSink,
        logLevel,
        request,
        withUnhandledRejectionHandler(lc =>
          workerFetch(request, env, router, lc, datadogMetricsOptions),
        ),
      );
    },
  };
}

async function workerFetch(
  request: Request,
  env: BaseWorkerEnv,
  router: WorkerRouter,
  lc: LogContext,
  datadogMetricsOptions: DatadogMetricsOptions | undefined,
): Promise<Response> {
  lc.debug?.('Handling request:', request.method, request.url);
  try {
    const resp = await withAllowAllCORS(request, async (request: Request) => {
      if (isTrueEnvValue(env.DISABLE)) {
        return new Response('Disabled', {status: 503});
      }
      return (
        (await router.dispatch(request, {
          lc,
          env,
          datadogMetricsOptions,
        })) ??
        new Response(null, {
          status: 404,
        })
      );
    });
    lc.debug?.(`Returning response: ${resp.status} ${resp.statusText}`);
    return resp;
  } catch (e) {
    lc.error?.('Unhandled exception in fetch', e);
    return new Response(e instanceof Error ? e.message : 'Unexpected error.', {
      status: 500,
    });
  }
}

async function withAllowAllCORS(
  request: Request,
  handle: (request: Request) => Promise<Response>,
): Promise<Response> {
  if (request.method === 'OPTIONS') {
    return handleOptions(request);
  }
  // Try newfangled routing first.
  const resp = await handle(request);
  // Clone so CORS headers can be set.  Clone using constructor copy
  // rather than clone method because CloudFlare's Response
  // class will throw
  // "TypeError: Cannot clone a response to a WebSocket handshake."
  // if the response has a defined webSocket property.
  const respWithAllowAllCORS = new Response(resp.body, resp);
  respWithAllowAllCORS.headers.set('Access-Control-Allow-Origin', '*');
  return respWithAllowAllCORS;
}

function handleOptions(request: Request): Response {
  const {headers} = request;
  // Check if necessary headers are present for this to be a valid pre-flight
  // request
  if (
    headers.has('Origin') &&
    headers.has('Access-Control-Request-Method') &&
    headers.has('Access-Control-Request-Headers')
  ) {
    // Handle CORS pre-flight request.
    const respHeaders = {
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Access-Control-Allow-Origin': '*',
      // TODO determine methods from route definitions, for now
      // just return support for all methods on all paths.
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Access-Control-Allow-Methods': 'GET,HEAD,POST,OPTIONS',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Access-Control-Max-Age': '86400', // 24 hours
      // eslint-disable-next-line @typescript-eslint/naming-convention
      'Access-Control-Allow-Headers':
        headers.get('Access-Control-Request-Headers') ?? '',
    };

    return new Response(null, {
      headers: respHeaders,
    });
  }
  // Handle standard OPTIONS request.
  // TODO implement based on route definitions, for now just return
  // support for all methods on all paths.
  return new Response(null, {
    headers: {
      ['Allow']: 'GET, HEAD, POST, OPTIONS',
    },
  });
}

async function withLogContext<R>(
  ctx: ExecutionContext,
  logSink: LogSink,
  logLevel: LogLevel,
  req: Request | undefined,
  fn: (lc: LogContext) => Promise<R>,
): Promise<R> {
  let lc = new LogContext(logLevel, undefined, logSink).withContext(
    'component',
    'Worker',
  );
  if (req !== undefined) {
    lc = populateLogContextFromRequest(lc, req);
  }
  try {
    return await fn(lc);
  } finally {
    if (logSink.flush) {
      ctx.waitUntil(logSink.flush());
    }
  }
}

// eslint-disable-next-line require-await
async function sendToAuthDO(
  ctx: WithLogContext & WithEnv,
  request: Request,
): Promise<Response> {
  const {lc, env} = ctx;
  const {authDO} = env;

  const id = authDO.idFromName('auth');
  const stub = authDO.get(id);

  lc.debug?.(`Sending request ${request.url} to authDO`);
  const responseFromDO = await timed(lc.debug, 'authDO fetch', async () => {
    try {
      return await stub.fetch(request);
    } catch (e) {
      lc.error?.(`Exception fetching ${request.url} from authDO`, e);
      throw e;
    }
  });
  lc.debug?.(
    'received authDO response',
    responseFromDO.status,
    responseFromDO.statusText,
  );
  return responseFromDO;
}

export const WORKER_ROUTES = {
  [REPORT_METRICS_PATH]: reportMetrics,
  [LOG_LOGS_PATH]: logLogs,
  [HELLO]: hello,
  [CANARY_GET]: canaryGet,
} as const;
