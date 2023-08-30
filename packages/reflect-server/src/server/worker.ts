import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {version} from 'reflect-shared';
import type {MaybePromise} from 'replicache';
import {Series, reportMetricsSchema} from '../types/report-metrics.js';
import {randomID} from '../util/rand.js';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {
  AUTH_ROUTES,
  AUTH_ROUTES_AUTHED_BY_API_KEY,
  AUTH_ROUTES_AUTHED_BY_AUTH_HANDLER,
  AUTH_ROUTES_UNAUTHED,
} from './auth-do.js';
import {createDatadogMetricsSink} from './datadog-metrics-sink.js';
import {CANARY_GET, HELLO, REPORT_METRICS_PATH} from './paths.js';
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
import {timed} from 'shared/src/timed.js';
import {populateLogContextFromRequest} from '../util/log-context-common.js';

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
   * If DISABLE is true, all request will be returned a 503 response, and
   * scheduled events will not be handled.
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  DISABLE?: boolean;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
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
  for (const pattern of Object.values(AUTH_ROUTES_AUTHED_BY_AUTH_HANDLER)) {
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
      return new Response('ok');
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
          fetch(request, env, router, lc, datadogMetricsOptions),
        ),
      );
    },
    scheduled: (
      _controller: ScheduledController,
      env: Env,
      ctx: ExecutionContext,
    ) => {
      const {logSink, logLevel} = getOptions(env);
      return withLogContext(
        ctx,
        logSink,
        logLevel,
        undefined,
        withUnhandledRejectionHandler(lc => scheduled(env, lc)),
      );
    },
  };
}

async function scheduled(env: BaseWorkerEnv, lc: LogContext): Promise<void> {
  lc = lc.withContext('scheduled', randomID());
  lc.info?.('Handling scheduled event');
  if (env.DISABLE) {
    lc.debug?.('Returning early because env.DISABLE is true.');
    return;
  }
  if (!env.REFLECT_AUTH_API_KEY) {
    lc.debug?.(
      'Returning early because REFLECT_AUTH_API_KEY is not defined in env.',
    );
    return;
  }
  lc.info?.(
    `Sending ${AUTH_ROUTES.authRevalidateConnections} request to AuthDO`,
  );
  const req = new Request(
    `https://unused-reflect-auth-do.dev${AUTH_ROUTES.authRevalidateConnections}`,
    {
      method: 'POST',
      headers: createAuthAPIHeaders(env.REFLECT_AUTH_API_KEY),
    },
  );
  const resp = await sendToAuthDO({lc, env}, req);
  lc.info?.(`Response: ${resp.status} ${resp.statusText}`);
}

async function fetch(
  request: Request,
  env: BaseWorkerEnv,
  router: WorkerRouter,
  lc: LogContext,
  datadogMetricsOptions: DatadogMetricsOptions | undefined,
): Promise<Response> {
  lc.debug?.('Handling request:', request.method, request.url);
  try {
    const resp = await withAllowAllCORS(request, async (request: Request) => {
      if (env.DISABLE) {
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
  [HELLO]: hello,
  [CANARY_GET]: canaryGet,
} as const;
