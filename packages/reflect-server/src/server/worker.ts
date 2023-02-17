import {LogContext, LogSink, LogLevel} from '@rocicorp/logger';
import {
  BaseContext,
  checkAuthAPIKey,
  Handler,
  post,
  Router,
  withBody,
  WithLogContext,
} from './router.js';
import {randomID} from '../util/rand.js';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {
  AUTH_ROUTES,
  AUTH_ROUTES_AUTHED_BY_API_KEY,
  AUTH_ROUTES_AUTHED_BY_AUTH_HANDLER,
} from './auth-do.js';
import {reportMetricsSchema} from '../types/report-metrics.js';
import {
  DD_AUTH_HEADER_NAME,
  DD_DISTRIBUTION_METRIC_URL,
  report,
} from '@rocicorp/datadog-util';
import {assert} from '../util/asserts.js';
import {REPORT_METRICS_PATH} from './paths.js';

export interface WorkerOptions<Env extends BaseWorkerEnv> {
  getLogSink: (env: Env) => LogSink;
  getLogLevel: (env: Env) => LogLevel;
}

export interface BaseWorkerEnv {
  authDO: DurableObjectNamespace;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
  /**
   * If not bound the reportMetrics API will return a 5xx.
   */
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_DATADOG_API_KEY?: string;
}

type WithEnv = {
  env: BaseWorkerEnv;
};

type WorkerContext = BaseContext & WithEnv;

type WorkerRouter = Router<WithLogContext & WithEnv>;

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
const reportMetrics = post(
  requireDataDogApiEnv(
    withBody(reportMetricsSchema, async ctx => {
      const {body, env} = ctx;

      if (body.series.length === 0) {
        return new Response('ok');
      }

      assert(env.REFLECT_DATADOG_API_KEY);
      const resp = await report(
        DD_DISTRIBUTION_METRIC_URL,
        {
          [DD_AUTH_HEADER_NAME]: env.REFLECT_DATADOG_API_KEY,
        },
        body.series,
      );
      if (resp.ok) {
        ctx.lc.debug?.(
          `Successfully proxied metrics report to Datadog: ${resp.status} ${resp.statusText}.`,
        );
      } else {
        ctx.lc.info?.(
          `Failed to report metrics to Datadog: ${resp.status} ${resp.statusText}.`,
          'Dropping metrics on the floor.',
        );
      }
      return new Response('ok');
    }),
  ),
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

function requireDataDogApiEnv(next: Handler<WorkerContext, Response>) {
  return (ctx: WorkerContext, req: Request) => {
    if (ctx.env.REFLECT_DATADOG_API_KEY === undefined) {
      ctx.lc.debug?.('reportMetrics: proxy metrics not enabled');
      return new Response('metrics not enabled', {
        status: 503,
      });
    }
    return next(ctx, req);
  };
}

export function createWorker<Env extends BaseWorkerEnv>(
  options: WorkerOptions<Env>,
): ExportedHandler<Env> {
  const {getLogSink, getLogLevel} = options;
  const router: WorkerRouter = new Router();
  registerRoutes(router);
  return {
    fetch: (request: Request, env: Env, ctx: ExecutionContext) =>
      withLogContext(env, ctx, getLogSink, getLogLevel, (lc: LogContext) =>
        fetch(request, env, router, lc),
      ),
    scheduled: (
      _controller: ScheduledController,
      env: Env,
      ctx: ExecutionContext,
    ) =>
      withLogContext(env, ctx, getLogSink, getLogLevel, (lc: LogContext) =>
        scheduled(env, lc),
      ),
  };
}

async function scheduled(env: BaseWorkerEnv, lc: LogContext): Promise<void> {
  lc = lc.addContext('scheduled', randomID());
  lc.info?.('Handling scheduled event');
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
): Promise<Response> {
  // TODO: pass request id through so request can be traced across
  // worker and DOs.
  lc = lc.addContext('req', randomID());
  lc.debug?.('Handling request:', request.method, request.url);
  try {
    const resp = await withAllowAllCORS(
      request,
      async (request: Request) =>
        (await router.dispatch(request, {lc, env})) ??
        new Response(null, {
          status: 404,
        }),
    );
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

async function withLogContext<Env extends BaseWorkerEnv, R>(
  env: Env,
  ctx: ExecutionContext,
  getLogSink: (env: Env) => LogSink,
  getLogLevel: (env: Env) => LogLevel,
  fn: (lc: LogContext) => Promise<R>,
): Promise<R> {
  const logSink = getLogSink(env);
  const lc = new LogContext(getLogLevel(env), logSink).addContext('Worker');
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

  lc.debug?.(`Sending request ${request.url} to authDO`);

  const id = authDO.idFromName('auth');
  const stub = authDO.get(id);
  return stub.fetch(request);
}

export const WORKER_ROUTES = {
  [REPORT_METRICS_PATH]: reportMetrics,
} as const;
