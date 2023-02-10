import {LogContext, LogSink, LogLevel} from '@rocicorp/logger';
import {Router} from 'itty-router';
import {randomID} from '../util/rand.js';
import {createAuthAPIHeaders} from './auth-api-headers.js';
import {dispatch, paths} from './dispatch.js';
import {paths as authDOPaths} from './auth-do-routes.js';
import {RociRequest, requireAuthAPIKeyMatchesEnv} from './middleware.js';

export interface WorkerOptions<Env extends BaseWorkerEnv> {
  getLogSink: (env: Env) => LogSink;
  getLogLevel: (env: Env) => LogLevel;
}

export interface BaseWorkerEnv {
  authDO: DurableObjectNamespace;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  REFLECT_AUTH_API_KEY?: string;
}

// Set up routes for authDO API calls that are not handled by
// dispatch.
const router = Router();
for (const path of authDOPaths()) {
  router.all(path, requireAuthAPIKeyMatchesEnv, sendToAuthDO);
}

export function createWorker<Env extends BaseWorkerEnv>(
  options: WorkerOptions<Env>,
): ExportedHandler<Env> {
  const {getLogSink, getLogLevel} = options;
  return {
    fetch: (request: Request, env: Env, ctx: ExecutionContext) =>
      withLogContext(env, ctx, getLogSink, getLogLevel, (lc: LogContext) =>
        fetch(request, env, lc),
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
  lc.info?.(`Sending ${paths.authRevalidateConnections} request to AuthDO`);
  const resp = await sendToAuthDO(
    new Request(
      `https://unused-reflect-auth-do.dev${paths.authRevalidateConnections}`,
      {
        headers: createAuthAPIHeaders(env.REFLECT_AUTH_API_KEY),
        method: 'POST',
      },
    ),
    env,
  );
  lc.info?.(`Response: ${resp.status} ${resp.statusText}`);
}

async function fetch(
  request: Request,
  env: BaseWorkerEnv,
  lc: LogContext,
): Promise<Response> {
  // TODO: pass request id through so request can be traced across
  // worker and DOs.
  lc = lc.addContext('req', randomID());
  lc.debug?.('Handling request:', request.method, request.url);
  try {
    const resp = await withAllowAllCORS(
      request,
      async (req: Request) =>
        (await router.handle(req, env)) ?? (await handleRequest(req, lc, env)),
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

function handleRequest(
  request: Request,
  lc: LogContext,
  env: BaseWorkerEnv,
): Promise<Response> {
  const forwardToAuthDO = (_lc: LogContext, request: Request) =>
    sendToAuthDO(request, env);
  return dispatch(request, lc, env.REFLECT_AUTH_API_KEY, {
    createRoom: forwardToAuthDO,
    connect: forwardToAuthDO,
    pull: forwardToAuthDO,
    authInvalidateForUser: forwardToAuthDO,
    authInvalidateForRoom: forwardToAuthDO,
    authInvalidateAll: forwardToAuthDO,
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

function sendToAuthDO(
  request: RociRequest,
  env: BaseWorkerEnv,
): Promise<Response> {
  const {authDO} = env;
  const id = authDO.idFromName('auth');
  const stub = authDO.get(id);
  return stub.fetch(request);
}
