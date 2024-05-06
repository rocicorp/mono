import type {
  ExecutionContext,
  ExportedHandler,
} from '@cloudflare/workers-types';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {timed} from 'shared/src/timed.js';
import type {ServiceRunnerEnv} from './service-runner.js';
import {assert} from 'shared/src/asserts.js';

export interface WorkerOptions {
  logSink: LogSink;
  logLevel: LogLevel;
}

async function withLogContext<R>(
  ctx: ExecutionContext,
  logSink: LogSink,
  logLevel: LogLevel,
  req: Request | undefined,
  fn: (lc: LogContext) => R | Promise<R>,
): Promise<R> {
  let lc = new LogContext(logLevel, undefined, logSink).withContext(
    'component',
    'Worker',
  );
  if (req !== undefined) {
    lc = lc.withContext('url', req.url);
  }
  try {
    return await fn(lc);
  } finally {
    if (logSink.flush) {
      ctx.waitUntil(logSink.flush());
    }
  }
}

export function createWorker<Env extends ServiceRunnerEnv>(
  getOptions: (env: Env) => WorkerOptions,
): ExportedHandler<Env> {
  return {
    fetch: (request: Request, env: Env, ctx: ExecutionContext) => {
      const {logSink, logLevel} = getOptions(env);
      return withLogContext(ctx, logSink, logLevel, request, lc =>
        workerFetch(request, env, lc),
      );
    },
  };
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

async function sendToRunnerDO(
  ctx: {
    lc: LogContext;
    env: ServiceRunnerEnv;
  },
  request: Request,
): Promise<Response> {
  const {lc, env} = ctx;
  const {runnerDO} = env;

  const id = runnerDO.idFromName('runnerDO');
  const locationHint = ctx.env.DO_LOCATION_HINT;
  assertDOLocation(locationHint);
  const stub = runnerDO.get(id, {locationHint});

  lc.debug?.(`Sending request ${request.url} to runnerDO`);
  const responseFromDO = await timed(lc.debug, 'runnerDO fetch', async () => {
    try {
      return await stub.fetch(request);
    } catch (e) {
      lc.error?.(`Exception fetching ${request.url} from runnerDO`, e);
      throw e;
    }
  });
  lc.debug?.(
    'received runnerDO response',
    responseFromDO.status,
    responseFromDO.statusText,
  );
  return responseFromDO;
}

async function workerFetch(
  request: Request,
  env: ServiceRunnerEnv,
  lc: LogContext,
): Promise<Response> {
  lc.debug?.('Handling request:', request.method, request.url);
  try {
    const resp = await withAllowAllCORS(
      request,
      async (request: Request) =>
        (await sendToRunnerDO(
          {
            lc,
            env,
          },
          request,
        )) ??
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

const DO_LOCATION_HINTS: ReadonlySet<string> = new Set([
  'wnam',
  'enam',
  'sam',
  'weur',
  'eeur',
  'apac',
  'oc',
  'afr',
  'me',
]);

function assertDOLocation(
  val: string,
): asserts val is DurableObjectLocationHint {
  assert(
    DO_LOCATION_HINTS.has(val),
    `${val} is not a valid location hint value.  Supported values: ${[
      ...DO_LOCATION_HINTS.values(),
    ].join(',')}.`,
  );
}
