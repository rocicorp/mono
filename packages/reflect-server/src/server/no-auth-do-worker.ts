import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {encodeHeaderValue} from '../util/headers.js';
import {randomID} from '../util/rand.js';
import {AuthHandler, UserData, USER_DATA_HEADER_NAME} from './auth.js';
import {createUnauthorizedResponse} from './create-unauthorized-response.js';

export interface NoAuthDOWorkerOptions {
  logSink: LogSink;
  logLevel: LogLevel;
  authHandler: AuthHandler;
}

export interface BaseNoAuthDOWorkerEnv {
  roomDO: DurableObjectNamespace;
}

export function createNoAuthDOWorker<Env extends BaseNoAuthDOWorkerEnv>(
  getOptions: (env: Env) => NoAuthDOWorkerOptions,
): ExportedHandler<Env> {
  return {
    fetch: (request: Request, env: Env, ctx: ExecutionContext) => {
      const {logSink, logLevel, authHandler} = getOptions(env);
      return withLogContext(ctx, logSink, logLevel, (lc: LogContext) =>
        fetch(request, lc, env.roomDO, authHandler),
      );
    },
    scheduled: (
      _controller: ScheduledController,
      env: Env,
      ctx: ExecutionContext,
    ) => {
      const {logSink, logLevel} = getOptions(env);
      return withLogContext(ctx, logSink, logLevel, (lc: LogContext) =>
        scheduled(env, lc),
      );
    },
  };
}

function scheduled(_env: BaseNoAuthDOWorkerEnv, lc: LogContext): void {
  lc = lc.addContext('scheduled', randomID());
  lc.info?.('Ignoring scheduled event because not configured with AuthDO');
}

async function fetch(
  request: Request,
  lc: LogContext,
  roomDO: DurableObjectNamespace,
  authHandler: AuthHandler,
) {
  // TODO: pass request id through so request can be traced across
  // worker and DOs.
  lc = lc.addContext('req', randomID());
  lc.debug?.('Handling request:', request.url);
  try {
    const resp = await handleRequest(request, lc, roomDO, authHandler);
    lc.debug?.(`Returning response: ${resp.status} ${resp.statusText}`);
    return resp;
  } catch (e) {
    lc.error?.('Unhandled exception in fetch', e);
    return new Response(e instanceof Error ? e.message : 'Unexpected error.', {
      status: 500,
    });
  }
}

async function handleRequest(
  request: Request,
  lc: LogContext,
  roomDO: DurableObjectNamespace,
  authHandler: AuthHandler,
): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname !== '/connect') {
    return new Response('unknown route', {
      status: 400,
    });
  }

  const roomID = url.searchParams.get('roomID');
  if (roomID === null || roomID === '') {
    return new Response('roomID parameter required', {
      status: 400,
    });
  }

  const clientID = url.searchParams.get('clientID');
  if (!clientID) {
    return new Response('clientID parameter required', {
      status: 400,
    });
  }

  const encodedAuth = request.headers.get('Sec-WebSocket-Protocol');
  if (!encodedAuth) {
    lc.info?.('auth not found in Sec-WebSocket-Protocol header.');
    return createUnauthorizedResponse('auth required');
  }

  let decodedAuth: string | undefined;
  try {
    decodedAuth = decodeURIComponent(encodedAuth);
  } catch (e) {
    lc.info?.('error decoding auth found in Sec-WebSocket-Protocol header.');
    return createUnauthorizedResponse('invalid auth');
  }
  const auth = decodedAuth;

  let userData: UserData | undefined;
  try {
    userData = await authHandler(auth, roomID);
  } catch (e) {
    return createUnauthorizedResponse();
  }
  if (!userData || !userData.userID) {
    if (!userData) {
      lc.info?.('userData returned by authHandler is falsey.');
    } else if (!userData.userID) {
      lc.info?.('userData returned by authHandler has no userID.');
    }
    return createUnauthorizedResponse();
  }

  // Forward the request to the Room Durable Object for roomID...
  const id = roomDO.idFromName(roomID);
  const stub = roomDO.get(id);
  const requestToDO = new Request(request);
  requestToDO.headers.set(
    USER_DATA_HEADER_NAME,
    encodeHeaderValue(JSON.stringify(userData)),
  );
  const responseFromDO = await stub.fetch(requestToDO);
  const responseHeaders = new Headers(responseFromDO.headers);
  // While Sec-WebSocket-Protocol is just being used as a mechanism for
  // sending `auth` since custom headers are not supported by the browser
  // WebSocket API, the Sec-WebSocket-Protocol semantics must be followed.
  // Send a Sec-WebSocket-Protocol response header with a value
  // matching the Sec-WebSocket-Protocol request header, to indicate
  // support for the protocol, otherwise the client will close the connection.
  responseHeaders.set('Sec-WebSocket-Protocol', encodedAuth);

  const response = new Response(responseFromDO.body, {
    status: responseFromDO.status,
    statusText: responseFromDO.statusText,
    webSocket: responseFromDO.webSocket,
    headers: responseHeaders,
  });
  return response;
}

async function withLogContext<R>(
  ctx: ExecutionContext,
  logSink: LogSink,
  logLevel: LogLevel,
  fn: (lc: LogContext) => Promise<R> | R,
): Promise<R> {
  const lc = new LogContext(logLevel, logSink).addContext('Worker');
  try {
    return await fn(lc);
  } finally {
    if (logSink.flush) {
      ctx.waitUntil(logSink.flush());
    }
  }
}
