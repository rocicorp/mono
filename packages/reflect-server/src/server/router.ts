import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {AUTH_API_KEY_HEADER_NAME} from './auth-api-headers.js';
import {createUnauthorizedResponse} from './create-unauthorized-response.js';
import {isWebsocketUpgrade} from './http-util.js';

/**
 * Handles a request dispatched by router. Handlers are meant to be nested
 * in a chain, implementing the concept of "middleware" that validate and/or
 * compute additional parameters used by downstream handlers.
 *
 * Request is passed through the handler chain as-is, unmolested. Each
 * handler however can create a new, different `context` and pass this to
 * the next handler. This is how things like body validation are implemented.
 */
export type Handler<Context, Resp> = (
  context: Context,
  request: Request,
) => MaybePromise<Resp>;

export type WithLogContext = {
  lc: LogContext;
};

export type WithParsedURL = {
  parsedURL: URLPatternURLPatternResult;
};

export type BaseContext = WithLogContext & WithParsedURL;

type Route<Context> = {
  pattern: URLPattern;
  handler: Handler<Context, Response>;
};

/**
 * Routes requests to a handler for processing and returns the response.
 *
 * Requests and responses are abstract, they don't need to be http `Request`.
 *
 * Handlers are typically chained together outside of router itself to create
 * "middleware", but that's convention. See below in this file for examples of
 * such middleware.
 */
export class Router<InitialContext extends BaseContext = BaseContext> {
  #routes: Route<InitialContext>[] = [];

  register(path: string, handler: Handler<InitialContext, Response>) {
    // It is OK add another route with the same path. However, the first one
    // will always be used.
    this.#routes.push({
      pattern: new URLPattern({pathname: path}),
      handler,
    });
  }

  dispatch(
    request: Request,
    context: Omit<InitialContext, 'parsedURL'>,
  ): MaybePromise<Response> {
    for (const route of this.#routes) {
      const {pattern} = route;
      const result = pattern.exec(request.url);
      if (result) {
        const {handler} = route;
        return handler(
          {...context, parsedURL: result} as InitialContext,
          request,
        );
      }
    }

    const {lc} = context;
    lc.debug?.(`no matching route for ${request.url}`);
    return new Response('not found', {status: 404});
  }
}

function requireMethod<Context extends BaseContext, Resp extends Response>(
  method: string,
  next: Handler<Context, Resp>,
) {
  return (context: Context, request: Request) => {
    if (request.method !== method) {
      return new Response('unsupported method', {status: 405});
    }
    return next(context, request);
  };
}

export function get<Context extends BaseContext, Resp extends Response>(
  next: Handler<Context, Resp>,
) {
  return requireMethod('GET', next);
}

export function post<Context extends BaseContext, Resp extends Response>(
  next: Handler<Context, Resp>,
) {
  return requireMethod('POST', next);
}

export function requireAuthAPIKey<Context extends BaseContext, Resp>(
  required: (context: Context) => string,
  next: Handler<Context, Resp>,
) {
  return (context: Context, req: Request) => {
    const resp = checkAuthAPIKey(required(context), req);
    if (resp) {
      return resp;
    }
    return next(context, req);
  };
}

export function checkAuthAPIKey(
  required: string | undefined,
  request: Request,
) {
  if (!required) {
    throw new Error('Internal error: expected auth api key cannot be empty');
  }

  let authHeader: string | null | undefined;
  if (isWebsocketUpgrade(request)) {
    // For websocket requests, the AUTH_API_KEY is in the Sec-WebSocket-Protocol header.
    const encodedAuth = request.headers.get('Sec-WebSocket-Protocol');
    if (encodedAuth) {
      authHeader = decodeURIComponent(encodedAuth);
    }
  } else {
    authHeader = request.headers.get(AUTH_API_KEY_HEADER_NAME);
  }

  if (authHeader !== required) {
    return createUnauthorizedResponse();
  }
  return undefined;
}

export type WithRoomID = {roomID: string};

export function withRoomID<Context extends BaseContext, Resp>(
  next: Handler<Context & WithRoomID, Resp>,
) {
  return (ctx: Context, req: Request) => {
    const {roomID} = ctx.parsedURL.pathname.groups;
    if (roomID === undefined) {
      throw new Error('Internal error: roomID not found by withRoomID');
    }
    const decoded = decodeURIComponent(roomID);
    return next({...ctx, roomID: decoded}, req);
  };
}

export function requireRoomIDSearchParam<Context extends BaseContext, Resp>(
  next: Handler<Context & WithRoomID, Resp>,
) {
  return (ctx: Context, req: Request) => {
    const url = new URL(req.url);
    const roomID = url.searchParams.get('roomID');
    if (!roomID) {
      return new Response('roomID search param required', {status: 400});
    }
    return next({...ctx, roomID}, req);
  };
}

export type WithVersion = {version: number};
export function withVersion<Context extends BaseContext, Resp>(
  next: Handler<Context & WithVersion, Resp>,
) {
  return (ctx: Context, req: Request) => {
    const {version: versionString} = ctx.parsedURL.pathname.groups;
    if (versionString === undefined) {
      throw new Error('version not found by withVersion url: ' + req.url);
    }
    if (!/^v\d+$/.test(versionString)) {
      throw new Error(`invalid version found by withVersion, ${versionString}`);
    }
    const version = Number(versionString.slice(1));
    return next({...ctx, version}, req);
  };
}

export function asJSON<Context extends BaseContext>(
  next: Handler<Context, ReadonlyJSONValue>,
) {
  return async (ctx: Context, req: Request) =>
    new Response(JSON.stringify(await next(ctx, req)));
}

export function withBody<T, Context extends BaseContext, Resp>(
  schema: valita.Type<T>,
  next: Handler<Context & {body: T}, Resp>,
) {
  return async (ctx: Context, req: Request) => {
    const {value, errorResponse} = await validateBody(req, schema);
    if (errorResponse) {
      return errorResponse;
    }
    return next({...ctx, body: value}, req);
  };
}

type ValidateResult<T> =
  | {value: T; errorResponse: undefined}
  | {value: undefined; errorResponse: Response};

async function validateBody<T>(
  request: Request,
  schema: valita.Type<T>,
): Promise<ValidateResult<T>> {
  let json;
  try {
    // Note: we don't clone the request here, because if we did clone and the
    // original request body is not consumed CF complains in the console, "Your
    // worker called response.clone(), but did not read the body of both
    // clones. <snip>". Routes that use validateBody, should use
    // the ValidateResult and not try to read the body, as reading the body
    // again will result in an error "TypeError: body used already for <snip>".
    json = await request.json();
  } catch (e) {
    return {
      errorResponse: new Response('Body must be valid json.', {status: 400}),
      value: undefined,
    };
  }
  const validateResult = valita.test(json, schema);
  if (!validateResult.ok) {
    return {
      errorResponse: new Response(
        'Body schema error. ' + validateResult.error,
        {
          status: 400,
        },
      ),
      value: undefined,
    };
  }
  return {
    value: validateResult.value,
    errorResponse: undefined,
  };
}
