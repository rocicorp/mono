import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise, ReadonlyJSONValue} from 'replicache';
import {Struct, validate} from 'superstruct';
import {createUnauthorizedResponse} from './create-unauthorized-response.js';

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
export class Router<InitialContext extends WithLogContext = WithLogContext> {
  private _routes: Route<InitialContext & WithParsedURL>[] = [];

  register(
    path: string,
    handler: Handler<InitialContext & WithParsedURL, Response>,
  ) {
    // It is OK add another route with the same path. However, the first one
    // will always be used.
    this._routes.push({
      pattern: new URLPattern({pathname: path}),
      handler,
    });
  }

  dispatch(request: Request, context: InitialContext): MaybePromise<Response> {
    const {lc} = context;
    // TODO(arv): This can be simpler using a for-of loop. No need to iterate
    // over all of them to find the first match.
    const matches = this._routes
      .map(route => {
        const {pattern} = route;
        const result = pattern.exec(request.url);
        return {route, result};
      })
      .filter(({result}) => result);

    if (matches.length === 0) {
      lc.debug?.(`no matching route for ${request.url}`);
      return new Response('not found', {status: 404});
    }

    const [match] = matches;
    const {route, result} = match;
    const {handler} = route;

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return handler({...context, parsedURL: result!}, request);
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

export function checkAuthAPIKey(required: string | undefined, req: Request) {
  if (!required) {
    throw new Error('Internal error: expected auth api key cannot be empty');
  }
  const authHeader = req.headers.get('x-reflect-auth-api-key');
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

export function asJSON<Context extends BaseContext>(
  next: Handler<Context, ReadonlyJSONValue>,
) {
  return async (ctx: Context, req: Request) =>
    new Response(JSON.stringify(await next(ctx, req)));
}

export function withBody<T, Context extends BaseContext, Resp>(
  struct: Struct<T>,
  next: Handler<Context & {body: T}, Resp>,
) {
  return async (ctx: Context, req: Request) => {
    const {value, errorResponse} = await validateBody(req, struct);
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
  struct: Struct<T>,
): Promise<ValidateResult<T>> {
  let json;
  try {
    // Note: if the original request body is not consumed after this clone
    // then CF complains in the console, "Your worker called response.clone(),
    // but did not read the body of both clones. <snip>". To eliminate that
    // log line we could consume the original request body here and then
    // both create and pass the validated request as well as the body
    // in case something downstream wants it.
    json = await request.clone().json();
  } catch (e) {
    return {
      errorResponse: new Response('Body must be valid json.', {status: 400}),
      value: undefined,
    };
  }
  const validateResult = validate(json, struct);
  if (validateResult[0]) {
    return {
      errorResponse: new Response(
        'Body schema error. ' + validateResult[0].message,
        {
          status: 400,
        },
      ),
      value: undefined,
    };
  }
  return {
    value: validateResult[1],
    errorResponse: undefined,
  };
}
