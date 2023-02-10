import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise, ReadonlyJSONValue} from 'replicache';

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
  request: Request,
  context: Context,
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
    this._routes.push({
      pattern: new URLPattern({pathname: path}),
      handler,
    });
  }

  dispatch(
    request: Request,
    context: InitialContext,
  ): MaybePromise<Response | undefined> {
    const {lc} = context;
    const matches = this._routes
      .map(route => {
        const {pattern} = route;
        const result = pattern.exec(request.url);
        return {route, result};
      })
      .filter(({result}) => result);

    if (matches.length === 0) {
      lc.debug?.(`no matching route for ${request.url}`);
      return undefined;
    }

    const [match] = matches;
    const {route, result} = match;
    const {handler} = route;

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return handler(request, {...context, parsedURL: result!});
  }
}

function requireMethod<Context extends BaseContext, Resp extends Response>(
  method: string,
  next: Handler<Context, Resp>,
) {
  return (request: Request, context: Context) => {
    if (request.method !== method) {
      return new Response('unsupported method', {status: 405});
    }
    return next(request, context);
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
  required: () => string,
  next: Handler<Context, Resp>,
) {
  return (req: Request, context: Context) => {
    const resp = checkAuthAPIKey(required(), req);
    if (resp) {
      return resp;
    }
    return next(req, context);
  };
}

export function checkAuthAPIKey(required: string | undefined, req: Request) {
  if (!required) {
    throw new Error('Internal error: expected auth api key cannot be empty');
  }
  const authHeader = req.headers.get('x-reflect-auth-api-key');
  if (authHeader !== required) {
    return new Response('unauthorized', {
      status: 401,
    });
  }
  return undefined;
}

export type WithRoomID = {roomID: string};
export function withRoomID<Context extends BaseContext, Resp>(
  next: Handler<Context & WithRoomID, Resp>,
) {
  return (req: Request, ctx: Context) => {
    const {roomID} = ctx.parsedURL.pathname.groups;
    if (roomID === undefined) {
      throw new Error('Internal error: roomID not found by withRoomID');
    }
    const decoded = decodeURIComponent(roomID);
    return next(req, {...ctx, roomID: decoded});
  };
}

export function asJSON<Context extends BaseContext>(
  next: Handler<Context, ReadonlyJSONValue>,
) {
  return async (req: Request, ctx: Context) =>
    new Response(JSON.stringify(await next(req, ctx)));
}
