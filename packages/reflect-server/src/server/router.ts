import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';
import {API_KEY_HEADER_NAME} from 'shared/src/api/headers.js';
import {makeAPIResponse} from 'shared/src/api/responses.js';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {APIError} from './api-errors.js';
import {HttpError, makeErrorResponse} from './errors.js';

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
    return new APIError(404, 'request', 'Unknown or invalid URL').response();
  }
}

function requireMethod<Context extends BaseContext>(method: string) {
  return (ctx: Context, request: Request) => {
    if (request.method !== method) {
      throw new APIError(405, 'request', 'unsupported method');
    }
    return ctx;
  };
}

export function get<Context extends BaseContext = BaseContext>() {
  return new ValidatorChainer<Context, Context>(requireMethod('GET'));
}

export function post<Context extends BaseContext = BaseContext>() {
  return new ValidatorChainer<Context, Context>(requireMethod('POST'));
}

export function requiredAuthAPIKey<Context extends BaseContext>(
  required: (context: Context) => string,
) {
  return (ctx: Context, req: Request) => {
    checkAuthAPIKey(required(ctx), req);
    return ctx;
  };
}

const LEGACY_API_KEY_HEADER_NAME = 'x-reflect-auth-api-key';

export function checkAuthAPIKey(
  required: string | undefined,
  request: Request,
) {
  if (!required) {
    throw new HttpError(
      500,
      'Internal error: expected auth api key cannot be empty',
    );
  }

  const authHeader =
    request.headers.get(API_KEY_HEADER_NAME) ??
    request.headers.get(LEGACY_API_KEY_HEADER_NAME);
  if (authHeader !== required) {
    throw new HttpError(401, 'Unauthorized');
  }
}

export function roomID<Context extends BaseContext>() {
  return (ctx: Context) => {
    const {roomID} = ctx.parsedURL.pathname.groups;
    assert(roomID, 'roomID() configured for URL without :roomID group');
    const decoded = decodeURIComponent(roomID);
    return {...ctx, roomID: decoded};
  };
}

export function userID<Context extends BaseContext>() {
  return (ctx: Context) => {
    const {userID} = ctx.parsedURL.pathname.groups;
    assert(userID, 'userID() configured for URL without :userID group');
    const decoded = decodeURIComponent(userID);
    return {...ctx, userID: decoded};
  };
}

export function urlVersion<Context extends BaseContext>() {
  return (ctx: Context, req: Request) => {
    const {version: versionString} = ctx.parsedURL.pathname.groups;
    if (versionString === undefined) {
      throw new HttpError(
        500,
        'version not found by withVersion url: ' + req.url,
      );
    }
    if (!/^v\d+$/.test(versionString)) {
      throw new HttpError(
        500,
        `invalid version found by withVersion, ${versionString}`,
      );
    }
    const version = Number(versionString.slice(1));
    return {...ctx, version};
  };
}

// Note: queryParams(), body(), and noInputParams() are mutually exclusive.
// (Currently, no endpoints read both they query string and the request body, but
//  if there need arises, inputParams() can be exported).
export function queryParams<T, Context extends BaseContext>(
  schema: valita.Type<T>,
) {
  return inputParams<T, null, Context>(schema, valita.null());
}

export function body<T, Context extends BaseContext>(schema: valita.Type<T>) {
  return inputParams<null, T, Context>(valita.null(), schema);
}

export function noInputParams<Context extends BaseContext>() {
  return inputParams<null, null, Context>(valita.null(), valita.null());
}

function inputParams<Q, B, Context extends BaseContext>(
  querySchema: valita.Type<Q>,
  bodySchema: valita.Type<B>,
) {
  return async (ctx: Context, req: Request) => {
    const {parsedURL} = ctx;
    const query = validateQuery(parsedURL, querySchema);
    const body = await validateBody(req, bodySchema);
    return {...ctx, query, body};
  };
}

function validateQuery<T>(
  parsedURL: URLPatternURLPatternResult,
  schema: valita.Type<T>,
): T {
  const queryString = parsedURL.search.input;
  const queryObj = Object.fromEntries(
    new URLSearchParams(queryString).entries(),
  );
  if (schema.name === 'null') {
    if (Object.keys(queryObj).length > 0) {
      throw new APIError(400, 'request', 'Unexpected query parameters');
    }
    return valita.parse(null, schema);
  }
  try {
    return valita.parse(queryObj, schema);
  } catch (e) {
    throw new APIError(
      400,
      'request',
      'Query string error. ' + (e as Error).message,
    );
  }
}

async function validateBody<T>(
  request: Request,
  schema: valita.Type<T>,
): Promise<T> {
  // Note: we don't clone the request here, because if we did clone and the
  // original request body is not consumed CF complains in the console, "Your
  // worker called response.clone(), but did not read the body of both
  // clones. <snip>". Routes that use validateBody, should use
  // the result and not try to read the body, as reading the body
  // again will result in an error "TypeError: body used already for <snip>".
  const text = await request.text();
  if (schema.name === 'null') {
    if (text.length > 0) {
      throw new APIError(400, 'request', 'Unexpected request body.');
    }
    return valita.parse(null, schema);
  }
  let json;
  try {
    json = JSON.parse(text);
  } catch (e) {
    throw new APIError(400, 'request', 'Body must be valid json.');
  }
  try {
    return valita.parse(json, schema);
  } catch (e) {
    throw new APIError(
      400,
      'request',
      'Body schema error. ' + (e as Error).message,
    );
  }
}

type RequestValidator<
  Input extends BaseContext,
  Output extends BaseContext = Input,
> = (ctx: Input, req: Request) => MaybePromise<Output>;

class ValidatorChainer<Input extends BaseContext, Output extends BaseContext> {
  readonly #requestValidator: RequestValidator<Input, Output>;

  constructor(requestValidator: RequestValidator<Input, Output>) {
    this.#requestValidator = requestValidator;
  }

  /**
   * Used to chain ContextValidators that convert / augment
   * the final context passed to the handler.
   */
  with<Next extends BaseContext>(
    nextValidator: RequestValidator<Output, Next>,
  ): ValidatorChainer<Input, Next> {
    return new ValidatorChainer(async (prev, req) => {
      const next = await this.#requestValidator(prev, req);
      return nextValidator(next, req);
    });
  }

  #validateAndHandle(
    handler: Handler<Output, Response>,
  ): Handler<Input, Response> {
    return async (origCtx: Input, request: Request) => {
      try {
        const ctx = await this.#requestValidator(origCtx, request);
        return await handler(ctx, request);
      } catch (e) {
        return makeErrorResponse(e);
      }
    };
  }

  handle(handler: Handler<Output, Response>): Handler<Input, Response> {
    return this.#validateAndHandle(handler);
  }

  handleJSON(
    handler: Handler<Output, ReadonlyJSONValue>,
  ): Handler<Input, Response> {
    return this.handle(
      async (ctx: Output, request: Request) =>
        new Response(JSON.stringify(await handler(ctx, request)), {
          headers: {'Content-Type': 'application/json'},
        }),
    );
  }

  handleAPIResult<Result extends ReadonlyJSONValue | void>(
    handler: Handler<Output, Result>,
  ): Handler<Input, Response> {
    return this.handleJSON(async (ctx: Output, request: Request) =>
      makeAPIResponse((await handler(ctx, request)) ?? {}),
    );
  }
}
