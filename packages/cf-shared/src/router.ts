import type {LogContext} from '@rocicorp/logger';
import {API_KEY_HEADER_NAME} from 'shared/src/api/headers.js';
import {makeAPIResponse} from 'shared/src/api/responses.js';
import {APIError, makeAPIErrorResponse} from './api-errors.js';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {decodeHeaderValue} from 'shared/src/headers.js';
import {HttpError, makeErrorResponse} from './errors.js';
import type {MaybePromise} from 'shared/src/types.js';

/**
 * Handles a request dispatched by router. The Handler is
 * invoked after it has passed through any number of
 * {@link RequestValidator}s.
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
    return makeAPIErrorResponse({
      code: 404,
      resource: 'request',
      message: 'Unknown or invalid URL',
    });
  }
}

function requireMethod<Context extends BaseContext>(
  method: string,
): RequestValidator<Context> {
  return (ctx: Context, request: Request) => {
    if (request.method !== method) {
      throw new APIError(405, 'request', 'unsupported method');
    }
    return {ctx};
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
): RequestValidator<Context> {
  return (ctx: Context, req: Request) => {
    checkAuthAPIKey(required(ctx), req);
    return {ctx};
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

export function roomID<Context extends BaseContext>(): RequestValidator<
  Context,
  Context & {roomID: string}
> {
  return (ctx: Context) => {
    const {roomID} = ctx.parsedURL.pathname.groups;
    assert(roomID, 'roomID() configured for URL without :roomID group');
    const decoded = decodeURIComponent(roomID);
    return {ctx: {...ctx, roomID: decoded}};
  };
}

export function userID<Context extends BaseContext>(): RequestValidator<
  Context,
  Context & {userID: string}
> {
  return (ctx: Context) => {
    const {userID} = ctx.parsedURL.pathname.groups;
    assert(userID, 'userID() configured for URL without :userID group');
    const decoded = decodeURIComponent(userID);
    return {ctx: {...ctx, userID: decoded}};
  };
}

export function urlVersion<Context extends BaseContext>(): RequestValidator<
  Context,
  Context & {version: number}
> {
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
    return {ctx: {...ctx, version}};
  };
}

// Note: queryParams(), bodyOnly(), and noInputParams() are mutually exclusive.
export function queryParams<T, Context extends BaseContext>(
  schema: valita.Type<T>,
): RequestValidator<Context, Context & {query: T; body: null}> {
  return inputParams<T, null, Context>(schema, valita.null());
}

export function bodyOnly<T, Context extends BaseContext>(
  schema: valita.Type<T>,
): RequestValidator<Context, Context & {body: T; query: null}> {
  return inputParams<null, T, Context>(valita.null(), schema);
}

export function bearerToken<Context extends BaseContext>(): RequestValidator<
  Context,
  Context & {bearerToken: string}
> {
  return bearerTokenImpl(true);
}

export function optionalBearerToken<
  Context extends BaseContext,
>(): RequestValidator<Context, Context & {bearerToken?: string}> {
  return bearerTokenImpl(false);
}

function bearerTokenImpl<Context extends BaseContext>(
  required: true,
): RequestValidator<Context, Context & {bearerToken: string}>;
function bearerTokenImpl<Context extends BaseContext>(
  required: false,
): RequestValidator<Context, Context & {bearerToken: string}>;
function bearerTokenImpl<Context extends BaseContext>(
  required: boolean,
): RequestValidator<Context, Context & {bearerToken?: string}> {
  function throwError(kind: string): never {
    throw new APIError(401, 'request', `${kind} Authorization header`);
  }
  return (ctx: Context, req: Request) => {
    const authHeader = req.headers.get('Authorization');
    if (authHeader === null) {
      if (required) {
        throwError('Missing');
      }
      return {ctx};
    }

    const parts = authHeader.split(/\s+/);
    if (parts.length !== 2) {
      throwError('Invalid');
    }
    const authScheme = parts[0].toLowerCase();
    const token = parts[1];
    if (authScheme !== 'bearer') {
      throwError('Invalid');
    }
    try {
      const decoded = decodeHeaderValue(token);
      return {
        ctx: {...ctx, bearerToken: decoded},
      };
    } catch {
      throwError('Malformed');
    }
  };
}

const arbitraryQueryParamsSchema = valita.record(valita.string());

// For reportMetrics the client sends common query parameters that the server ignores.
export function bodyAndArbitraryQueryParams<T, Context extends BaseContext>(
  schema: valita.Type<T>,
): RequestValidator<
  Context,
  Context & {body: T; query: Record<string, string>}
> {
  return inputParams<Record<string, string>, T, Context>(
    arbitraryQueryParamsSchema,
    schema,
  );
}

export function noInputParams<Context extends BaseContext>(): RequestValidator<
  Context,
  Context & {body: null; query: null}
> {
  return inputParams<null, null, Context>(valita.null(), valita.null());
}

export function inputParams<Q, B, Context extends BaseContext>(
  querySchema: valita.Type<Q>,
  bodySchema: valita.Type<B>,
): RequestValidator<Context, Context & {query: Q; body: B}> {
  return async (ctx: Context, req: Request) => {
    const {parsedURL} = ctx;
    const query = validateQuery(parsedURL, querySchema);
    const text = await req.text();
    const body = validateBody(text, bodySchema);
    return {
      ctx: {...ctx, query, body},
      // Create a new Request if the body of the input Request was consumed.
      req: !req.bodyUsed ? req : new Request(req, {body: text}),
    };
  };
}

export function queryParamsIgnoreBody<Q, Context extends BaseContext>(
  querySchema: valita.Type<Q>,
): RequestValidator<Context, Context & {query: Q}> {
  return (ctx: Context) => {
    const {parsedURL} = ctx;
    const query = validateQuery(parsedURL, querySchema);
    return {
      ctx: {...ctx, query},
    };
  };
}

function validateQuery<T>(
  parsedURL: URLPatternURLPatternResult,
  schema: valita.Type<T>,
): T {
  const queryString = parsedURL.search.input;

  // Parses duplicate keys as arrays.
  const params = new Map<string, string | string[]>();
  for (const [key, val] of new URLSearchParams(queryString).entries()) {
    const existing = params.get(key);
    if (Array.isArray(existing)) {
      existing.push(val);
    } else if (typeof existing === 'string') {
      params.set(key, [existing, val]);
    } else {
      params.set(key, val);
    }
  }
  const queryObj = Object.fromEntries(params);
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

function validateBody<T>(text: string, schema: valita.Type<T>): T {
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

/**
 * A RequestValidator validates an `Input` context and returns
 * a (possibly augmented) `Output` context to be passed to the next
 * RequestValidator, and eventually to the {@link Handler}.
 *
 * If the RequestValidator reads the `body` of the Request, it should
 * return a new copy of the Request so that subsequent logic in the pipeline
 * can read the body (e.g. `new Request(req, {body: ...})`). If no request
 * is returned, the original request is passed to the next validator or handler.
 */
type RequestValidator<
  Input extends BaseContext,
  Output extends BaseContext = Input,
> = (ctx: Input, req: Request) => MaybePromise<{ctx: Output; req?: Request}>;

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
      return nextValidator(next.ctx, next.req ?? req);
    });
  }

  #validateAndHandle(
    handler: Handler<Output, Response>,
  ): Handler<Input, Response> {
    return async (origCtx: Input, request: Request) => {
      try {
        const validated = await this.#requestValidator(origCtx, request);
        return await handler(validated.ctx, validated.req ?? request);
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
