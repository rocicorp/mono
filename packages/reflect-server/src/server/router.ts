import type {LogContext} from '@rocicorp/logger';
import type {MaybePromise} from 'replicache';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import {API_KEY_HEADER_NAME} from './api-headers.js';
import {makeAPIResponse} from './api-response.js';
import {HttpError, makeErrorResponse} from './errors.js';
import {makeListControl} from './list.js';

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

function requireMethod<Context extends BaseContext>(method: string) {
  return (ctx: Context, request: Request) => {
    if (request.method !== method) {
      throw new HttpError(405, 'unsupported method');
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

export function listControl<Context extends BaseContext>(
  maxMaxResults: number,
) {
  return (ctx: Context) => {
    const {parsedURL} = ctx;
    const listControl = makeListControl(parsedURL.search.input, maxMaxResults);
    return {...ctx, listControl};
  };
}

export function roomID<Context extends BaseContext>() {
  return (ctx: Context) => {
    const {roomID} = ctx.parsedURL.pathname.groups;
    if (roomID === undefined) {
      throw new HttpError(500, 'Internal error: roomID not found');
    }
    const decoded = decodeURIComponent(roomID);
    return {...ctx, roomID: decoded};
  };
}

export function userID<Context extends BaseContext>() {
  return (ctx: Context) => {
    const {userID} = ctx.parsedURL.pathname.groups;
    if (userID === undefined) {
      throw new HttpError(500, 'Internal error: userID not found');
    }
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

export function body<T, Context extends BaseContext>(schema: valita.Type<T>) {
  return async (ctx: Context, req: Request) => {
    const body = await validateBody(req, schema);
    return {...ctx, body};
  };
}

async function validateBody<T>(
  request: Request,
  schema: valita.Type<T>,
): Promise<T> {
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
    throw new HttpError(400, 'Body must be valid json.');
  }
  const validateResult = valita.test(json, schema);
  if (!validateResult.ok) {
    throw new HttpError(400, 'Body schema error. ' + validateResult.error);
  }
  return validateResult.value;
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
        new Response(JSON.stringify(await handler(ctx, request))),
    );
  }

  handleAPIResult(
    handler: Handler<Output, ReadonlyJSONValue>,
  ): Handler<Input, Response> {
    return this.handleJSON(async (ctx: Output, request: Request) =>
      makeAPIResponse(await handler(ctx, request)),
    );
  }
}
