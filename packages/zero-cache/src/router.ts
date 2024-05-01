import {makeAPIError, type APIErrorInfo} from 'shared/src/api/responses.js';
import type {LogContext} from '@rocicorp/logger';

export type MaybePromise<T> = T | Promise<T>;

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

export function makeAPIErrorResponse(info: APIErrorInfo): Response {
  const apiResponse = makeAPIError(info);
  return new Response(JSON.stringify(apiResponse), {
    status: info.code,
    headers: {'Content-Type': 'application/json'},
  });
}
