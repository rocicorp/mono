import {
  InvalidateForRoomRequest,
  invalidateForRoomRequestSchema,
  InvalidateForUserRequest,
  invalidateForUserRequestSchema,
} from '../protocol/api/auth.js';
import {Struct, validate} from 'superstruct';
import type {LogContext} from '@rocicorp/logger';
import {
  CreateRoomRequest,
  createRoomRequestSchema,
} from '../protocol/api/room.js';

export type Handler<T = undefined> = (
  this: Handlers,
  lc: LogContext,
  request: Request,
  body: T,
) => Promise<Response>;

// TODO This definition and dispatch() itself are used by *both* the
// authDO and the roomDO to route requests. This forces both DOs to
// implement exactly the same handlers and routes.
//
// We are moving to a model where each of {worker, authDO, roomDO} implements
// its own dispatch using itty-router. This enables each component to
// implement whatever routes it needs and reduces boilerplate. We still
// achieve the duplication-reduction goal of the approach here by sharing
// routes, see how worker.ts uses authDO routes.
//
// Don't add new routes to this list. Instead, add them to Router in the
// appropriate place (eg, worker or authDO).
//
// We should move these over to Router when we get a chance. Having two
// ways of doing something is bad.
export interface Handlers {
  createRoom: Handler<CreateRoomRequest>;
  connect: Handler;

  authInvalidateForUser: Handler<InvalidateForUserRequest>;
  authInvalidateForRoom: Handler<InvalidateForRoomRequest>;
  authInvalidateAll: Handler;
  authRevalidateConnections?: Handler;
  authConnections?: Handler;
}

export const paths: Readonly<Record<keyof Handlers, string>> = {
  createRoom: '/createRoom',
  connect: '/connect',
  authInvalidateForUser: '/api/auth/v0/invalidateForUser',
  authInvalidateForRoom: '/api/auth/v0/invalidateForRoom',
  authInvalidateAll: '/api/auth/v0/invalidateAll',
  authRevalidateConnections: '/api/auth/v0/revalidateConnections',
  authConnections: '/api/auth/v0/connections',
};

function createUnauthorizedResponse(message = 'Unauthorized'): Response {
  return new Response(message, {
    status: 401,
  });
}

function createBadRequestResponse(message = 'Bad Request'): Response {
  return new Response(message, {
    status: 400,
  });
}

export function dispatch(
  request: Request,
  lc: LogContext,
  // If authApiKey is not provided, then the auth api is disabled.
  authApiKey: string | undefined,
  handlers: Handlers,
): Promise<Response> {
  const url = new URL(request.url);
  lc.debug?.('Dispatching path', url.pathname);

  async function validateAndDispatch<T>(
    method: string,
    validateBody: (request: Request) => Promise<ValidateResult<T>>,
    handler: Handler<T> | undefined,
    // If set to 'authApiKey' then auth api auth is enforced on the handler. It
    // checks that the auth api key has been passed in Authorization. The idea is
    // that we could have multiple api keys, and this string would select which
    // to require.
    apiKey?: 'authApiKey',
  ): Promise<Response> {
    if (!handler) {
      return createBadRequestResponse('Unsupported path.');
    }
    if (apiKey === 'authApiKey') {
      // Auth API is disabled so everything is unauthorized.
      if (authApiKey === undefined) {
        return createUnauthorizedResponse();
      }
      const authApiKeyHeaderValue = request.headers.get(
        'x-reflect-auth-api-key',
      );
      if (authApiKeyHeaderValue !== authApiKey) {
        return createUnauthorizedResponse();
      }
    }
    if (request.method.toLowerCase() !== method.toLowerCase()) {
      lc.error?.(`Unsupported method ${request.method.toLowerCase()}`);
      return new Response(`Method not allowed. Use "${method}".`, {
        status: 405,
      });
    }
    const validateResult = await validateBody(request);
    if (validateResult.errorResponse) {
      return validateResult.errorResponse;
    }
    lc.debug?.('Calling handler');
    return handler.call(handlers, lc, request, validateResult.value);
  }

  switch (url.pathname) {
    case paths.createRoom:
      return validateAndDispatch(
        'post',
        request => validateBody(request, createRoomRequestSchema),
        handlers.createRoom,
        'authApiKey',
      );
    case paths.connect:
      return validateAndDispatch('get', noOpValidateBody, handlers.connect);
    case paths.authInvalidateForUser:
      return validateAndDispatch(
        'post',
        request => validateBody(request, invalidateForUserRequestSchema),
        handlers.authInvalidateForUser,
        'authApiKey',
      );
    case paths.authInvalidateForRoom:
      return validateAndDispatch(
        'post',
        request => validateBody(request, invalidateForRoomRequestSchema),
        handlers.authInvalidateForRoom,
        'authApiKey',
      );
    case paths.authInvalidateAll:
      return validateAndDispatch(
        'post',
        noOpValidateBody,
        handlers.authInvalidateAll,
        'authApiKey',
      );
    case paths.authRevalidateConnections:
      return validateAndDispatch(
        'post',
        noOpValidateBody,
        handlers.authRevalidateConnections,
        'authApiKey',
      );
    case paths.authConnections:
      return validateAndDispatch(
        'get',
        noOpValidateBody,
        handlers.authConnections,
        'authApiKey',
      );
    default:
      return Promise.resolve(createBadRequestResponse('Unsupported path.'));
  }
}

const noOpValidateBody = () =>
  Promise.resolve({value: undefined, errorResponse: undefined});

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
      errorResponse: createBadRequestResponse(
        'Body schema error. ' + validateResult[0].message,
      ),
      value: undefined,
    };
  }
  return {
    value: validateResult[1],
    errorResponse: undefined,
  };
}
