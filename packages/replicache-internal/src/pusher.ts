import {httpRequest} from './http-request.js';
import {assertHTTPRequestInfo, HTTPRequestInfo} from './http-request-info.js';
import {assertObject} from './asserts.js';
import {
  assertVersionNotSupportedResponse,
  ClientStateNotFoundResponse,
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';

export type PusherResult = {
  response?: PushResponse | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * The response from a push can contain information about error conditions.
 */
export type PushResponse =
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;

export function assertPusherResult(v: unknown): asserts v is PusherResult {
  assertObject(v);
  assertHTTPRequestInfo(v.httpRequestInfo);
  if (v.response !== undefined) {
    assertPushResponse(v.response);
  }
}

function assertPushResponse(v: unknown): asserts v is PushResponse {
  if (isClientStateNotFoundResponse(v)) {
    return;
  }
  assertVersionNotSupportedResponse(v);
}

function isPushResponse(v: unknown): v is PushResponse {
  return isClientStateNotFoundResponse(v) || isVersionNotSupportedResponse(v);
}

/**
 * Pusher is the function type used to do the fetch part of a push. The request
 * is a POST request where the body is JSON with the type {@link PushRequest}.
 *
 * The return value should either be a {@link HTTPRequestInfo} or a
 * {@link PusherResult}. The reason for the two different return types is that
 * we didn't use to care about the response body of the push request. The
 * default pusher implementation checks if the response body is JSON and if it
 * matches the type {@link PusherResponse}. If it does, it is included in the
 * return value.
 */
export type Pusher = (
  request: Request,
) => Promise<HTTPRequestInfo | PusherResult>;

export const defaultPusher: Pusher = async request => {
  const {response, httpRequestInfo} = await httpRequest(request);
  if (httpRequestInfo.httpStatusCode === 200) {
    // In case we get an error response, we have already consumed the response body.
    let json;
    try {
      json = await response.json();
    } catch {
      // Ignore JSON parse errors. It is valid to return a non-JSON response.
      return httpRequestInfo;
    }

    if (isPushResponse(json)) {
      return {
        response: json,
        httpRequestInfo,
      };
    }
  }
  return httpRequestInfo;
};

/**
 * This error is thrown when the pusher fails for any reason.
 */
export class PushError extends Error {
  name = 'PushError';
  // causedBy is used instead of cause, because while cause has been proposed as a
  // JavaScript language standard for this purpose (see
  // https://github.com/tc39/proposal-error-cause) current browser behavior is
  // inconsistent.
  causedBy?: Error | undefined;
  constructor(causedBy?: Error) {
    super('Failed to push');
    this.causedBy = causedBy;
  }
}
