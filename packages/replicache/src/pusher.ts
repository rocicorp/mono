import {assertHTTPRequestInfo, HTTPRequestInfo} from './http-request-info.js';
import {assertObject} from 'shared';
import {
  assertVersionNotSupportedResponse,
  ClientStateNotFoundResponse,
  isClientStateNotFoundResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';
import type {PushRequestDD31, PushRequestSDD} from './sync/push.js';

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
  requestBody: PushRequestDD31 | PushRequestSDD,
  requestID: string,
) => Promise<PusherResult>;

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
