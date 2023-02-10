import {httpRequest} from './http-request';
import type {HTTPRequestInfo} from './http-request-info';
import type {PushResponse} from './sync/push';

/**
 * @deprecated Use `PusherResult` instead.
 */
export type LegacyPusherResult = HTTPRequestInfo;
export type PusherResult = {
  response: PushResponse;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * Pusher is the function type used to do the fetch part of a push. The request
 * is a POST request where the body is JSON with the type [[PushRequest]].
 *
 * Implementations can return [[LegacyPusherResult]], but it's recommended to
 * return [[PusherResult]] as that will become required in the future.
 */
export type Pusher = (
  request: Request,
) => Promise<LegacyPusherResult | PusherResult>;

export const defaultPusher: Pusher = async request => {
  const {httpRequestInfo, response} = await httpRequest(request);
  if (httpRequestInfo.httpStatusCode !== 200) {
    return {
      httpRequestInfo,
      response: {},
    };
  }
  if (response.headers.get('content-type') === 'application/json') {
    return {
      response: await response.json(),
      httpRequestInfo,
    };
  }
  return {
    httpRequestInfo,
    response: {},
  };
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
  causedBy?: Error;
  constructor(causedBy?: Error) {
    super('Failed to push');
    this.causedBy = causedBy;
  }
}
