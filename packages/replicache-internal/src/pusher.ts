import {httpRequest} from './http-request.js';
import type {HTTPRequestInfo} from './http-request-info.js';

/**
 * Pusher is the function type used to do the fetch part of a push. The request
 * is a POST request where the body is JSON with the type {@link PushRequest}.
 */
export type Pusher = (request: Request) => Promise<HTTPRequestInfo>;

export const defaultPusher: Pusher = async request => {
  return (await httpRequest(request)).httpRequestInfo;
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
