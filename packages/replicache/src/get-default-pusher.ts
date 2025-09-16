/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {callDefaultFetch} from './call-default-fetch.ts';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from './error-responses.ts';
import type {Pusher, PusherResult} from './pusher.ts';
import type {PushRequest} from './sync/push.ts';

/**
 * This creates a default pusher which uses HTTP POST to send the push request.
 */
export function getDefaultPusher(rep: {pushURL: string; auth: string}): Pusher {
  async function pusher(
    requestBody: PushRequest,
    requestID: string,
  ): Promise<PusherResult> {
    const [response, httpRequestInfo] = await callDefaultFetch(
      rep.pushURL,
      rep.auth,
      requestID,
      requestBody,
    );
    if (!response) {
      return {httpRequestInfo};
    }

    const rv: PusherResult = {
      httpRequestInfo,
    };

    let result;
    try {
      result = await response.json();
    } catch {
      // Ignore JSON parse errors. It is valid to return a non-JSON response.
      return rv;
    }

    if (
      isClientStateNotFoundResponse(result) ||
      isVersionNotSupportedResponse(result)
    ) {
      rv.response = result;
    }

    return rv;
  }

  defaultPushers.add(pusher);
  return pusher;
}

const defaultPushers = new WeakSet();

export function isDefaultPusher(pusher: Pusher): boolean {
  return defaultPushers.has(pusher);
}
