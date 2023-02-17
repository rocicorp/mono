import {callDefaultFetch} from './call-default-fetch.js';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from './error-responses.js';
import type {Pusher, PusherResult} from './pusher.js';
import type {PushRequestDD31, PushRequestSDD} from './sync/push.js';

/**
 * This creates a default pusher which uses HTTP POST to send the push request.
 */
export function getDefaultPusher(rep: {pushURL: string; auth: string}): Pusher {
  async function pusher(
    requestBody: PushRequestDD31 | PushRequestSDD,
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
