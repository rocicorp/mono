import {
  assertNumber,
  assertObject,
  assertString,
} from '../../shared/src/asserts.js';
import {callDefaultFetch} from './call-default-fetch.js';
import {assertCookie} from './cookies.js';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from './error-responses.js';
import {assertHTTPRequestInfo} from './http-request-info.js';
import {assertPatchOperations} from './patch-operation.js';
import type {
  PullResponseV1,
  Puller,
  PullerResult,
  PullerResultV1,
} from './puller.js';
import type {PullRequest} from './sync/pull.js';

/**
 * This creates a default puller which uses HTTP POST to send the pull request.
 */
export function getDefaultPuller(rep: {pullURL: string; auth: string}): Puller {
  async function puller(
    requestBody: PullRequest,
    requestID: string,
  ): Promise<PullerResult> {
    const [response, httpRequestInfo] = await callDefaultFetch(
      rep.pullURL,
      rep.auth,
      requestID,
      requestBody,
    );
    if (!response) {
      return {httpRequestInfo};
    }

    return {
      response: await response.json(),
      httpRequestInfo,
    };
  }

  defaultPullers.add(puller);
  return puller;
}

const defaultPullers = new WeakSet();

export function isDefaultPuller(puller: Puller): boolean {
  return defaultPullers.has(puller);
}

export function assertPullResponseV1(v: unknown): asserts v is PullResponseV1 {
  assertObject(v);
  if (isClientStateNotFoundResponse(v) || isVersionNotSupportedResponse(v)) {
    return;
  }
  const v2 = v as Record<string, unknown>;
  if (v2.cookie !== undefined) {
    assertCookie(v2.cookie);
  }
  assertLastMutationIDChanges(v2.lastMutationIDChanges);
  assertPatchOperations(v2.patch);
}

function assertLastMutationIDChanges(
  lastMutationIDChanges: unknown,
): asserts lastMutationIDChanges is Record<string, number> {
  assertObject(lastMutationIDChanges);
  for (const [key, value] of Object.entries(lastMutationIDChanges)) {
    assertString(key);
    assertNumber(value);
  }
}

export function assertPullerResultV1(v: unknown): asserts v is PullerResultV1 {
  assertObject(v);
  assertHTTPRequestInfo(v.httpRequestInfo);
  if (v.response !== undefined) {
    assertPullResponseV1(v.response);
  }
}
