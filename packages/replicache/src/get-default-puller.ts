import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from './error-responses.js';
import type {PullRequestDD31, PullRequestSDD} from './sync/pull.js';
import type {
  Puller,
  PullerResultDD31,
  PullerResultSDD,
  PullResponseDD31,
  PullResponseOKSDD,
  PullResponseSDD,
} from './puller.js';
import {assertNumber, assertObject, assertString} from './asserts.js';
import {assertPatchOperations} from './patch-operation.js';
import {assertJSONValue} from './json.js';
import {assertHTTPRequestInfo} from './http-request-info.js';
import {callDefaultFetch} from './call-default-fetch.js';
import {assertCookie} from './cookies.js';

/**
 * This creates a default puller which uses HTTP POST to send the pull request.
 */
export function getDefaultPuller(rep: {pullURL: string; auth: string}): Puller {
  async function puller(
    requestBody: PullRequestDD31 | PullRequestSDD,
    requestID: string,
  ): Promise<PullerResultDD31 | PullerResultSDD> {
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

export function assertPullResponseSDD(
  v: unknown,
): asserts v is PullResponseSDD {
  assertObject(v);
  if (isClientStateNotFoundResponse(v) || isVersionNotSupportedResponse(v)) {
    return;
  }
  const v2 = v as Partial<PullResponseOKSDD>;
  if (v2.cookie !== undefined) {
    assertJSONValue(v2.cookie);
  }
  assertNumber(v2.lastMutationID);
  assertPatchOperations(v2.patch);
}

export function assertPullResponseDD31(
  v: unknown,
): asserts v is PullResponseDD31 {
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

export function assertPullerResultDD31(
  v: unknown,
): asserts v is PullerResultDD31 {
  assertObject(v);
  assertHTTPRequestInfo(v.httpRequestInfo);
  if (v.response !== undefined) {
    assertPullResponseDD31(v.response);
  }
}

export function assertPullerResultSDD(
  v: unknown,
): asserts v is PullerResultSDD {
  assertObject(v);
  assertHTTPRequestInfo(v.httpRequestInfo);
  if (v.response !== undefined) {
    assertPullResponseSDD(v.response);
  }
}
