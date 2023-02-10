import {
  assertArray,
  assertNumber,
  assertObject,
  assertString,
} from './asserts.js';
import {httpRequest} from './http-request.js';
import {assertJSONValue, ReadonlyJSONValue} from './json.js';
import type {HTTPRequestInfo} from './http-request-info.js';
import type {ClientID} from './sync/ids.js';

export type PullerResult = {
  response?: PullResponse | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

// TODO(arv): Does it really make sense to call this httpRequestInfo? It is
// really the response status code and error message!

export type PullerResultDD31 = {
  response?: PullResponseDD31 | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * Puller is the function type used to do the fetch part of a pull. The request
 * is a POST request where the body is JSON with the type {@link PullRequest}.
 */
export type Puller = (request: Request) => Promise<PullerResult>;

/**
 * Puller is the function type used to do the fetch part of a pull. The request
 * is a POST request where the body is JSON with the type {@link PullRequest}.
 */
export type PullerDD31 = (request: Request) => Promise<PullerResultDD31>;

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOK = {
  cookie?: ReadonlyJSONValue | undefined;
  lastMutationID: number;
  patch: PatchOperation[];
};

export type {PullResponseOK as PullResponseOKSDD};

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOKDD31 = {
  // TODO(DD31): Use more specific types for cookie
  cookie: ReadonlyJSONValue;
  // All last mutation IDs from clients in clientGroupID that changed
  // between PullRequest.cookie and PullResponseOK.cookie.
  lastMutationIDChanges: Record<ClientID, number>;
  patch: PatchOperation[];
};

/**
 * In certain scenarios the server can signal that it does not know about the
 * client. For example, the server might have deleted the client.
 */
export type ClientStateNotFoundResponse = {
  error: 'ClientStateNotFound';
};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponse = PullResponseOK | ClientStateNotFoundResponse;

export type {PullResponse as PullResponseSDD};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponseDD31 = PullResponseOKDD31 | ClientStateNotFoundResponse;

export function isClientStateNotFoundResponse(
  result: unknown,
): result is ClientStateNotFoundResponse {
  return (
    typeof result === 'object' &&
    result !== null &&
    (result as Partial<ClientStateNotFoundResponse>).error ===
      'ClientStateNotFound'
  );
}

export function assertPullResponseSDD(v: unknown): asserts v is PullResponse {
  if (typeof v !== 'object' || v === null) {
    throw new Error('PullResponse must be an object');
  }
  if (isClientStateNotFoundResponse(v)) {
    return;
  }
  const v2 = v as Partial<PullResponseOK>;
  if (v2.cookie !== undefined) {
    assertJSONValue(v2.cookie);
  }
  assertNumber(v2.lastMutationID);
  assertPatchOperations(v2.patch);
}

export function assertPullResponseDD31(
  v: unknown,
): asserts v is PullResponseDD31 {
  if (typeof v !== 'object' || v === null) {
    throw new Error('PullResponseDD31 must be an object');
  }
  if (isClientStateNotFoundResponse(v)) {
    return;
  }
  const v2 = v as Partial<PullResponseOKDD31>;
  if (v2.cookie !== undefined) {
    assertJSONValue(v2.cookie);
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

/**
 * This type describes the patch field in a {@link PullResponse} and it is used
 * to describe how to update the Replicache key-value store.
 */
export type PatchOperation =
  | {
      readonly op: 'put';
      readonly key: string;
      readonly value: ReadonlyJSONValue;
    }
  | {
      readonly op: 'del';
      readonly key: string;
    }
  | {
      readonly op: 'clear';
    };

const defaultPullerShared = async (request: Request) => {
  const {httpRequestInfo, response} = await httpRequest(request);
  if (httpRequestInfo.httpStatusCode !== 200) {
    return {
      httpRequestInfo,
    };
  }
  // TODO(greg): Should this assertPullResponseDD31, we also assert
  // in pull.ts/pulldd31.ts (since it may be a non default puller
  // that doesn't assert itself)
  return {
    response: await response.json(),
    httpRequestInfo,
  };
};

export const defaultPuller: Puller = defaultPullerShared;

export const defaultPullerDD31: PullerDD31 = defaultPullerShared;

export function assertPatchOperations(
  p: unknown,
): asserts p is PatchOperation[] {
  assertArray(p);
  for (const item of p) {
    assertPatchOperation(item);
  }
}

function assertPatchOperation(p: unknown): asserts p is PatchOperation {
  assertObject(p);
  switch (p.op) {
    case 'put':
      assertString(p.key);
      assertJSONValue(p.value);
      break;
    case 'del':
      assertString(p.key);
      break;
    case 'clear':
      break;
    default:
      throw new Error(
        `unknown patch op \`${p.op}\`, expected one of \`put\`, \`del\`, \`clear\``,
      );
  }
}

/**
 * This error is thrown when the puller fails for any reason.
 */
export class PullError extends Error {
  name = 'PullError';
  // causedBy is used instead of cause, because while cause has been proposed as a
  // JavaScript language standard for this purpose (see
  // https://github.com/tc39/proposal-error-cause) current browser behavior is
  // inconsistent.
  causedBy?: Error | undefined;
  constructor(causedBy?: Error) {
    super('Failed to pull');
    this.causedBy = causedBy;
  }
}
