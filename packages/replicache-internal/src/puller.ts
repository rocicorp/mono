import {assertArray, assertNumber, assertObject, assertString} from './asserts';
import {httpRequest} from './http-request';
import {assertJSONValue, JSONValue, ReadonlyJSONValue} from './json';
import type {HTTPRequestInfo} from './http-request-info';
import type {ClientID} from './sync/client-id';

export type PullerResult = {
  response?: PullResponse;
  httpRequestInfo: HTTPRequestInfo;
};

export type PullerResultDD31 = {
  response?: PullResponseDD31;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * Puller is the function type used to do the fetch part of a pull. The request
 * is a POST request where the body is JSON with the type [[PullRequest]].
 */
export type Puller = (request: Request) => Promise<PullerResult>;

export type PullerDD31 = (request: Request) => Promise<PullerResultDD31>;

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOK = {
  cookie?: ReadonlyJSONValue;
  lastMutationID: number;
  patch: PatchOperation[];
};

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOKDD31 = {
  cookie?: ReadonlyJSONValue;
  lastMutationIDs: Record<ClientID, number>;
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

export function assertPullResponse(v: unknown): asserts v is PullResponse {
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
  assertLastMutationIDs(v2.lastMutationIDs);
  assertPatchOperations(v2.patch);
}

/**
 * This type describes the patch field in a [[PullResponse]] and it is used
 * to describe how to update the Replicache key-value store.
 */
export type PatchOperation =
  | {
      op: 'put';
      key: string;
      value: JSONValue;
    }
  | {op: 'del'; key: string}
  | {op: 'clear'};

export const defaultPuller: Puller = async request => {
  const {httpRequestInfo, response} = await httpRequest(request);
  if (httpRequestInfo.httpStatusCode !== 200) {
    return {
      httpRequestInfo,
    };
  }
  return {
    response: await response.json(),
    httpRequestInfo,
  };
};

export const defaultPullerDD31: PullerDD31 = async request => {
  const {httpRequestInfo, response} = await httpRequest(request);
  if (httpRequestInfo.httpStatusCode !== 200) {
    return {
      httpRequestInfo,
    };
  }
  // TODO(greg): Should this assertPullResponseDD31, we also assert
  // in pull.ts/pulldd31.ts (since it may be a non default puller
  // that doesnt assert itself)
  return {
    response: await response.json(),
    httpRequestInfo,
  };
};

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

function assertLastMutationIDs(
  lastMutationIDs: unknown,
): asserts lastMutationIDs is Record<string, number> {
  assertObject(lastMutationIDs);
  for (const [key, value] of Object.entries(lastMutationIDs)) {
    assertString(key);
    assertNumber(value);
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
  causedBy?: Error;
  constructor(causedBy?: Error) {
    super('Failed to pull');
    this.causedBy = causedBy;
  }
}
