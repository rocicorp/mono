import {assertArray, assertNumber, assertObject, assertString} from './asserts';
import {httpRequest} from './http-request';
import {assertJSONValue, JSONValue, ReadonlyJSONValue} from './json';
import type {HTTPRequestInfo} from './http-request-info';
import {
  ClientStateNotFoundResponse,
  isClientStateNotFoundResponse,
} from './sync/errors';

export type PullerResult = {
  response?: PullResponse;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * Puller is the function type used to do the fetch part of a pull. The request
 * is a POST request where the body is JSON with the type [[PullRequest]].
 */
export type Puller = (request: Request) => Promise<PullerResult>;

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOK = {
  cookie?: ReadonlyJSONValue;
  lastMutationID: number;
  patch: PatchOperation[];
};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponse = PullResponseOK | ClientStateNotFoundResponse;

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
  causedBy?: Error;
  constructor(causedBy?: Error) {
    super('Failed to pull');
    this.causedBy = causedBy;
  }
}
