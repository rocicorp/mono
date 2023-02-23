import type {HTTPRequestInfo} from './http-request-info.js';
import type {ClientID} from './sync/ids.js';
import type {
  ClientStateNotFoundResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';
import type {Cookie} from './cookies.js';
import type {PullRequestDD31, PullRequestSDD} from './sync/pull.js';
import type {PatchOperation} from './patch-operation.js';
import type {ReadonlyJSONValue} from './json.js';

export type PullerResultSDD = {
  response?: PullResponseSDD | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

// TODO(arv): Does it really make sense to call this httpRequestInfo? It is
// really the response status code and error message!

export type PullerResultDD31 = {
  response?: PullResponseDD31 | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

/**
 * Puller is the function type used to do the fetch part of a pull.
 *
 * Puller needs to support dealing with pull request of version 0 and 1. Version
 * 0 is used when doing mutation recovery of old clients. If a
 * {@link PullRequestDD31} is passed in the n a {@link PullerResultDD31} should
 * be returned. We do a runtime assert to make this is the case.
 *
 * If you do not support old clients you can just throw if `pullVersion` is `0`,
 */
export type Puller = (
  requestBody: PullRequestDD31 | PullRequestSDD,
  requestID: string,
) => Promise<PullerResultDD31 | PullerResultSDD>;

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOKSDD = {
  cookie?: ReadonlyJSONValue | undefined;
  lastMutationID: number;
  patch: PatchOperation[];
};

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOKDD31 = {
  cookie: Cookie;
  // All last mutation IDs from clients in clientGroupID that changed
  // between PullRequest.cookie and PullResponseOK.cookie.
  lastMutationIDChanges: Record<ClientID, number>;
  patch: PatchOperation[];
};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponseSDD =
  | PullResponseOKSDD
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponseDD31 =
  | PullResponseOKDD31
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;
