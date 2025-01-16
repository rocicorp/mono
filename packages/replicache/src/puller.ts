import type {Cookie} from './cookies.js';
import type {
  ClientStateNotFoundResponse,
  VersionNotSupportedResponse,
} from './error-responses.js';
import type {HTTPRequestInfo} from './http-request-info.js';
import type {
  PatchOperation,
  PatchOperationInternal,
} from './patch-operation.js';
import type {ClientID} from './sync/ids.js';
import type {PullRequest} from './sync/pull.js';

// TODO(arv): Does it really make sense to call this httpRequestInfo? It is
// really the response status code and error message!

export type PullerResultV1 = {
  response?: PullResponseV1 | undefined;
  httpRequestInfo: HTTPRequestInfo;
};

export type PullerResult = PullerResultV1;

/**
 * Puller is the function type used to do the fetch part of a pull.
 *
 * Puller needs to support dealing with pull request of version 0 and 1. Version
 * 0 is used when doing mutation recovery of old clients. If a
 * {@link PullRequestV1} is passed in the n a {@link PullerResultV1} should
 * be returned. We do a runtime assert to make this is the case.
 *
 * If you do not support old clients you can just throw if `pullVersion` is `0`,
 */
export type Puller = (
  requestBody: PullRequest,
  requestID: string,
) => Promise<PullerResult>;

/**
 * The shape of a pull response under normal circumstances.
 */
export type PullResponseOKV1 = {
  cookie: Cookie;
  // All last mutation IDs from clients in clientGroupID that changed
  // between PullRequest.cookie and PullResponseOK.cookie.
  lastMutationIDChanges: Record<ClientID, number>;
  patch: PatchOperation[];
};

export type PullResponseOKV1Internal = {
  cookie: Cookie;
  // All last mutation IDs from clients in clientGroupID that changed
  // between PullRequest.cookie and PullResponseOK.cookie.
  lastMutationIDChanges: Record<ClientID, number>;
  patch: PatchOperationInternal[];
};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
export type PullResponseV1 =
  | PullResponseOKV1
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;

export type PullResponseV1Internal =
  | PullResponseOKV1Internal
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;

export type PullResponse = PullResponseV1;
