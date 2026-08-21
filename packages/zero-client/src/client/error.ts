import {unreachable} from '../../../shared/src/asserts.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  type BackoffBody,
  type ErrorBody,
  isRetryableHTTPStatus,
  isProtocolError,
  type ProtocolError,
  type PushFailedBody,
  type TransformFailedBody,
} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';

export type AuthError = ProtocolError<NeedsAuthReason>;
export type ClientErrorBody = {
  kind: ClientErrorKind;
  origin: typeof ErrorOrigin.Client;
  message: string;
};
export type ClosedError = ClientError<{
  kind: ClientErrorKind.ClientClosed;
  message: string;
}>;
export type NeedsAuthReason = Expand<
  | (ErrorBody & {
      kind: ErrorKind.AuthInvalidated | ErrorKind.Unauthorized;
    })
  | (Extract<PushFailedBody, {reason: ErrorReason.HTTP}> & {status: 401 | 403})
  | (Extract<TransformFailedBody, {reason: ErrorReason.HTTP}> & {
      status: 401 | 403;
    })
>;
export type OfflineError = ClientError<{
  kind: ClientErrorKind.Offline;
  message: string;
}>;
export type NoSocketOriginError = ClientError<{
  kind: ClientErrorKind.NoSocketOrigin;
  message: string;
}>;
export type HiddenError = ClientError<{
  kind: ClientErrorKind.Hidden;
  message: string;
}>;
export type DisconnectedReason =
  | OfflineError
  | NoSocketOriginError
  | HiddenError;
export type ServerError = ProtocolError<ErrorBody>;
export type ZeroError = ServerError | ClientError;
export type ZeroErrorBody = Expand<ErrorBody | ClientErrorBody>;
export type ZeroErrorDetails = Expand<Omit<ZeroErrorBody, 'message'>>;
export type ZeroErrorKind = Expand<ErrorKind | ClientErrorKind>;

/**
 * Represents an error encountered by the Zero client.
 */
export class ClientError<
  const T extends Omit<ClientErrorBody, 'origin'> = Omit<
    ClientErrorBody,
    'origin'
  >,
> extends Error {
  readonly errorBody: {origin: typeof ErrorOrigin.Client} & T;

  constructor(errorBody: T, options?: ErrorOptions) {
    super(errorBody.message, options);
    this.name = 'ClientError';
    this.errorBody = {...errorBody, origin: ErrorOrigin.Client};
  }

  get kind(): T['kind'] {
    return this.errorBody.kind;
  }
}

export function isZeroError(ex: unknown): ex is ZeroError {
  return isClientError(ex) || isServerError(ex);
}

export function isClientError(ex: unknown): ex is ClientError<ClientErrorBody> {
  return (
    ex instanceof ClientError && ex.errorBody.origin === ErrorOrigin.Client
  );
}

export function isServerError(ex: unknown): ex is ServerError {
  return (
    isProtocolError(ex) &&
    (ex.errorBody.origin === ErrorOrigin.Server ||
      ex.errorBody.origin === ErrorOrigin.ZeroCache)
  );
}

export function isOfflineError(ex: unknown): ex is OfflineError {
  return isClientError(ex) && ex.kind === ClientErrorKind.Offline;
}

export function isAuthError(ex: unknown): ex is AuthError {
  if (isServerError(ex)) {
    if (
      ex.kind === ErrorKind.AuthInvalidated ||
      ex.kind === ErrorKind.Unauthorized
    ) {
      return true;
    }
    if (
      (ex.errorBody.kind === ErrorKind.PushFailed ||
        ex.errorBody.kind === ErrorKind.TransformFailed) &&
      ex.errorBody.reason === ErrorReason.HTTP &&
      (ex.errorBody.status === 401 || ex.errorBody.status === 403)
    ) {
      return true;
    }
  }

  return false;
}

export function getBackoffParams(error: ZeroError): BackoffBody | undefined {
  if (isServerError(error)) {
    switch (error.errorBody.kind) {
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return error.errorBody;
    }
  }
  return undefined;
}

export const NO_STATUS_TRANSITION = 'NO_STATUS_TRANSITION';
export const MAX_AMBIGUOUS_ERROR_RETRIES = 3;

type RetryTransition = {
  status: typeof NO_STATUS_TRANSITION;
  reason: ZeroError;
  /**
   * Stop retrying after this many reconnects. Omitted for errors known to be
   * transient.
   */
  maxRetries?: number | undefined;
};

export type ErrorConnectionTransition =
  | RetryTransition
  | {status: ConnectionStatus.NeedsAuth; reason: AuthError}
  | {status: ConnectionStatus.Error; reason: ZeroError}
  | {status: ConnectionStatus.Disconnected; reason: DisconnectedReason}
  | {status: ConnectionStatus.Closed; reason: ZeroError};

/**
 * Returns the status to transition to, or null if the error
 * indicates that the connection should continue in the current state.
 */
export function getErrorConnectionTransition(
  ex: unknown,
): ErrorConnectionTransition {
  // Handle auth errors by transitioning to needs-auth state
  if (isAuthError(ex)) {
    return {
      status: ConnectionStatus.NeedsAuth,
      reason: ex,
    } as const;
  }

  if (isClientError(ex)) {
    switch (ex.kind) {
      // Connecting errors that should continue in the current state
      case ClientErrorKind.AbruptClose:
      case ClientErrorKind.CleanClose:
      case ClientErrorKind.ConnectTimeout:
      case ClientErrorKind.PingTimeout:
      case ClientErrorKind.PullTimeout:
      case ClientErrorKind.UnexpectedBaseCookie:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

      // Internal is the catch-all for unexpected exceptions (poke processing,
      // socket event handlers). Reconnecting starts a fresh snapshot, which
      // clears many of them. Bound the retries because a deterministic client
      // bug can otherwise reconnect forever.
      case ClientErrorKind.Internal:
        return {
          status: NO_STATUS_TRANSITION,
          reason: ex,
          maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
        } as const;

      // Fatal errors that should transition to error state.
      // InvalidMessage means we sent a message the server refused (e.g. a
      // mutation over the size limit). Reconnecting re-sends it from IndexedDB,
      // so retrying here would loop forever; the caller pairs this with
      // disableClientGroup() + onClientStateNotFound.
      case ClientErrorKind.InvalidMessage:
      case ClientErrorKind.UserDisconnect:
        return {status: ConnectionStatus.Error, reason: ex} as const;

      // Disconnected errors
      case ClientErrorKind.Hidden:
      case ClientErrorKind.Offline:
      case ClientErrorKind.NoSocketOrigin:
        return {
          status: ConnectionStatus.Disconnected,
          reason: ex as DisconnectedReason,
        } as const;

      // Closed error (this should already result in a closed state)
      case ClientErrorKind.ClientClosed:
        return {status: ConnectionStatus.Closed, reason: ex} as const;

      default:
        unreachable(ex.kind);
    }
  }

  if (isServerError(ex)) {
    // Switch on the body rather than the `kind` getter so that TypeScript
    // narrows the body's per-kind fields (e.g. `reason`).
    const body = ex.errorBody;
    switch (body.kind) {
      // Errors that should transition to error state
      case ErrorKind.ClientNotFound:
      case ErrorKind.InvalidConnectionRequest:
      case ErrorKind.InvalidConnectionRequestBaseCookie:
      case ErrorKind.InvalidConnectionRequestLastMutationID:
      case ErrorKind.InvalidConnectionRequestClientDeleted:
      case ErrorKind.InvalidMessage:
      case ErrorKind.InvalidPush:
      case ErrorKind.VersionNotSupported:
      case ErrorKind.SchemaVersionNotSupported:
        return {status: ConnectionStatus.Error, reason: ex} as const;

      case ErrorKind.PushFailed:
      case ErrorKind.TransformFailed:
        return pushOrTransformFailedTransition(ex, body);

      // Errors that should continue with backoff/retry
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

      case ErrorKind.Internal: {
        if (body.retryable === false) {
          return {status: ConnectionStatus.Error, reason: ex} as const;
        }
        if (body.retryable === true) {
          return {status: NO_STATUS_TRANSITION, reason: ex} as const;
        }
        // The next connection may land on a healthy task, but an unmarked
        // Internal can also be a deterministic bug. Give it a bounded chance
        // to recover.
        return {
          status: NO_STATUS_TRANSITION,
          reason: ex,
          maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
        } as const;
      }

      // Auth errors are handled above by isAuthError check
      case ErrorKind.AuthInvalidated:
      case ErrorKind.Unauthorized:
        return {
          status: ConnectionStatus.NeedsAuth,
          reason: ex as AuthError,
        } as const;

      // Mutation-specific errors don't affect connection state
      case ErrorKind.MutationRateLimited:
      case ErrorKind.MutationFailed:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

      default:
        unreachable(body);
    }
  }

  // Catch-all for unexpected errors. Give the connection a bounded chance to
  // recover without allowing an unknown deterministic error to loop forever.
  return {
    status: NO_STATUS_TRANSITION,
    reason: new ClientError(
      {
        kind: ClientErrorKind.Internal,
        message: 'Unexpected internal error: ' + getErrorMessage(ex),
      },
      {cause: ex},
    ),
    maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
  } as const;
}

function pushOrTransformFailedTransition(
  error: ServerError,
  body: PushFailedBody | TransformFailedBody,
): Extract<
  ErrorConnectionTransition,
  {status: typeof NO_STATUS_TRANSITION | ConnectionStatus.Error}
> {
  // Newer servers make the decision explicit. Fall back to fields understood
  // by older servers when the marker is absent.
  if (body.retryable === true) {
    return {status: NO_STATUS_TRANSITION, reason: error};
  }
  if (body.retryable === false) {
    return {status: ConnectionStatus.Error, reason: error};
  }

  switch (body.reason) {
    case ErrorReason.HTTP:
      return isRetryableHTTPStatus(body.status)
        ? {status: NO_STATUS_TRANSITION, reason: error}
        : {status: ConnectionStatus.Error, reason: error};
    case ErrorReason.Timeout:
      return {status: NO_STATUS_TRANSITION, reason: error};
    case ErrorReason.Database:
    case ErrorReason.Internal:
      return {
        status: NO_STATUS_TRANSITION,
        reason: error,
        maxRetries: MAX_AMBIGUOUS_ERROR_RETRIES,
      };
    case ErrorReason.Parse:
    case ErrorReason.OutOfOrderMutation:
    case ErrorReason.UnsupportedPushVersion:
      return {status: ConnectionStatus.Error, reason: error};
    default:
      unreachable(body);
  }
}
