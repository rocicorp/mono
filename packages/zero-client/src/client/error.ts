import {unreachable} from '../../../shared/src/asserts.ts';
import {getErrorMessage} from '../../../shared/src/error.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import type {ProtocolError} from '../../../zero-protocol/src/error.ts';
import {
  type BackoffBody,
  type ErrorBody,
  isProtocolError,
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

export type ErrorConnectionTransition =
  | {status: typeof NO_STATUS_TRANSITION; reason: ZeroError}
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

      // Fatal errors that should transition to error state
      case ClientErrorKind.Internal:
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
    switch (ex.kind) {
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
      case ErrorKind.Internal:
      // PushFailed and TransformFailed can be auth errors (401/403)
      // or other errors - handle non-auth cases here
      case ErrorKind.PushFailed:
      case ErrorKind.TransformFailed:
        return {status: ConnectionStatus.Error, reason: ex} as const;

      // Errors that should continue with backoff/retry
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

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
        unreachable(ex.kind);
    }
  }

  // we default to error state if we don't know what to do
  // this is a catch-all for unexpected errors
  return {
    status: ConnectionStatus.Error,
    reason: new ClientError(
      {
        kind: ClientErrorKind.Internal,
        message: 'Unexpected internal error: ' + getErrorMessage(ex),
      },
      {cause: ex},
    ),
  } as const;
}
