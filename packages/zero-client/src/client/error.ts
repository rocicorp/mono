import {unreachable} from '../../../shared/src/asserts.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../zero-protocol/src/error-reason.ts';
import {
  type BackoffBody,
  type ErrorBody,
  isProtocolError,
  ProtocolError,
} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';

export type ZeroError = ProtocolError | ClientError;
export type ZeroErrorBody = Expand<ErrorBody | ClientErrorBody>;
export type ZeroErrorKind = Expand<ErrorKind | ClientErrorKind>;

export type ClientErrorBody = {
  kind: ClientErrorKind;
  origin: typeof ErrorOrigin.Client;
  message: string;
};

/**
 * Represents an error encountered by the client.
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

export function isServerError(ex: unknown): ex is ProtocolError<ErrorBody> {
  return (
    isProtocolError(ex) &&
    (ex.errorBody.origin === ErrorOrigin.Server ||
      ex.errorBody.origin === ErrorOrigin.ZeroCache)
  );
}

export function isAuthError(ex: unknown): ex is ProtocolError & {
  kind: ErrorKind.AuthInvalidated | ErrorKind.Unauthorized;
} {
  if (isServerError(ex)) {
    if (isAuthErrorKind(ex.errorBody.kind)) {
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

function isAuthErrorKind(
  kind: ErrorKind,
): kind is ErrorKind.AuthInvalidated | ErrorKind.Unauthorized {
  return kind === ErrorKind.AuthInvalidated || kind === ErrorKind.Unauthorized;
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

export function isClientError(ex: unknown): ex is ClientError<ClientErrorBody> {
  return (
    ex instanceof ClientError && ex.errorBody.origin === ErrorOrigin.Client
  );
}

export const NO_STATUS_TRANSITION = 'NO_STATUS_TRANSITION';

/**
 * Returns the status to transition to, or null if the error
 * indicates that the connection should continue in the current state.
 */
export function getErrorConnectionTransition(ex: unknown) {
  if (isClientError(ex)) {
    switch (ex.kind) {
      // Connecting errors that should continue in the current state
      case ClientErrorKind.AbruptClose:
      case ClientErrorKind.CleanClose:
      case ClientErrorKind.ConnectTimeout:
      case ClientErrorKind.PingTimeout:
      case ClientErrorKind.PullTimeout:
      case ClientErrorKind.Hidden:
      case ClientErrorKind.NoSocketOrigin:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

      // Fatal errors that should transition to error state
      case ClientErrorKind.UnexpectedBaseCookie:
      case ClientErrorKind.Internal:
      case ClientErrorKind.InvalidMessage:
      case ClientErrorKind.UserDisconnect:
        return {status: ConnectionStatus.Error, reason: ex} as const;

      // Disconnected error (this should already result in a disconnected state)
      case ClientErrorKind.DisconnectTimeout:
        return {status: ConnectionStatus.Disconnected, reason: ex} as const;

      // Closed error (this should already result in a closed state)
      case ClientErrorKind.ClientClosed:
        return {status: ConnectionStatus.Closed, reason: ex} as const;

      default:
        unreachable(ex.kind);
    }
  }

  // TODO(0xcadams): change this once we have a proper auth error handling flow
  if (isAuthError(ex)) {
    return {status: NO_STATUS_TRANSITION, reason: ex} as const;
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
      case ErrorKind.PushFailed:
      case ErrorKind.TransformFailed:
        return {status: ConnectionStatus.Error, reason: ex} as const;

      // Errors that should continue with backoff/retry
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

      // Auth errors will eventually transition to needs-auth state
      // For now, treat them as non-fatal so we can retry
      case ErrorKind.AuthInvalidated:
      case ErrorKind.Unauthorized:
        return {status: NO_STATUS_TRANSITION, reason: ex} as const;

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
    reason: new ClientError({
      kind: ClientErrorKind.Internal,
      message:
        'Unexpected internal error: ' +
        (ex instanceof Error
          ? ex.message
          : typeof ex === 'string'
            ? ex
            : String(ex ?? 'Unknown error')),
    }),
  } as const;
}
