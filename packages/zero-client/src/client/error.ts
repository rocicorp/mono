import {unreachable} from '../../../shared/src/asserts.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {BackoffBody, ErrorBody} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {ConnectionStatus} from './connection-status.ts';

export type ClientErrorBody = {
  kind: ClientErrorKind;
  message: string;
};

export type ZeroErrorBody = Expand<ErrorBody | ClientErrorBody>;
export type ZeroErrorKind = Expand<ErrorKind | ClientErrorKind>;

abstract class BaseError<
  T extends ErrorBody | ClientErrorBody,
  Name extends T extends ErrorBody ? 'ServerError' : 'ClientError',
> extends Error {
  readonly errorBody: T;
  constructor(errorBody: T, options?: ErrorOptions) {
    super(errorBody.message, options);
    this.errorBody = errorBody;
  }
  get kind(): T['kind'] {
    return this.errorBody.kind;
  }
  abstract get name(): Name;
}

/**
 * Represents an error sent by server as part of Zero protocol.
 */
export class ServerError extends BaseError<ErrorBody, 'ServerError'> {
  get name() {
    return 'ServerError' as const;
  }
}

/**
 * Represents an error encountered by the client.
 */
export class ClientError extends BaseError<ClientErrorBody, 'ClientError'> {
  get name() {
    return 'ClientError' as const;
  }
}

export type ZeroError = ServerError | ClientError;

export function isServerError(ex: unknown): ex is ServerError {
  return ex instanceof ServerError;
}

export function isAuthError(ex: unknown): ex is ServerError & {
  kind: ErrorKind.AuthInvalidated | ErrorKind.Unauthorized;
} {
  return isServerError(ex) && isAuthErrorKind(ex.kind);
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

export function isClientError(ex: unknown): ex is ClientError {
  return ex instanceof ClientError;
}

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
        return {status: null, reason: ex} as const;

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
        return {status: ConnectionStatus.Error, reason: ex} as const;

      // Errors that should continue with backoff/retry
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return {status: null, reason: ex} as const;

      // Auth errors will eventually transition to needs-auth state
      // For now, treat them as non-fatal so we can retry
      case ErrorKind.AuthInvalidated:
      case ErrorKind.Unauthorized:
        return {status: null, reason: ex} as const;

      // Mutation-specific errors don't affect connection state
      case ErrorKind.MutationRateLimited:
      case ErrorKind.MutationFailed:
        return {status: null, reason: ex} as const;

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
