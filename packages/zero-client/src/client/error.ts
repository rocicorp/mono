import type {Expand} from '../../../shared/src/expand.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {BackoffBody, ErrorBody} from '../../../zero-protocol/src/error.ts';
import {ClientErrorKind} from './client-error-kind.ts';

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
    super(errorBody.kind + ': ' + errorBody.message, options);
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

export function isBackoffError(ex: unknown): BackoffBody | undefined {
  if (isServerError(ex)) {
    switch (ex.errorBody.kind) {
      case ErrorKind.Rebalance:
      case ErrorKind.Rehome:
      case ErrorKind.ServerOverloaded:
        return ex.errorBody;
    }
  }
  return undefined;
}

export function isClientError(ex: unknown): ex is ClientError {
  return ex instanceof ClientError;
}
