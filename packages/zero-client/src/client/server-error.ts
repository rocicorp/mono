import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.js';
import {
  type BackoffBody,
  type ErrorBody,
} from '../../../zero-protocol/src/error.js';

type ErrorKind = (typeof ErrorKind)[keyof typeof ErrorKind];

/**
 * Represents an error sent by server as part of Zero protocol.
 */
export class ServerError<K extends ErrorKind = ErrorKind> extends Error {
  readonly name = 'ServerError';
  readonly errorBody: ErrorBody;
  get kind(): K {
    return this.errorBody.kind as K;
  }

  constructor(errorBody: ErrorBody) {
    super(errorBody.kind + ': ' + errorBody.message);
    this.errorBody = errorBody;
  }
}

export function isServerError(ex: unknown): ex is ServerError {
  return ex instanceof ServerError;
}

export function isAuthError(
  ex: unknown,
): ex is
  | ServerError<ErrorKind.AuthInvalidated>
  | ServerError<ErrorKind.Unauthorized> {
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
