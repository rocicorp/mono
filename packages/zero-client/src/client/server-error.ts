import {
  ErrorKind,
  type ErrorBody,
  type ServerOverloadedBody,
} from '../../../zero-protocol/src/error.js';

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

export function isServerOverloadedError(
  ex: unknown,
): ServerOverloadedBody | undefined {
  return isServerError(ex) && ex.errorBody.kind === ErrorKind.ServerOverloaded
    ? ex.errorBody
    : undefined;
}
