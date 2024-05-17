import {ErrorKind} from 'zero-protocol';

/**
 * Represents an error sent by server as part of Zero protocol.
 */
export class ServerError<K extends ErrorKind = ErrorKind> extends Error {
  readonly kind: K;
  readonly name = 'ServerError';
  constructor(kind: K, message: string) {
    super(kind + ': ' + message);
    this.kind = kind;
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
