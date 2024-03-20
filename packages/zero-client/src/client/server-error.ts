import type {ErrorKind} from 'reflect-protocol';

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
): ex is ServerError<'AuthInvalidated'> | ServerError<'Unauthorized'> {
  return isServerError(ex) && isAuthErrorKind(ex.kind);
}

function isAuthErrorKind(
  kind: ErrorKind,
): kind is 'AuthInvalidated' | 'Unauthorized' {
  return kind === 'AuthInvalidated' || kind === 'Unauthorized';
}
