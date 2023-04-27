import type {ErrorKind} from 'reflect-protocol';

/**
 * Represents an error sent by server as part of Reflect protocol.
 */
export class ServerError<K extends ErrorKind = ErrorKind> extends Error {
  readonly kind: K;
  readonly name = 'ServerError';
  constructor(kind: K, message: string) {
    super(kind + ': ' + message);
    this.kind = kind;
  }
}

export function isAuthError(
  ex: unknown,
): ex is ServerError<'AuthInvalidated'> | ServerError<'Unauthorized'> {
  return ex instanceof ServerError && isAuthErrorKind(ex.kind);
}

function isAuthErrorKind(
  kind: ErrorKind,
): kind is 'AuthInvalidated' | 'Unauthorized' {
  return kind === 'AuthInvalidated' || kind === 'Unauthorized';
}
