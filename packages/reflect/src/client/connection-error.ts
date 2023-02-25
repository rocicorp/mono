import {ErrorKind} from 'reflect-protocol';

export class MessageError<K extends ErrorKind = ErrorKind> extends Error {
  readonly kind: K;
  readonly name = 'MessageError';
  constructor(kind: K, message: string) {
    super(kind + ': ' + message);
    this.kind = kind;
  }
}

export function isAuthError(
  ex: unknown,
): ex is
  | MessageError<ErrorKind.AuthInvalidated>
  | MessageError<ErrorKind.Unauthorized> {
  return ex instanceof MessageError && isAuthErrorKind(ex.kind);
}

function isAuthErrorKind(
  kind: ErrorKind,
): kind is ErrorKind.AuthInvalidated | ErrorKind.Unauthorized {
  return kind === ErrorKind.AuthInvalidated || kind === ErrorKind.Unauthorized;
}
