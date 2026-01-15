import type {LogLevel} from '@rocicorp/logger';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';

export class ProtocolErrorWithLevel extends ProtocolError {
  readonly logLevel: LogLevel;

  constructor(
    errorBody: ErrorBody,
    logLevel: LogLevel = 'warn',
    options?: ErrorOptions,
  ) {
    super(errorBody, options);
    this.logLevel = logLevel;
  }
}

export function getLogLevel(error: unknown): LogLevel {
  return error instanceof ProtocolErrorWithLevel
    ? error.logLevel
    : isProtocolError(error)
      ? 'warn'
      : 'error';
}

export function wrapWithProtocolError(error: unknown): ProtocolError {
  if (isProtocolError(error)) {
    return error;
  }

  return new ProtocolError(
    {
      kind: ErrorKind.Internal,
      message: getErrorMessage(error),
      origin: ErrorOrigin.ZeroCache,
    },
    {cause: error},
  );
}
