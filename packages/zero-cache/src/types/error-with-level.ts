import type {LogLevel} from '@rocicorp/logger';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';

export class ProtocolErrorWithLevel extends ProtocolError {
  readonly logLevel: LogLevel;

  constructor(
    errorBody: ErrorBody,
    logLevel: LogLevel = 'error',
    options?: ErrorOptions,
  ) {
    super(errorBody, options);
    this.logLevel = logLevel;
  }
}

export function getLogLevel(error: unknown): LogLevel {
  return error instanceof ProtocolErrorWithLevel ? error.logLevel : 'error';
}

export function wrapWithProtocolError(error: unknown): ProtocolError {
  if (isProtocolError(error)) {
    return error;
  }

  return new ProtocolError(
    {
      kind: ErrorKind.Internal,
      message: error instanceof Error ? error.message : String(error),
      origin: ErrorOrigin.ZeroCache,
    },
    {cause: error},
  );
}
