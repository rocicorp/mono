import type {LogContext, LogLevel} from '@rocicorp/logger';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';

export class ErrorWithLevel extends Error {
  readonly logLevel: LogLevel;

  constructor(
    message: string,
    logLevel: LogLevel = 'error',
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.logLevel = logLevel;
  }
}

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
  if (error instanceof ErrorWithLevel) {
    return error.logLevel;
  }
  if (error instanceof ProtocolErrorWithLevel) {
    return error.logLevel;
  }
  return isProtocolError(error) ? 'warn' : 'error';
}

export function logError(
  lc: LogContext,
  e: unknown,
  msg?: string,
  classify?: (e: unknown) => LogLevel,
): void {
  const level =
    e instanceof ErrorWithLevel || e instanceof ProtocolErrorWithLevel
      ? e.logLevel
      : classify
        ? classify(e)
        : 'error';
  msg ??= e instanceof Error ? e.message : String(e);
  lc[level]?.(msg, e);
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
