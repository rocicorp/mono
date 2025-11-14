import type {LogContext, LogLevel} from '@rocicorp/logger';
import {getErrorMessage} from '../../../shared/src/error.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import {
  isProtocolError,
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';

const IS_ERROR_WITH_LEVEL = Symbol('isErrorWithLevel');

export class ErrorWithLevel extends Error {
  readonly logLevel: LogLevel;
  readonly [IS_ERROR_WITH_LEVEL] = true;

  constructor(
    message: string,
    logLevel: LogLevel = 'error',
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.logLevel = logLevel;
  }

  // Use duck-typing for instanceof since it would otherwise require
  // multiple inheritance for ProtocolErrorWithLevel to be instanceof
  // ErrorWithLevel.
  static [Symbol.hasInstance](instance: unknown): boolean {
    return (
      instance !== null &&
      typeof instance === 'object' &&
      IS_ERROR_WITH_LEVEL in instance &&
      (instance as Record<symbol, unknown>)[IS_ERROR_WITH_LEVEL] === true
    );
  }
}

export class ProtocolErrorWithLevel extends ProtocolError {
  readonly logLevel: LogLevel;
  readonly [IS_ERROR_WITH_LEVEL] = true;

  constructor(
    errorBody: ErrorBody,
    logLevel: LogLevel = 'error',
    options?: ErrorOptions,
  ) {
    super(errorBody, options);
    this.logLevel = logLevel;
  }
}

export function isErrorWithLevel(error: unknown): error is ErrorWithLevel {
  return error instanceof ErrorWithLevel;
}

export function getLogLevel(error: unknown): LogLevel {
  if (isErrorWithLevel(error)) {
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
  const level = isErrorWithLevel(e)
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
