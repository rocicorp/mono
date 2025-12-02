import type {LogLevel} from '@rocicorp/logger';
import {
  ProtocolError,
  type ErrorBody,
} from '../../../zero-protocol/src/error.ts';

/**
 * A ProtocolError that includes a log level for controlling how the error
 * is logged on the server side.
 */
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
  return error instanceof ProtocolErrorWithLevel ? error.logLevel : 'error';
}
