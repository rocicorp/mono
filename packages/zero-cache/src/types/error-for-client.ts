import type {LogLevel} from '@rocicorp/logger';
import type {ErrorBody} from '../../../zero-protocol/src/error.js';

export class ErrorWithLevel extends Error {
  readonly logLevel: LogLevel;

  constructor(
    msg: string,
    logLevel: LogLevel = 'error',
    options?: ErrorOptions,
  ) {
    super(msg, options);
    this.logLevel = logLevel;
  }
}

export function getLogLevel(error: unknown) {
  return error instanceof ErrorWithLevel ? error.logLevel : 'error';
}

export class ErrorForClient extends ErrorWithLevel {
  readonly errorBody;
  constructor(
    errorBody: ErrorBody,
    logLevel: LogLevel = 'warn', // 'warn' by default most of these are not server errors
    options?: ErrorOptions,
  ) {
    super(JSON.stringify(errorBody), logLevel, options);
    this.errorBody = errorBody;
  }
}

export function findErrorForClient(error: unknown): ErrorForClient | undefined {
  if (error instanceof ErrorForClient) {
    return error;
  }
  if (error instanceof Error && error.cause) {
    return findErrorForClient(error.cause);
  }
  return undefined;
}
