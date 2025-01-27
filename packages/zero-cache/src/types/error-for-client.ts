import type {LogLevel} from '@rocicorp/logger';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';

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

export function getLogLevel(error: unknown): LogLevel {
  return error instanceof ErrorWithLevel ? error.logLevel : 'error';
}

export class ErrorForClient extends ErrorWithLevel {
  readonly errorBody;
  constructor(
    errorBody: ErrorBody,
    logLevel: LogLevel = 'warn', // 'warn' by default since these are generally not server issues
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
