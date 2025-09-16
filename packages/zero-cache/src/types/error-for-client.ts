/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
