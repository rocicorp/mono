/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {
  consoleLogSink,
  LogContext,
  TeeLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';

/**
 * Creates a LogContext
 * @param logLevel The log level to use. Default is `'info'`.
 * @param logSinks Destination for logs. Default is `[consoleLogSink]`.
 * @param context Optional: Additional information that can be associated with logs.
 * @returns A LogContext instance configured with the provided options.
 */
export function createLogContext(
  logLevel: LogLevel = 'info',
  logSinks: LogSink[] = [consoleLogSink],
  context?: Context | undefined,
): LogContext {
  const logSink =
    logSinks.length === 1 ? logSinks[0] : new TeeLogSink(logSinks);
  return new LogContext(logLevel, context, logSink);
}
