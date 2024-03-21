import {
  consoleLogSink,
  TeeLogSink,
  LogContext,
  type LogLevel,
  type LogSink,
  Context,
} from '@rocicorp/logger';

/**
 * Creates a LogContext
 *
 * @param opts - Optional logging configuration options.
 * @param operation - A unique identifier for the operation.
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
