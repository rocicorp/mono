import {
  LogContext,
  consoleLogSink,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';

export function createLogContext(
  level: LogLevel,
  sink: LogSink = consoleLogSink,
): LogContext {
  return new LogContext(level, {}, sink);
}
