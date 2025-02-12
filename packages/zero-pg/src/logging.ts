import {LogContext, consoleLogSink, type LogLevel} from '@rocicorp/logger';

export function createLogContext(level: LogLevel): LogContext {
  return new LogContext(level, {}, consoleLogSink);
}
