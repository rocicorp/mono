import {
  type Context,
  LogContext,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';

export class TestLogSink implements LogSink {
  messages: [LogLevel, Context | undefined, unknown[]][] = [];
  flushCallCount = 0;

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    this.messages.push([level, context, args]);
  }

  flush() {
    this.flushCallCount++;
    return Promise.resolve();
  }
}

export class SilentLogSink implements LogSink {
  log(_l: LogLevel, _c: Context | undefined, ..._args: unknown[]): void {
    return;
  }
}

export function createSilentLogContext() {
  return new LogContext('error', undefined, new SilentLogSink());
}
