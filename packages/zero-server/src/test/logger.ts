import {Context, LogContext, LogLevel, LogSink} from '@rocicorp/logger';

export class SilentLogSink implements LogSink {
  log(_l: LogLevel, _c: Context | undefined, ..._args: unknown[]): void {
    return;
  }
}

export function createSilentLogContext() {
  return new LogContext('error', undefined, new SilentLogSink());
}
