import {
  FormatLogger,
  logLevelPrefix,
  type LogLevel,
  type LogSink,
  type Context,
  LogContext,
} from '@rocicorp/logger';
import {stringify} from './bigint-json.ts';
import chalk from 'chalk';
import {pid} from 'node:process';

export type LogConfig = {
  level: LogLevel;
  format: 'text' | 'json';
};

const colors = {
  debug: chalk.grey,
  info: chalk.whiteBright,
  warn: chalk.yellow,
  error: chalk.red,
};

export const consoleSink = new FormatLogger((level, ...args) => [
  colors[level](
    logLevelPrefix[level],
    ...args.map(s => (typeof s === 'string' ? s : stringify(s))),
  ),
]);

export function getLogSink(config: LogConfig): LogSink {
  return config.format === 'json' ? consoleJsonLogSink : consoleSink;
}

export function createLogContext(
  {log}: {log: LogConfig},
  context = {},
  sink = getLogSink(log),
): LogContext {
  const ctx = {pid, ...context};
  const lc = new LogContext(log.level, ctx, sink);
  // Emit a blank line to absorb random ANSI control code garbage that
  // for some reason gets prepended to the first log line in CloudWatch.
  lc.info?.('');
  return lc;
}

const consoleJsonLogSink: LogSink = {
  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    // If the last arg is an object or an Error, combine those fields into the message.
    const lastObj = errorOrObject(args.at(-1));
    if (lastObj) {
      args.pop();
    }
    const message = args.length
      ? {
          message: args
            .map(s => (typeof s === 'string' ? s : stringify(s)))
            .join(' '),
        }
      : undefined;

    // eslint-disable-next-line no-console
    console[level](
      stringify({
        level: level.toUpperCase(),
        ...context,
        ...lastObj,
        ...message,
      }),
    );
  },
};

export function errorOrObject(v: unknown): object | undefined {
  if (v instanceof Error) {
    return {
      ...v, // some properties of Error subclasses may be enumerable
      name: v.name,
      errorMsg: v.message,
      stack: v.stack,
      ...('cause' in v ? {cause: errorOrObject(v.cause)} : null),
    };
  }
  if (v && typeof v === 'object') {
    return v;
  }
  return undefined;
}
