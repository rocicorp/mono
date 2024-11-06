import {
  LogContext,
  consoleLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {pid} from 'node:process';
import {type LogConfig} from '../config/zero-config.js';
import {stringify} from '../types/bigint-json.js';

function createLogSink(config: LogConfig) {
  return config.format === 'json' ? consoleJsonLogSink : consoleLogSink;
}

export function createLogContext(
  config: LogConfig,
  context: {worker: string},
): LogContext {
  const ctx = {...context, pid};
  return new LogContext(config.level, ctx, createLogSink(config));
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

function errorOrObject(v: unknown): object | undefined {
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
