import {
  LogContext,
  consoleLogSink,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {pid} from 'node:process';
import {type LogConfig, type ZeroConfig} from '../config/zero-config.ts';
import {stringify} from '../types/bigint-json.ts';

function createLogSink(config: LogConfig) {
  return config.format === 'json' ? consoleJsonLogSink : consoleLogSink;
}

export function createLogContext(
  config: Pick<ZeroConfig, 'log' | 'tenantID'>,
  context: {worker: string},
): LogContext {
  const {log, tenantID: tid} = config;
  const ctx = {
    ...((tid ?? '').length ? {tid} : {}),
    ...context,
    pid,
  };
  const lc = new LogContext(log.level, ctx, createLogSink(log));
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
