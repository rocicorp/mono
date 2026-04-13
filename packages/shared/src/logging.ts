import {pid} from 'node:process';
import {styleText} from 'node:util';
/* oxlint-disable no-console */
import {
  type Context,
  LogContext,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {stringify} from './bigint-json.ts';

export type LogConfig = {
  level: LogLevel;
  format: 'text' | 'json';
};

function style(color: 'gray' | 'yellow' | 'red', args: unknown[]) {
  return styleText(color, args.join(' '));
}

const COLOR_DEBUG = 'gray';
const COLOR_WARN = 'yellow';
const COLOR_ERROR = 'red';

/**
 * Returns an object for writing colorized output to a provided console.
 * Note this should only be used when console is a TTY (i.e., Node).
 */
export const colorConsole = {
  log: (...args: unknown[]) => {
    console.log(...args);
  },
  debug: (...args: unknown[]) => {
    console.debug(style(COLOR_DEBUG, args));
  },
  info: (...args: unknown[]) => {
    console.info(...args);
  },
  warn: (...args: unknown[]) => {
    console.warn(style(COLOR_WARN, args));
  },
  error: (...args: unknown[]) => {
    console.error(style(COLOR_ERROR, args));
  },
};

export const consoleSink: LogSink = {
  log(level, context, ...args) {
    colorConsole[level](
      toLocalIsoString(),
      stringifyContext(context),
      ...args.map(stringifyValue),
    );
  },
};

function toLocalIsoString(date = new Date()) {
  const tzo = -date.getTimezoneOffset();
  const sign = tzo >= 0 ? '+' : '-';
  const pad = (n: number, len = 2) => String(Math.abs(n)).padStart(len, '0');

  return (
    date.getFullYear() +
    '-' +
    pad(date.getMonth() + 1) +
    '-' +
    pad(date.getDate()) +
    'T' +
    pad(date.getHours()) +
    ':' +
    pad(date.getMinutes()) +
    ':' +
    pad(date.getSeconds()) +
    '.' +
    pad(date.getMilliseconds(), 3) +
    sign +
    pad(tzo / 60) +
    ':' +
    pad(tzo % 60)
  );
}

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
          message: args.map(stringifyValue).join(' '),
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

export function errorOrObject(v: unknown): object | undefined {
  if (v instanceof Error) {
    return toErrorLogObject(v);
  }
  if (v && typeof v === 'object') {
    return v;
  }
  return undefined;
}

function toErrorLogObject(v: Error) {
  return {
    ...v, // some properties of Error subclasses may be enumerable
    name: v.name,
    errorMsg: v.message,
    stack: v.stack,
    ...('cause' in v ? {cause: errorOrObject(v.cause)} : null),
  };
}

function stringifyContext(context: Context | undefined): unknown[] {
  const args = [];
  for (const [k, v] of Object.entries(context ?? {})) {
    const arg = v === undefined ? k : `${k}=${v}`;
    args.push(arg);
  }
  return args;
}

function stringifyValue(v: unknown) {
  if (typeof v === 'string') {
    return v;
  }
  if (v instanceof Error) {
    return stringify(toErrorLogObject(v));
  }
  return stringify(v);
}
