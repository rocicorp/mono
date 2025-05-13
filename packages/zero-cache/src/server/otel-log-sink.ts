import {
  logs,
  type Logger,
  SeverityNumber,
  type LogRecord,
  type AnyValueMap,
} from '@opentelemetry/api-logs';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {errorOrObject} from './logging.ts';
import {stringify} from '../types/bigint-json.ts';
import {startOtelAuto} from './otel-start.ts';

export class OtelLogSink implements LogSink {
  readonly #logger: Logger;

  constructor() {
    // start otel in case it was not started yet
    // this is a no-op if already started
    this.#logger = logs.getLogger('zero-cache');
    startOtelAuto();
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    const lastObj = errorOrObject(args.at(-1));
    if (lastObj) {
      args.pop();
    }

    let message = args.length
      ? args.map(s => (typeof s === 'string' ? s : stringify(s))).join(' ')
      : '';

    if (lastObj) {
      message += ` ${stringify(lastObj)}`;
    }

    const payload: LogRecord = {
      severityNumber: toErrorNum(level),
      severityText: level,
      body: message,
      timestamp: Date.now() * 1_000_000, // nanoseconds
    };
    if (context) {
      payload.attributes = context as AnyValueMap;
    }

    this.#logger.emit(payload);
  }
}

function toErrorNum(level: LogLevel): SeverityNumber {
  switch (level) {
    case 'error':
      return SeverityNumber.ERROR;
    case 'warn':
      return SeverityNumber.WARN;
    case 'info':
      return SeverityNumber.INFO;
    case 'debug':
      return SeverityNumber.DEBUG;
    default:
      throw new Error(`Unknown log level: ${level}`);
  }
}
