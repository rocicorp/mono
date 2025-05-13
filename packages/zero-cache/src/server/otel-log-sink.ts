import {logs, type Logger, type LogRecord} from '@opentelemetry/api-logs';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {errorOrObject} from './logging.ts';
import {stringify} from '../types/bigint-json.ts';
import {startOtelAuto} from './otel-start.ts';

export class OtelLogSink implements LogSink {
  readonly #logger: Logger;

  constructor() {
    // start otel in case it was not started yet
    // this is a no-op if already started
    startOtelAuto();
    this.#logger = logs.getLogger('zero-cache');
  }

  log(
    level: LogLevel,
    _context: Context | undefined,
    ...args: unknown[]
  ): void {
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
      severityText: level,
      body: message,
    };

    // eslint-disable-next-line no-console
    this.#logger.emit(payload);
  }
}
