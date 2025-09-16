/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {
  logs,
  SeverityNumber,
  type AnyValueMap,
  type Logger,
  type LogRecord,
} from '@opentelemetry/api-logs';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {errorOrObject} from '../../../shared/src/logging.ts';
import {stringify} from '../../../shared/src/bigint-json.ts';
import {startOtelAuto} from './otel-start.ts';

export class OtelLogSink implements LogSink {
  readonly #logger: Logger;

  constructor() {
    // start otel in case it was not started yet
    // this is a no-op if already started
    startOtelAuto();
    this.#logger = logs.getLogger('zero-cache');
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
      severityText: level,
      severityNumber: toErrorNum(level),
      body: message,
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
