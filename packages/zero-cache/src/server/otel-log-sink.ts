import {
  logs,
  SeverityNumber,
  type AnyValueMap,
  type Logger,
  type LoggerProvider,
  type LogRecord,
} from '@opentelemetry/api-logs';
import type {Context, LogLevel, LogSink} from '@rocicorp/logger';
import {stringify} from '../../../shared/src/bigint-json.ts';
import {errorOrObject} from '../../../shared/src/logging.ts';

type FlushableLoggerProvider = LoggerProvider & {
  forceFlush?: (() => Promise<void>) | undefined;
};

export class OtelLogSink implements LogSink {
  readonly #logger: Logger;
  readonly #loggerProvider: FlushableLoggerProvider;

  constructor(
    loggerProvider = logs.getLoggerProvider() as FlushableLoggerProvider,
  ) {
    this.#loggerProvider = loggerProvider;
    this.#logger = loggerProvider.getLogger('zero-cache');
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

  async flush(): Promise<void> {
    await this.#loggerProvider.forceFlush?.();
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
