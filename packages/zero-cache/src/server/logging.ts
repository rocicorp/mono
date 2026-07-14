import type {LogContext} from '@rocicorp/logger';
import {type Context, type LogLevel, type LogSink} from '@rocicorp/logger';
import {otelLogsEnabled} from '../../../otel/src/enabled.ts';
import {
  createLogContext as createLogContextShared,
  getLogSink,
  type LogConfig,
} from '../../../shared/src/logging.ts';
import {logLastChanceSQLiteCorruptionDiagnostics} from '../db/sqlite-corruption.ts';
import {UNHANDLED_EXCEPTION_ERROR_CODE} from '../services/life-cycle.ts';
import {OtelLogSink} from './otel-log-sink.ts';

type UncaughtExceptionHandler = (
  err: Error,
  origin: NodeJS.UncaughtExceptionOrigin,
) => Promise<void>;

let bootstrapUncaughtExceptionHandler: UncaughtExceptionHandler | undefined;

export function createLogContext(
  {log}: {log: LogConfig},
  worker: string,
  workerIndex = 0,
  includeOtel = true,
): LogContext {
  const logSink = createLogSink(log, includeOtel);
  const lc = createLogContextShared({log}, {worker, workerIndex}, logSink);
  const handleUncaughtException: UncaughtExceptionHandler = async (
    err,
    origin,
  ) => {
    // Workers create an includeOtel=false context while bootstrapping OTel,
    // then a primary context. Run full corruption checks from the primary
    // handler only.
    try {
      await logUncaughtException(lc, logSink, err, origin, includeOtel);
    } finally {
      process.exit(UNHANDLED_EXCEPTION_ERROR_CODE);
    }
  };
  if (bootstrapUncaughtExceptionHandler) {
    process.off('uncaughtException', bootstrapUncaughtExceptionHandler);
    bootstrapUncaughtExceptionHandler = undefined;
  }
  process.on('uncaughtException', handleUncaughtException);
  if (!includeOtel) {
    bootstrapUncaughtExceptionHandler = handleUncaughtException;
  }
  return lc;
}

export async function logUncaughtException(
  lc: LogContext,
  logSink: LogSink,
  err: unknown,
  origin: string,
  includeLastChanceDiagnostics = true,
): Promise<void> {
  lc.error?.(origin, err);
  if (includeLastChanceDiagnostics) {
    try {
      logLastChanceSQLiteCorruptionDiagnostics(lc, err);
    } catch (diagnosticError) {
      lc.error?.('SQLite corruption last-chance diagnostic failed', {
        error: diagnosticError,
      });
    }
  }
  await logSink.flush?.();
}

function createLogSink(config: LogConfig, includeOtel: boolean): LogSink {
  const sink = getLogSink(config);
  if (includeOtel && otelLogsEnabled()) {
    const otelSink = new OtelLogSink();
    return new CompositeLogSink([otelSink, sink]);
  }
  return sink;
}

export class CompositeLogSink implements LogSink {
  readonly #sinks: LogSink[];

  constructor(sinks: LogSink[]) {
    this.#sinks = sinks;
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    for (const sink of this.#sinks) {
      sink.log(level, context, ...args);
    }
  }

  async flush(): Promise<void> {
    await Promise.all(this.#sinks.map(sink => sink.flush?.()));
  }
}
