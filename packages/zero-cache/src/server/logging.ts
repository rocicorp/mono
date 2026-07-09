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

export function createLogContext(
  {log}: {log: LogConfig},
  worker: string,
  workerIndex = 0,
  includeOtel = true,
): LogContext {
  const logSink = createLogSink(log, includeOtel);
  const lc = createLogContextShared({log}, {worker, workerIndex}, logSink);
  process.on('uncaughtException', async (err, origin) => {
    // Workers create an includeOtel=false context while bootstrapping OTel,
    // then a primary context. Run full corruption checks from the primary
    // handler only.
    await logUncaughtException(lc, logSink, err, origin, includeOtel);
    process.exit(UNHANDLED_EXCEPTION_ERROR_CODE);
  });
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

class CompositeLogSink implements LogSink {
  readonly #sinks: LogSink[];

  constructor(sinks: LogSink[]) {
    this.#sinks = sinks;
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    for (const sink of this.#sinks) {
      sink.log(level, context, ...args);
    }
  }
}
