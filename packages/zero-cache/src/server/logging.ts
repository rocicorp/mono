import type {LogContext} from '@rocicorp/logger';
import {type Context, type LogLevel, type LogSink} from '@rocicorp/logger';
import {otelLogsEnabled} from '../../../otel/src/enabled.ts';
import {
  createLogContext as createLogContextShared,
  getLogSink,
  type LogConfig,
} from '../../../shared/src/logging.ts';
import {UNHANDLED_EXCEPTION_ERROR_CODE} from '../services/life-cycle.ts';
import {OtelLogSink} from './otel-log-sink.ts';

export function createLogContext(
  {log}: {log: LogConfig},
  context: {worker: string},
  includeOtel = true,
): LogContext {
  const logSink = createLogSink(log, includeOtel);
  const lc = createLogContextShared({log}, context, logSink);
  process.on('uncaughtException', async (err, origin) => {
    lc.error?.(origin, err);
    await logSink.flush?.();
    process.exit(UNHANDLED_EXCEPTION_ERROR_CODE);
  });
  return lc;
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
