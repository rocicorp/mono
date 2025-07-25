import {
  LogContext,
  type Context,
  type LogLevel,
  type LogSink,
} from '@rocicorp/logger';
import {otelLogsEnabled} from '../../../otel/src/enabled.ts';
import {
  createLogContext as createLogContextShared,
  getLogSink,
  type LogConfig,
} from '../../../shared/src/logging.ts';
import {OtelLogSink} from './otel-log-sink.ts';

export function createLogContext(
  {log}: {log: LogConfig},
  context: {worker: string},
): LogContext {
  return createLogContextShared({log}, context, createLogSink(log));
}

function createLogSink(config: LogConfig): LogSink {
  const sink = getLogSink(config);
  if (otelLogsEnabled()) {
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
