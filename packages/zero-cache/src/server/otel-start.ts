import {logs} from '@opentelemetry/api-logs';
import {getNodeAutoInstrumentations} from '@opentelemetry/auto-instrumentations-node';
import {resourceFromAttributes} from '@opentelemetry/resources';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {ATTR_SERVICE_VERSION} from '@opentelemetry/semantic-conventions';
import type {LogContext} from '@rocicorp/logger';
import {
  otelEnabled,
  otelLogsEnabled,
  otelMetricsEnabled,
  otelTracesEnabled,
} from '../../../otel/src/enabled.ts';
import {setupOtelDiagnosticLogger} from './otel-diag-logger.ts';

class OtelManager {
  static #instance: OtelManager;
  #started = false;

  private constructor() {}

  static getInstance(): OtelManager {
    if (!OtelManager.#instance) {
      OtelManager.#instance = new OtelManager();
    }
    return OtelManager.#instance;
  }

  startOtelAuto(
    lc: LogContext | undefined,
    workerName: string,
    workerIndex: number,
  ) {
    if (this.#started || !otelEnabled()) {
      return;
    }
    this.#started = true;

    // Store and temporarily remove OTEL_LOG_LEVEL to prevent NodeSDK from setting its own logger
    const otelLogLevel = process.env.OTEL_LOG_LEVEL;
    delete process.env.OTEL_LOG_LEVEL;

    // Use exponential histograms by default to reduce cardinality from auto-instrumentation
    // This affects HTTP server/client and other auto-instrumented histogram metrics
    // Exponential histograms automatically adjust bucket boundaries and use fewer buckets
    process.env.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION ??=
      'base2_exponential_bucket_histogram';

    const resource = resourceFromAttributes({
      [ATTR_SERVICE_VERSION]: process.env.ZERO_SERVER_VERSION ?? 'unknown',
      // Tag every metric/trace/log with the worker name and index so each
      // worker process in a multi-worker pod is distinguishable. Without
      // this, N syncer workers sharing the same pod labels clobber each
      // other in the OTel collector on every scrape interval.
      // These mirror the 'worker' and 'workerIndex' keys in every log
      // context so logs and metrics can be correlated on the same fields.
      // Using a stable index instead of PID avoids label churn in Prometheus.
      'process.worker': workerName,
      'process.worker_index': workerIndex,
    });

    // Set defaults to be backwards compatible with the previously
    // hard-coded exporters
    process.env.OTEL_EXPORTER_OTLP_PROTOCOL ??= 'http/json';
    process.env.OTEL_METRICS_EXPORTER ??= otelMetricsEnabled()
      ? 'otlp'
      : 'none';
    process.env.OTEL_TRACES_EXPORTER ??= otelTracesEnabled() ? 'otlp' : 'none';
    process.env.OTEL_LOGS_EXPORTER ??= otelLogsEnabled() ? 'otlp' : 'none';

    const sdk = new NodeSDK({
      resource,
      autoDetectResources: true,
      instrumentations:
        process.env.OTEL_NODE_ENABLED_INSTRUMENTATIONS ||
        process.env.OTEL_NODE_DISABLED_INSTRUMENTATIONS
          ? [getNodeAutoInstrumentations()]
          : [],
    });

    try {
      sdk.start();
    } finally {
      if (otelLogLevel) {
        process.env.OTEL_LOG_LEVEL = otelLogLevel;
      }
    }
    setupOtelDiagnosticLogger(lc, true);

    logs.getLogger('zero-cache').emit({
      severityText: 'INFO',
      body: 'OpenTelemetry SDK started successfully',
    });
  }
}

export const startOtelAuto = (
  lc: LogContext | undefined,
  workerName: string,
  workerIndex: number,
) => OtelManager.getInstance().startOtelAuto(lc, workerName, workerIndex);
