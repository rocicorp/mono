import {diag} from '@opentelemetry/api';
import {logs} from '@opentelemetry/api-logs';
import * as autoInstrumentationsModule from '@opentelemetry/auto-instrumentations-node';
import type {Instrumentation} from '@opentelemetry/instrumentation';
import {resourceFromAttributes} from '@opentelemetry/resources';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {ATTR_SERVICE_VERSION} from '@opentelemetry/semantic-conventions';
import {LogContext} from '@rocicorp/logger';
import {
  otelEnabled,
  otelLogsEnabled,
  otelMetricsEnabled,
  otelTracesEnabled,
} from '../../../otel/src/enabled.ts';

class OtelManager {
  static #instance: OtelManager;
  #started = false;
  #autoInstrumentations: Instrumentation[] | null = null;

  private constructor() {}

  static getInstance(): OtelManager {
    if (!OtelManager.#instance) {
      OtelManager.#instance = new OtelManager();
    }
    return OtelManager.#instance;
  }

  startOtelAuto(lc?: LogContext) {
    if (lc) {
      const log = lc.withContext('component', 'otel');
      diag.setLogger({
        verbose: (msg: string, ...args: unknown[]) => log.debug?.(msg, ...args),
        debug: (msg: string, ...args: unknown[]) => log.debug?.(msg, ...args),
        info: (msg: string, ...args: unknown[]) => log.info?.(msg, ...args),
        warn: (msg: string, ...args: unknown[]) => log.warn?.(msg, ...args),
        error: (msg: string, ...args: unknown[]) => log.error?.(msg, ...args),
      });
    }
    if (this.#started || !otelEnabled()) {
      return;
    }
    this.#started = true;

    // Use exponential histograms by default to reduce cardinality from auto-instrumentation
    // This affects HTTP server/client and other auto-instrumented histogram metrics
    // Exponential histograms automatically adjust bucket boundaries and use fewer buckets
    process.env.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION ??=
      'base2_exponential_bucket_histogram';

    const resource = resourceFromAttributes({
      [ATTR_SERVICE_VERSION]: process.env.ZERO_SERVER_VERSION ?? 'unknown',
    });

    // Lazy load the auto-instrumentations module
    // avoid MODULE_NOT_FOUND errors in environments where it's not being used
    if (!this.#autoInstrumentations) {
      this.#autoInstrumentations =
        autoInstrumentationsModule.getNodeAutoInstrumentations();
    }
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
      instrumentations: this.#autoInstrumentations
        ? [this.#autoInstrumentations]
        : [],
    });

    // Start SDK: will deploy Trace, Metrics, and Logs pipelines as per env vars
    sdk.start();
    logs.getLogger('zero-cache').emit({
      severityText: 'INFO',
      body: 'OpenTelemetry SDK started successfully',
    });
  }
}

export const startOtelAuto = (lc?: LogContext) =>
  OtelManager.getInstance().startOtelAuto(lc);
