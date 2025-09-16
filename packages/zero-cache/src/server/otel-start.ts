/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {logs} from '@opentelemetry/api-logs';
import {getNodeAutoInstrumentations} from '@opentelemetry/auto-instrumentations-node';
import type {Instrumentation} from '@opentelemetry/instrumentation';
import {resourceFromAttributes} from '@opentelemetry/resources';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {ATTR_SERVICE_VERSION} from '@opentelemetry/semantic-conventions';
import {LogContext} from '@rocicorp/logger';
import {setupOtelDiagnosticLogger} from './otel-diag-logger.js';
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
    });

    // Lazy load the auto-instrumentations module
    // avoid MODULE_NOT_FOUND errors in environments where it's not being used
    if (!this.#autoInstrumentations) {
      this.#autoInstrumentations = getNodeAutoInstrumentations();
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

export const startOtelAuto = (lc?: LogContext) =>
  OtelManager.getInstance().startOtelAuto(lc);
