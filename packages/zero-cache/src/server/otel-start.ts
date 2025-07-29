import {diag} from '@opentelemetry/api';
import {logs} from '@opentelemetry/api-logs';
import * as autoInstrumentationsModule from '@opentelemetry/auto-instrumentations-node';
import {OTLPLogExporter} from '@opentelemetry/exporter-logs-otlp-http';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import type {Instrumentation} from '@opentelemetry/instrumentation';
import {
  defaultResource,
  detectResources,
  envDetector,
  hostDetector,
  processDetector,
  resourceFromAttributes,
} from '@opentelemetry/resources';
import {
  BatchLogRecordProcessor,
  LoggerProvider,
  type LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
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
    process.env.OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION =
      'base2_exponential_bucket_histogram';

    const logRecordProcessors: LogRecordProcessor[] = [];
    const envResource = detectResources({
      detectors: [envDetector, processDetector, hostDetector],
    });

    const customResource = resourceFromAttributes({
      [ATTR_SERVICE_VERSION]: process.env.ZERO_SERVER_VERSION ?? 'unknown',
    });

    const resource = defaultResource().merge(envResource).merge(customResource);

    // Initialize logger provider if not already set
    if (!logs.getLoggerProvider()) {
      const provider = new LoggerProvider({resource});
      if (otelLogsEnabled()) {
        const processor = new BatchLogRecordProcessor(new OTLPLogExporter());
        logRecordProcessors.push(processor);
        provider.addLogRecordProcessor(processor);
      }
      logs.setGlobalLoggerProvider(provider);
    }

    const logger = logs.getLogger('zero-cache');

    // Lazy load the auto-instrumentations module
    // avoid MODULE_NOT_FOUND errors in environments where it's not being used
    if (!this.#autoInstrumentations) {
      this.#autoInstrumentations =
        autoInstrumentationsModule.getNodeAutoInstrumentations();
    }

    const sdk = new NodeSDK({
      resource,
      instrumentations: this.#autoInstrumentations
        ? [this.#autoInstrumentations]
        : [],
      ...(otelTracesEnabled() ? {traceExporter: new OTLPTraceExporter()} : {}),
      ...(otelMetricsEnabled()
        ? {
            metricReader: new PeriodicExportingMetricReader({
              exportIntervalMillis: 60000,
              exporter: new OTLPMetricExporter(),
            }),
          }
        : {}),
      logRecordProcessors,
    });

    // Start SDK: will deploy Trace, Metrics, and Logs pipelines as per env vars
    sdk.start();
    logger.emit({
      severityText: 'INFO',
      body: 'OpenTelemetry SDK started successfully',
    });
  }
}

export const startOtelAuto = (lc?: LogContext) =>
  OtelManager.getInstance().startOtelAuto(lc);
