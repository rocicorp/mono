import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {OTLPLogExporter} from '@opentelemetry/exporter-logs-otlp-http';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {
  detectResources,
  envDetector,
  processDetector,
  hostDetector,
  resourceFromAttributes,
  defaultResource,
} from '@opentelemetry/resources';
import {ATTR_SERVICE_VERSION} from '@opentelemetry/semantic-conventions';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {version} from '../../../otel/src/version.ts';
import {
  BatchLogRecordProcessor,
  LoggerProvider,
  type LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import {logs} from '@opentelemetry/api-logs';
import {getNodeAutoInstrumentations} from '@opentelemetry/auto-instrumentations-node';

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

  startOtelAuto() {
    if (this.#started || !process.env.OTEL_EXPORTER_OTLP_ENDPOINT) {
      return;
    }
    this.#started = true;

    const logRecordProcessors: LogRecordProcessor[] = [];
    const envResource = detectResources({
      detectors: [envDetector, processDetector, hostDetector],
    });

    const customResource = resourceFromAttributes({
      [ATTR_SERVICE_VERSION]: version,
    });

    const resource = defaultResource().merge(envResource).merge(customResource);

    // Initialize logger provider if not already set
    if (!logs.getLoggerProvider()) {
      const provider = new LoggerProvider({resource});
      const processor = new BatchLogRecordProcessor(new OTLPLogExporter());
      logRecordProcessors.push(processor);
      provider.addLogRecordProcessor(processor);
      logs.setGlobalLoggerProvider(provider);
    }

    const logger = logs.getLogger('zero-cache');
    const sdk = new NodeSDK({
      resource,
      // Automatically instruments all supported modules
      instrumentations: [getNodeAutoInstrumentations()],
      traceExporter: new OTLPTraceExporter(),
      metricReader: new PeriodicExportingMetricReader({
        exportIntervalMillis: 5000,
        exporter: new OTLPMetricExporter(),
      }),
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

export const startOtelAuto = () => OtelManager.getInstance().startOtelAuto();
