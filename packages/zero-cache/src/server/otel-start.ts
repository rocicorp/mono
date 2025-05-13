import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {OTLPLogExporter} from '@opentelemetry/exporter-logs-otlp-http';
import {NodeSDK} from '@opentelemetry/sdk-node';
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions';
import {
  PeriodicExportingMetricReader,
  ConsoleMetricExporter,
} from '@opentelemetry/sdk-metrics';
import {resourceFromAttributes} from '@opentelemetry/resources';
import {version} from '../../../otel/src/version.ts';
import {
  BatchLogRecordProcessor,
  LoggerProvider,
  type LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import {logs} from '@opentelemetry/api-logs';
import {getNodeAutoInstrumentations} from '@opentelemetry/auto-instrumentations-node';

let started = false;
export function startOtelAuto() {
  if (started) {
    return;
  }
  started = true;

  const logRecordProcessors: LogRecordProcessor[] = [];
  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: 'syncer',
    [ATTR_SERVICE_VERSION]: version,
  });

  // Initialize logger provider if not already set
  if (!logs.getLoggerProvider()) {
    const provider = new LoggerProvider({resource});
    const processor = new BatchLogRecordProcessor(new OTLPLogExporter());
    logRecordProcessors.push(processor);
    provider.addLogRecordProcessor(processor);
    logs.setGlobalLoggerProvider(provider);
  }

  const logger = logs.getLogger('zero-cache');
  logger.emit({
    severityText: 'INFO',
    body: 'Starting OpenTelemetry with configuration',
  });

  const sdk = new NodeSDK({
    resource,
    // Automatically instruments all supported modules
    instrumentations: [getNodeAutoInstrumentations()],
    traceExporter: new OTLPTraceExporter(),
    metricReader: new PeriodicExportingMetricReader({
      exportIntervalMillis: 5000,
      exporter: (() => {
        if (process.env.NODE_ENV === 'dev') {
          return new ConsoleMetricExporter();
        }
        return new OTLPMetricExporter();
      })(),
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
