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
import {NoopSpanExporter} from '../../../otel/src/noop-span-exporter.ts';
import {NoopMetricExporter} from '../../../otel/src/noop-metric-exporter.ts';
import {version} from '../../../otel/src/version.ts';
import {
  BatchLogRecordProcessor,
  ConsoleLogRecordExporter,
  LoggerProvider,
  SimpleLogRecordProcessor,
  type LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import {logs} from '@opentelemetry/api-logs';

export type OtelEndpoints = {
  traceCollector?: string | undefined;
  metricCollector?: string | undefined;
  logCollector?: string | undefined;
};

let started = false;
export function startOtel(endpoints: OtelEndpoints) {
  if (started) {
    return;
  }
  started = true;

  const logRecordProcessors: LogRecordProcessor[] = [];
  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: 'syncer',
    [ATTR_SERVICE_VERSION]: version,
  });

  if (endpoints.logCollector !== undefined) {
    const provider = new LoggerProvider({
      resource,
    });
    const processor = new BatchLogRecordProcessor(
      new OTLPLogExporter({
        url: endpoints.logCollector,
      }),
    );
    const consoleProcessor = new SimpleLogRecordProcessor(
      // TODO: we need to write a custom console log exporter to preserve
      // our old format.
      new ConsoleLogRecordExporter(),
    );
    logRecordProcessors.push(processor);
    logRecordProcessors.push(consoleProcessor);
    provider.addLogRecordProcessor(processor);
    provider.addLogRecordProcessor(consoleProcessor);
    logs.setGlobalLoggerProvider(provider);
  }

  const sdk = new NodeSDK({
    resource,
    traceExporter:
      endpoints.traceCollector === undefined
        ? new NoopSpanExporter()
        : new OTLPTraceExporter({
            url: endpoints.traceCollector,
          }),
    metricReader: new PeriodicExportingMetricReader({
      exportIntervalMillis: 5000,
      exporter: (() => {
        if (endpoints.metricCollector === undefined) {
          if (process.env.NODE_ENV === 'dev') {
            return new ConsoleMetricExporter();
          }

          return new NoopMetricExporter();
        }

        return new OTLPMetricExporter({
          url: endpoints.metricCollector,
        });
      })(),
    }),
    logRecordProcessors,
  });
  sdk.start();
}
