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
  LoggerProvider,
  type LogRecordProcessor,
} from '@opentelemetry/sdk-logs';
import {logs} from '@opentelemetry/api-logs';
import type {LogContext} from '@rocicorp/logger';


let started = false;
export function startOtel(lc: LogContext) {
  if (started) {
    return;
  }
  started = true;

  const logRecordProcessors: LogRecordProcessor[] = [];
  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: 'syncer',
    [ATTR_SERVICE_VERSION]: version,
  });

  // Parse headers from environment variable
  const headers: Record<string, string> = {};
  if (process.env.OTEL_EXPORTER_OTLP_HEADERS) {
    process.env.OTEL_EXPORTER_OTLP_HEADERS.split(',').forEach(header => {
      const [key, value] = header.split('=');
      if (key && value) {
        headers[key.trim()] = value.trim();
      }
    });
  }

  const otlpEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  if (!otlpEndpoint) {
    lc.warn?.('OTEL_EXPORTER_OTLP_ENDPOINT is not set, using noop exporters');
  }

  const commonConfig = otlpEndpoint ? {
    url: otlpEndpoint,
    headers,
  } : undefined;

  const traceCollector = process.env.OTEL_TRACES_EXPORTER === 'otlp' && otlpEndpoint;
  const metricCollector = process.env.OTEL_METRICS_EXPORTER === 'otlp' && otlpEndpoint;
  const logCollector = process.env.OTEL_LOGS_EXPORTER === 'otlp' && otlpEndpoint;

  if (logCollector && commonConfig) {
    const provider = new LoggerProvider({
      resource,
    });
    const processor = new BatchLogRecordProcessor(
      new OTLPLogExporter(commonConfig),
    );
    logRecordProcessors.push(processor);
    provider.addLogRecordProcessor(processor);
    logs.setGlobalLoggerProvider(provider);
  }

  const sdk = new NodeSDK({
    resource,
    traceExporter:
      !traceCollector || !commonConfig
        ? new NoopSpanExporter()
        : new OTLPTraceExporter(commonConfig),
    metricReader: new PeriodicExportingMetricReader({
      exportIntervalMillis: 5000,
      exporter: (() => {
        if (!metricCollector || !commonConfig) {
          if (process.env.NODE_ENV === 'dev') {
            return new ConsoleMetricExporter();
          }
          return new NoopMetricExporter();
        }

        return new OTLPMetricExporter(commonConfig);
      })(),
    }),
    logRecordProcessors,
  });
  sdk.start();
}
