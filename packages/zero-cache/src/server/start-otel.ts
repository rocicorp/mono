import {OTLPTraceExporter} from '@opentelemetry/exporter-trace-otlp-http';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
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

let started = false;
export function startOtel(endpoints: {
  traceCollector?: string | undefined;
  metricCollector?: string | undefined;
  logCollector?: string | undefined;
}) {
  if (started) {
    return;
  }
  started = true;

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: 'syncer',
      [ATTR_SERVICE_VERSION]: version,
    }),
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
  });
  sdk.start();
}
